"""Cached LiteLLM list prices for local Codex token accounting."""

from datetime import timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
import json
import math
from pathlib import Path
import random
import re
import threading
import time

import httpx

from openaidr.core.files import atomic_write
from . import network_proxy

URL = "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json"
DAY = 86400
HOUR = 3600
MAX_BYTES = 16 << 20
FIELDS = ("input_cost_per_token", "cache_read_input_token_cost", "cache_creation_input_token_cost",
          "output_cost_per_token")
TIERS = {"": "", "standard": "", "default": "", "auto": "", "priority": "_priority",
         "fast": "_priority", "ultrafast": "_ultrafast", "batch": "_batches", "flex": "_flex"}
CONTEXT = re.compile(r"_above_([0-9]{1,8})k_tokens")


def token_rates(item):
    rates = {}
    for name, value in item.items():
        if name.startswith(FIELDS):
            if type(value) not in (float, int) or not math.isfinite(value) or not 0 <= value <= 2**53:
                raise ValueError("Invalid token price")
            rates[name] = value
    return rates


def rates_from_feed(data):
    """Keep exact, direct OpenAI text-model rows and numeric token rates only."""
    if not isinstance(data, dict):
        raise ValueError("Invalid model pricing catalog")
    rows = {}
    for model, item in data.items():
        if (not isinstance(item, dict) or "/" in model or item.get("litellm_provider") != "openai"
                or item.get("mode") not in ("chat", "responses") or "tiered_pricing" in item):
            continue
        rates = token_rates(item)
        if FIELDS[0] in rates and FIELDS[-1] in rates:
            rows[model] = rates
    if not rows:
        raise ValueError("No supported model prices")
    return rows


def token_cost(rates, tier, counts, attrs):
    suffix = TIERS.get(tier)
    if suffix is None:
        return None
    total_input = sum(counts[:3])
    thresholds = {int(match[1]) for key in rates if (match := CONTEXT.search(key))}
    threshold = max((size for size in thresholds if total_input > size * 1000), default=0)
    context = f"_above_{threshold}k_tokens" if threshold else ""
    units = dict(zip(FIELDS, counts))
    if counts[2]:
        retention = attrs.get("prompt_cache_retention")
        if retention == "12h":
            units[FIELDS[2] + "_above_12hr"] = units.pop(FIELDS[2])
        elif retention not in (None, "", "30m"):
            return None
    cost = Decimal(0)
    for field, count in units.items():
        if count:
            rate = rates.get(field + context + suffix)
            if rate is None:
                return NotImplemented
            cost += count * Decimal(str(rate))
    return cost


class Catalog:
    def __init__(self, config):
        self.config = config
        self.path = Path(config["data_dir"]) / "model-pricing.json"
        self.rows = {}
        self.etag = ""
        self.checked = 0
        self.missing = set()
        self.lock = threading.Lock()
        self.next_attempt = time.time() + random.uniform(0, 60)
        try:
            with self.path.open("rb") as stream:
                raw = stream.read(MAX_BYTES + 1)
            if len(raw) > MAX_BYTES:
                raise ValueError("Oversized pricing cache")
            cached = json.loads(raw)
            checked, etag = cached["checked"], cached["etag"]
            if not (type(checked) in (int, float) and 0 < checked <= time.time()
                    and isinstance(etag, str) and etag.isascii() and len(etag) <= 256
                    and etag.isprintable()):
                raise ValueError("Invalid pricing cache metadata")
            self.rows = self._restore(cached["models"], checked)
            self.etag, self.checked = etag, checked
        except (OSError, KeyError, ValueError, TypeError, RecursionError, AttributeError, OverflowError):
            pass
        self.due = self.checked + DAY * random.uniform(.95, 1)

    @staticmethod
    def _restore(rows, checked):
        if not isinstance(rows, dict) or not rows:
            raise ValueError("Empty price cache")
        result = {}
        for model, (start, item) in rows.items():
            rates = token_rates(item)
            if (type(start) not in (float, int) or not 0 < start <= checked
                    or FIELDS[0] not in rates or FIELDS[-1] not in rates):
                raise ValueError("Invalid cached model price")
            result[model] = (start, rates)
        return result

    def quote(self, model, tier, counts, attrs, when):
        """Defer to bundled pricing when the catalog has no applicable quote."""
        rows = self.rows
        record = rows.get(model)
        if record is None:
            if model and len(model) <= 256:
                with self.lock:
                    if len(self.missing) < 128 and model not in self.missing:
                        self.missing.add(model)
                        self.due = min(self.due, max(self.checked + HOUR, time.time()))
            return NotImplemented
        since, rates = record
        if when is not None and when.timestamp() < since:
            return NotImplemented
        return token_cost(rates, tier, counts, attrs)

    def refresh(self):
        now = time.time()
        headers = {"If-None-Match": self.etag} if self.etag else {}
        options = network_proxy.transport_options(self.config, True)
        with httpx.Client(timeout=10, **options) as client, client.stream("GET", URL, headers=headers) as response:
            if response.status_code == 429 or response.status_code == 503:
                delay = response.headers.get("retry-after", "")
                try:
                    seconds = int(delay) if delay.isdecimal() else (
                        parsedate_to_datetime(delay).astimezone(timezone.utc).timestamp() - now)
                    self.next_attempt = max(self.next_attempt, now + seconds)
                except (ValueError, TypeError, OverflowError):
                    pass
            if response.status_code == 304 and self.rows and self.etag:
                rows = self.rows
                etag = self.etag
            else:
                response.raise_for_status()
                if response.status_code != 200:
                    raise ValueError("Unexpected catalog response")
                deadline = time.monotonic() + 20
                body = bytearray()
                for chunk in response.iter_bytes(chunk_size=65536):
                    if time.monotonic() > deadline:
                        raise TimeoutError("Pricing download took too long")
                    if len(body) + len(chunk) > MAX_BYTES:
                        raise ValueError("Oversized pricing catalog")
                    body.extend(chunk)
                models = rates_from_feed(json.loads(body))
                rows = {model: self.rows[model] if model in self.rows and self.rows[model][1] == rates
                        else (now, rates) for model, rates in models.items()}
                etag = response.headers.get("etag", "")
                if len(etag) > 256 or not etag.isascii() or not etag.isprintable():
                    etag = ""
        # Include the notice in each cached copy. Publish data and its ETag together.
        notice = Path(__file__).with_name("litellm-LICENSE.txt").read_text(encoding="utf-8")
        cached = dict(license=notice, checked=now, etag=etag, models=rows)
        atomic_write(self.path, json.dumps(cached, separators=(",", ":")))
        with self.lock:
            self.rows, self.checked, self.etag = rows, now, etag
            self.due = now + DAY * random.uniform(.95, 1)

    def run(self, stop, paused):
        while not stop.is_set():
            now = time.time()
            if not paused() and now >= max(self.due, self.next_attempt):
                self.next_attempt = now + HOUR
                if now >= self.checked + DAY * .95:
                    with self.lock:
                        self.missing.clear()
                try:
                    self.refresh()
                except Exception:
                    # Pricing is best effort. Collection continues with the last valid cache.
                    pass
            stop.wait(30)
