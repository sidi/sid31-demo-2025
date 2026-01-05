from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

import requests
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from fx.models import Currency, Rate


DEFAULT_QUOTES = ["USD", "EUR", "CNY"]
DEFAULT_API_BASE = "https://connect.bcm.mr/api/cours_change_reference"


@dataclass(frozen=True)
class BcmRateRow:
    rate_date: date
    currency: str
    value: Decimal
    weight: int
    name_english: str


def _parse_date_yyyy_mm_dd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _to_decimal(x: Any) -> Decimal:
    try:
        return Decimal(str(x))
    except (InvalidOperation, TypeError):
        raise CommandError(f"Cannot parse Decimal from value: {x!r}")


def _http_get_json(session: requests.Session, url: str, params: Dict[str, str]) -> Any:
    headers = {
        "User-Agent": os.environ.get(
            "BCM_USER_AGENT",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        ),
        "Accept": "application/json,*/*",
        "Referer": os.environ.get("BCM_REFERER", "https://www.bcm.mr/money-rate-table"),
    }
    resp = session.get(url, params=params, headers=headers, timeout=25)
    resp.raise_for_status()
    return resp.json()


def _fetch_latest_rate_in_range(
    session: requests.Session,
    api_base: str,
    currency: str,
    from_date: date,
    to_date: date,
) -> Optional[BcmRateRow]:
    """
    Fetch rates for a currency in [from_date, to_date].
    If empty, return None.
    If multiple rows returned, return the most recent by 'date'.
    """
    payload = _http_get_json(
        session,
        api_base,
        params={
            "from": from_date.isoformat(),
            "to": to_date.isoformat(),
            "currency": currency.upper(),
        },
    )

    if not isinstance(payload, list) or not payload:
        return None

    rows: List[BcmRateRow] = []
    for item in payload:
        if not isinstance(item, dict):
            continue

        ccy = str(item.get("currency", "")).upper()
        if ccy != currency.upper():
            continue

        d_raw = item.get("date")
        v_raw = item.get("value")
        w_raw = item.get("weight", 1)
        name_en = str(item.get("nameEnglish", "") or "").strip()

        if not d_raw or v_raw is None:
            continue

        d = _parse_date_yyyy_mm_dd(str(d_raw))
        v = _to_decimal(v_raw)

        try:
            w = int(w_raw) if w_raw is not None else 1
        except (ValueError, TypeError):
            w = 1

        if w <= 0:
            continue

        rows.append(BcmRateRow(rate_date=d, currency=ccy, value=v, weight=w, name_english=name_en))

    if not rows:
        return None

    return max(rows, key=lambda r: r.rate_date)


class Command(BaseCommand):
    help = (
        "Fetch MRU exchange rates from BCM (connect.bcm.mr) and store them using "
        "Currency(code,label) and Rate(unit,price,date,currency). "
        "Handles holidays/weekends by looking back to find the latest available day."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--quotes",
            nargs="*",
            default=DEFAULT_QUOTES,
            help="List of currencies to fetch (e.g., USD EUR CNY).",
        )
        parser.add_argument(
            "--api-base",
            default=os.environ.get("BCM_API_BASE", DEFAULT_API_BASE),
            help=f"BCM API base URL (default: {DEFAULT_API_BASE}). You can also set BCM_API_BASE.",
        )
        parser.add_argument(
            "--to-date",
            default=os.environ.get("BCM_TO_DATE", ""),
            help="Target end date YYYY-MM-DD. Default: today.",
        )
        parser.add_argument(
            "--lookback-days",
            type=int,
            default=int(os.environ.get("BCM_LOOKBACK_DAYS", "30")),
            help="Initial lookback window in days (default: 30).",
        )
        parser.add_argument(
            "--max-lookback-days",
            type=int,
            default=int(os.environ.get("BCM_MAX_LOOKBACK_DAYS", "365")),
            help="Max lookback days if still empty (default: 365).",
        )

    @transaction.atomic
    def handle(self, *args, **opts):
        quotes: List[str] = [q.upper() for q in (opts["quotes"] or [])]
        api_base: str = str(opts["api_base"]).strip()
        if not api_base:
            raise CommandError("Missing BCM API base URL.")

        to_date_str: str = str(opts["to_date"]).strip()
        to_dt: date = _parse_date_yyyy_mm_dd(to_date_str) if to_date_str else date.today()

        lookback_days: int = int(opts["lookback_days"])
        max_lookback_days: int = 365

        if lookback_days <= 0:
            raise CommandError("--lookback-days must be > 0")
        if max_lookback_days < lookback_days:
            raise CommandError("--max-lookback-days must be >= --lookback-days")

        session = requests.Session()

        stored = 0
        for ccy in quotes:
            current_lookback = lookback_days
            latest: Optional[BcmRateRow] = None

            while current_lookback <= max_lookback_days and latest is None:
                from_dt = to_dt - timedelta(days=current_lookback)
                latest = _fetch_latest_rate_in_range(session, api_base, ccy, from_dt, to_dt)
                if latest is None:
                    # expand window in steps
                    current_lookback = min(current_lookback + lookback_days, max_lookback_days + 1)

            if latest is None:
                self.stdout.write(self.style.WARNING(
                    f"No BCM data found for {ccy} in the last {max_lookback_days} day(s) ending {to_dt}."
                ))
                continue

            # Ensure Currency exists; use nameEnglish as label fallback.
            label = latest.name_english or latest.currency
            currency_obj, _created = Currency.objects.update_or_create(
                code=latest.currency,
                defaults={"label": label},
            )

            # BCM returns: value for 'weight' units (based on your sample).
            # Store exactly that into Rate(unit=weight, price=value).
            # If you prefer per-1-unit normalization: set unit=1 and price=value/weight instead.
            Rate.objects.update_or_create(
                currency=currency_obj,
                date=latest.rate_date,
                defaults={
                    "unit": int(latest.weight),
                    "price": float(latest.value),
                },
            )

            stored += 1
            self.stdout.write(
                f"{ccy}: stored unit={latest.weight}, price={latest.value} for {latest.rate_date} "
                f"(searched back up to {current_lookback} days)"
            )

        self.stdout.write(self.style.SUCCESS(f"Done. Stored/updated {stored} rate(s)."))
