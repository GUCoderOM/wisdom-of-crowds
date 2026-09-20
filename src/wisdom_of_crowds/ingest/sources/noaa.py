"""NOAA GEFS ensemble-weather ingestion.

The Global Ensemble Forecast System is NOAA's answer to the fact that a
single weather forecast is a point estimate of a chaotic system. Instead of
running the model once, GEFS runs it **31 times** per cycle — one unperturbed
"control" run plus 30 members whose initial conditions are nudged within
observational error. The spread between those members *is* the forecast's
uncertainty, and their centre is a better estimate than any single member.

That is Galton's ox, computed rather than polled: 31 independent-ish
estimators of one unknown number. Each ``(location, variable, forecast_time)``
becomes one ``numeric_guesses`` row whose ``guesses`` array holds every
member's value for that hour.

Why this API
------------
GEFS is published natively as GRIB2, which needs ``cfgrib``/``eccodes`` —
heavy binary dependencies we deliberately keep out of the Spark runtime (see
the dependency note in ``pyproject.toml``). The **Open-Meteo Ensemble API**
[1] re-publishes the same GEFS run as point forecasts in plain JSON: free, no
API key, no registration, and it pre-extracts each ensemble member into its
own series (``temperature_2m_member01`` … ``_member30``, with the bare
``temperature_2m`` key carrying the control run). One ``urllib`` call and a
``json.loads`` gets us the whole crowd, so this source needs nothing beyond
the standard library.

The ``model`` parameter pins us to ``gfs025`` — GEFS at 0.25 degrees —
rather than Open-Meteo's ``*_seamless`` blends, which splice several
different NWP models together and would quietly stop being "GEFS".

References:
  [1] https://open-meteo.com/en/docs/ensemble-api
  [2] https://www.emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/gefs.php
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import math
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from wisdom_of_crowds.ingest.base import Source, SourceConfig
from wisdom_of_crowds.schema.answers import INGEST_ANSWER_SCHEMA, QuestionType

_log = logging.getLogger(__name__)

_DEFAULT_API_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"

#: GEFS at 0.25 degrees. Deliberately not a ``*_seamless`` blend — those mix
#: in non-GEFS models and this source claims to be GEFS.
_DEFAULT_MODEL = "gfs025"

_DEFAULT_LOCATIONS: tuple[dict[str, Any], ...] = (
    {"name": "London", "lat": 51.5, "lon": -0.13},
    {"name": "NYC", "lat": 40.71, "lon": -74.0},
    {"name": "Tokyo", "lat": 35.68, "lon": 139.69},
)
_DEFAULT_VARIABLES: tuple[str, ...] = ("temperature_2m", "precipitation")
_DEFAULT_FORECAST_HOURS: tuple[int, ...] = (24, 72, 168)

#: ``api_variable -> (market_id slug, human label)``. Unlisted variables fall
#: back to a slugified form of the API name, so adding a variable to the YAML
#: works without touching this table.
_VAR_META: dict[str, tuple[str, str]] = {
    "temperature_2m":       ("temp_2m",    "2m temperature"),
    "precipitation":        ("precip",     "precipitation"),
    "relative_humidity_2m": ("rh_2m",      "2m relative humidity"),
    "wind_speed_10m":       ("wind_10m",   "10m wind speed"),
    "pressure_msl":         ("pressure",   "mean sea-level pressure"),
    "cloud_cover":          ("cloud",      "cloud cover"),
}

#: Open-Meteo's ensemble horizon cap for the 0.25-degree GEFS product.
_MAX_FORECAST_DAYS = 35

# The API returns naive local-to-``timezone`` stamps like "2026-09-23T00:00".
_API_TIME_FORMAT = "%Y-%m-%dT%H:%M"


class NOAASource(Source):
    """One ``numeric_guesses`` row per (location, variable, forecast_time).

    The crowd is the GEFS ensemble: ``guesses`` holds one value per member,
    and ``trader_count`` records how many members actually reported (members
    occasionally drop out of a series, so this is not always 31).
    """

    slug = "noaa"

    def extract(self, spark: SparkSession, cfg: SourceConfig) -> DataFrame:
        api_url   = str(cfg.params.get("api_url", _DEFAULT_API_URL))
        model     = str(cfg.params.get("model", _DEFAULT_MODEL))
        locations = list(cfg.params.get("locations", _DEFAULT_LOCATIONS))
        variables = [str(v) for v in cfg.params.get("variables", _DEFAULT_VARIABLES)]
        retries   = int(cfg.params.get("retries", 3))
        timeout   = int(cfg.params.get("timeout_seconds", 60))
        # The control run is a genuine GEFS member (it is the unperturbed
        # forecast), so it counts as a voice in the crowd by default.
        include_control = bool(cfg.params.get("include_control", True))

        try:
            forecast_hours = sorted({int(h) for h in
                                     cfg.params.get("forecast_hours", _DEFAULT_FORECAST_HOURS)})
        except (TypeError, ValueError):
            _log.info("noaa_bad_forecast_hours_empty_batch", extra={
                "forecast_hours": cfg.params.get("forecast_hours"),
            })
            return spark.createDataFrame([], INGEST_ANSWER_SCHEMA)

        if not locations or not variables or not forecast_hours:
            _log.info("noaa_nothing_configured_empty_batch", extra={
                "n_locations": len(locations),
                "n_variables": len(variables),
                "n_horizons":  len(forecast_hours),
            })
            return spark.createDataFrame([], INGEST_ANSWER_SCHEMA)

        try:
            payloads = self._fetch(
                api_url, locations, variables, forecast_hours, model,
                retries=retries, timeout=timeout,
            )
        except Exception as exc:
            # A dead upstream is an empty batch, never a failed run: silver
            # stays consistent and the next cycle simply tries again.
            _log.info("noaa_download_failed_empty_batch", extra={
                "api_url": api_url, "model": model, "err": repr(exc),
            })
            return spark.createDataFrame([], INGEST_ANSWER_SCHEMA)

        rows = list(self._rows_from_payloads(
            payloads, locations, variables, forecast_hours, cfg,
            include_control=include_control,
        ))
        if not rows:
            _log.info("noaa_no_rows_empty_batch", extra={
                "api_url": api_url, "n_payloads": len(payloads),
            })
            return spark.createDataFrame([], INGEST_ANSWER_SCHEMA)

        _log.info("noaa_rows_built", extra={"rows": len(rows), "model": model})
        return spark.createDataFrame(rows, INGEST_ANSWER_SCHEMA)

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    @classmethod
    def _fetch(
        cls,
        api_url: str,
        locations: Sequence[Mapping[str, Any]],
        variables: Sequence[str],
        forecast_hours: Sequence[int],
        model: str,
        *,
        retries: int,
        timeout: int,
    ) -> list[dict[str, Any]]:
        """Fetch every location in a single request.

        Open-Meteo accepts comma-separated coordinate lists and answers with a
        JSON array in the same order (a bare object when only one location is
        asked for). One request keeps us well inside the free tier's rate
        limit no matter how many locations the YAML lists.
        """
        # +1 day of slack so the last requested hour is never the final
        # element of the series (the API trims to whole local days).
        forecast_days = min(_MAX_FORECAST_DAYS, math.ceil(max(forecast_hours) / 24) + 1)

        query = {
            "latitude":      ",".join(str(loc["lat"]) for loc in locations),
            "longitude":     ",".join(str(loc["lon"]) for loc in locations),
            "hourly":        ",".join(variables),
            "models":        model,
            "forecast_days": forecast_days,
            "timezone":      "GMT",
        }
        payload = cls._get_json(api_url, query, retries=retries, timeout=timeout)
        return payload if isinstance(payload, list) else [payload]

    @staticmethod
    def _get_json(
        url: str,
        query: Mapping[str, Any] | None = None,
        *,
        retries: int = 3,
        timeout: int = 60,
    ) -> Any:
        """GET + parse JSON with exponential backoff, as in ``manifold.py``."""
        full_url = url
        if query:
            full_url += "?" + urllib.parse.urlencode(dict(query))
        req = urllib.request.Request(full_url, headers={"User-Agent": "wisdom-of-crowds/0.3"})

        last: Exception | None = None
        for attempt in range(retries):
            try:
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    return json.loads(r.read().decode())
            except (urllib.error.URLError, TimeoutError, ValueError) as e:
                last = e
                if attempt < retries - 1:
                    time.sleep(1.5 ** attempt)
        raise RuntimeError(f"Open-Meteo ensemble API failed after {retries} attempts: {last}")

    # ------------------------------------------------------------------
    # payload -> INGEST_ANSWER_SCHEMA rows
    # ------------------------------------------------------------------

    @classmethod
    def _rows_from_payloads(
        cls,
        payloads: Sequence[Mapping[str, Any]],
        locations: Sequence[Mapping[str, Any]],
        variables: Sequence[str],
        forecast_hours: Sequence[int],
        cfg: SourceConfig,
        *,
        include_control: bool = True,
    ) -> Iterator[tuple]:
        """Yield one INGEST_ANSWER_SCHEMA tuple per (location, variable, horizon).

        Pure apart from logging — no Spark, no network — so the row shape can
        be unit-tested against a recorded payload.
        """
        if len(payloads) != len(locations):
            # Shouldn't happen: the API echoes one object per coordinate.
            # Pair up what we can rather than dropping the whole batch.
            _log.warning("noaa_payload_location_mismatch", extra={
                "n_payloads": len(payloads), "n_locations": len(locations),
            })

        for loc, payload in zip(locations, payloads, strict=False):
            loc_name = str(loc.get("name") or "unknown")
            hourly = payload.get("hourly") or {}
            times = hourly.get("time") or []
            if not times:
                _log.warning("noaa_location_without_hourly", extra={"location": loc_name})
                continue

            units = payload.get("hourly_units") or {}
            # Anchor horizons to the first hour of the returned series, which
            # is 00Z of the current day — so "t+72h" is a stable, reproducible
            # label rather than "72 hours after whenever this job happened
            # to start".
            base = _parse_api_time(times[0])
            index_of = {t: i for i, t in enumerate(times)}

            for var in variables:
                member_keys = _member_keys(hourly, var, include_control=include_control)
                if not member_keys:
                    _log.warning("noaa_variable_missing", extra={
                        "location": loc_name, "variable": var,
                    })
                    continue

                var_slug, var_label = _VAR_META.get(var, (_slugify(var), var.replace("_", " ")))
                unit = _clean_unit(units.get(var))

                for hours in forecast_hours:
                    target = base + dt.timedelta(hours=hours)
                    idx = index_of.get(target.strftime(_API_TIME_FORMAT))
                    if idx is None:
                        _log.warning("noaa_horizon_out_of_range", extra={
                            "location": loc_name, "variable": var, "hours": hours,
                        })
                        continue

                    market_id = _market_id(var_slug, loc_name, target, hours)
                    question  = _question(var_label, unit, loc_name, target, hours)
                    any_row = False
                    for key in member_keys:
                        value = _f(_at(hourly.get(key), idx))
                        if value is None:
                            continue
                        any_row = True
                        # ``name`` is the ensemble member: ``control`` for the
                        # unperturbed run, ``memberNN`` for perturbed members.
                        member = "control" if key == var else key[len(f"{var}_"):]
                        yield (
                            cls.slug,                       # source
                            market_id,
                            question,
                            QuestionType.NUMERIC_GUESSES,
                            member,                         # name
                            value,                          # answer_value
                            None,                           # answer_outcome
                            None,                           # weight
                            target,                         # created_at = forecast time
                            cfg.cycle_ts,                   # cycle_ts
                        )
                    if not any_row:
                        _log.warning("noaa_no_members_reported", extra={
                            "location": loc_name, "variable": var, "hours": hours,
                        })


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _market_id(var_slug: str, loc_name: str, target: dt.datetime, hours: int) -> str:
    """Deterministic id, e.g. ``gefs:temp_2m:london:2026-09-23T00Z:t+72h``.

    Stable across runs for the same forecast target, so re-running a cycle
    truncate-loads the same rows instead of duplicating them.
    """
    return (
        f"gefs:{var_slug}:{_slugify(loc_name)}:"
        f"{target.strftime('%Y-%m-%dT%H')}Z:t+{hours}h"
    )


def _question(var_label: str, unit: str, loc_name: str, target: dt.datetime, hours: int) -> str:
    """e.g. ``GEFS ensemble: 2m temperature (C) at London on 2026-09-23 00Z (72h out)``."""
    unit_part = f" ({unit})" if unit else ""
    return (
        f"GEFS ensemble: {var_label}{unit_part} at {loc_name} "
        f"on {target.strftime('%Y-%m-%d %H')}Z ({hours}h out)"
    )


def _member_keys(
    hourly: Mapping[str, Any], var: str, *, include_control: bool,
) -> list[str]:
    """Return the ensemble-member series keys for ``var``, in member order.

    Open-Meteo names the perturbed members ``<var>_member01`` … ``_member30``
    and puts the unperturbed control run under the bare ``<var>`` key.
    """
    prefix = f"{var}_member"
    members = sorted(
        (k for k in hourly if k.startswith(prefix) and k[len(prefix):].isdigit()),
        key=lambda k: int(k[len(prefix):]),
    )
    if include_control and var in hourly:
        return [var, *members]
    return members


def _at(series: Any, idx: int) -> Any:
    """``series[idx]`` when that is meaningful, else ``None``."""
    if isinstance(series, Sequence) and not isinstance(series, str) and idx < len(series):
        return series[idx]
    return None


def _f(x: Any) -> float | None:
    """Best-effort float cast; returns None for null/nan/non-numeric."""
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v == v else None  # filter NaN


def _parse_api_time(value: str) -> dt.datetime:
    """Parse an Open-Meteo ``iso8601`` stamp. The request pins ``timezone=GMT``,
    so the naive stamps it returns are UTC."""
    return dt.datetime.fromisoformat(str(value)).replace(tzinfo=dt.UTC)


def _clean_unit(unit: Any) -> str:
    """``"°C"`` -> ``"C"``; keeps ``mm``/``%`` as-is."""
    if not unit:
        return ""
    return str(unit).replace("°", "").strip()


def _slugify(value: str) -> str:
    """Lowercase, non-alphanumerics collapsed to single underscores."""
    slug = re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")
    return slug or "unknown"
