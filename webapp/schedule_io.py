"""Single-day UTC schedule CSV parsing.

Accepted header aliases (case-insensitive, whitespace trimmed):

==================  ========================================
concept            accepted column names
==================  ========================================
trip_id            trip_id, 班次
stop_sequence      stop_sequence, 站序
stop_code          stop_code, 站号
scheduled_time     scheduled_time, 计划时刻, 计划时间
==================  ========================================

Scheduled times may be ``HH:MM[:SS]`` (interpreted in UTC on the
service date), full ISO-8601 UTC timestamps, or epoch seconds.
Everything is normalized to ``YYYY-MM-DDTHH:MM:SSZ``.
"""
from __future__ import annotations

import csv
import io
import re
from datetime import datetime, timezone

HEADER_ALIASES = {
    "trip_id": {"trip_id", "班次id", "班次", "trip"},
    "stop_sequence": {"stop_sequence", "站序", "序号", "stopsequence"},
    "stop_code": {"stop_code", "站号", "stopid", "stop"},
    "scheduled_time": {"scheduled_time", "计划时刻", "计划时间", "scheduledtime"},
}

_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_ISO_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?"
    r"(?:\.\d+)?(Z|[+-]\d{2}:?\d{2})?$"
)
_HMS_RE = re.compile(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?$")
_EPOCH_RE = re.compile(r"^\d{9,11}$")


class ScheduleError(ValueError):
    """Raised for any malformed schedule upload."""


def _normalize_header(name: str) -> str:
    return name.strip().lstrip("﻿").lower().replace(" ", "")


def _map_headers(fieldnames: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for raw in fieldnames or []:
        key = _normalize_header(raw)
        for concept, aliases in HEADER_ALIASES.items():
            if key in aliases and concept not in mapping:
                mapping[concept] = raw
    missing = [c for c in HEADER_ALIASES if c not in mapping]
    if missing:
        raise ScheduleError(
            "CSV 缺少必需列: " + ", ".join(missing)
            + f"；实际表头: {fieldnames}"
        )
    return mapping


def parse_time(value: str, service_date: str | None) -> tuple[str, str]:
    """Return (iso8601 UTC string, service_date)."""
    v = value.strip()
    if not v:
        raise ScheduleError("空的计划时刻")

    m = _HMS_RE.match(v)
    if m:
        if not service_date:
            raise ScheduleError(
                f"时刻 {v} 为 HH:MM 格式，需要提供 service_date 服务日期"
            )
        _validate_date(service_date)
        hh, mm, ss = m.group(1), m.group(2), m.group(3) or "00"
        if int(hh) > 23 or int(mm) > 59 or int(ss) > 59:
            raise ScheduleError(f"非法时刻: {v}")
        return f"{service_date}T{hh.zfill(2)}:{mm}:{ss}Z", service_date

    m = _ISO_RE.match(v)
    if m:
        year, mon, day, hh, mm = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5)
        ss = m.group(6) or "00"
        tz = m.group(7)
        dt = datetime(
            int(year), int(mon), int(day), int(hh), int(mm), int(ss),
            tzinfo=timezone.utc,
        )
        if tz and tz != "Z":
            sign = 1 if tz[0] == "+" else -1
            tz_clean = tz.replace(":", "")
            off_h, off_m = int(tz_clean[1:3]), int(tz_clean[3:5])
            from datetime import timedelta

            dt = dt - sign * timedelta(hours=off_h, minutes=off_m)
        date_part = dt.strftime("%Y-%m-%d")
        if service_date and date_part != service_date:
            raise ScheduleError(
                f"行时刻日期 {date_part} 与服务日期 {service_date} 不一致"
            )
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ"), date_part

    if _EPOCH_RE.match(v):
        dt = datetime.fromtimestamp(int(v), tz=timezone.utc)
        date_part = dt.strftime("%Y-%m-%d")
        if service_date and date_part != service_date:
            raise ScheduleError(
                f"行时刻日期 {date_part} 与服务日期 {service_date} 不一致"
            )
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ"), date_part

    raise ScheduleError(f"无法解析的计划时刻: {v}")


def _validate_date(service_date: str) -> None:
    if not _DATE_RE.match(service_date):
        raise ScheduleError(f"服务日期格式应为 YYYY-MM-DD: {service_date}")
    datetime.strptime(service_date, "%Y-%m-%d")  # raises on invalid date


def parse_schedule_csv(
    text: str, service_date: str | None = None
) -> tuple[str, list[dict]]:
    if service_date:
        _validate_date(service_date)
    if not text or not text.strip():
        raise ScheduleError("CSV 内容为空")

    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise ScheduleError("CSV 缺少表头")
    col = _map_headers(list(reader.fieldnames))

    rows: list[dict] = []
    seen: set[tuple[str, int]] = set()
    inferred_date = service_date
    for line_no, raw in enumerate(reader, start=2):
        if raw is None:
            continue
        if all((c is None or str(c).strip() == "") for c in raw.values()):
            continue  # blank line
        try:
            trip_id = (raw.get(col["trip_id"]) or "").strip()
            if not trip_id:
                raise ScheduleError("trip_id 为空")
            seq_raw = (raw.get(col["stop_sequence"]) or "").strip()
            try:
                stop_sequence = int(seq_raw)
            except ValueError:
                raise ScheduleError(f"stop_sequence 非整数: {seq_raw!r}")
            stop_code = (raw.get(col["stop_code"]) or "").strip()
            if not stop_code:
                raise ScheduleError("站号为空")
            iso_time, row_date = parse_time(
                raw.get(col["scheduled_time"]) or "", inferred_date
            )
        except ScheduleError as exc:
            raise ScheduleError(f"第 {line_no} 行: {exc}") from exc
        if inferred_date is None:
            inferred_date = row_date
        key = (trip_id, stop_sequence)
        if key in seen:
            raise ScheduleError(
                f"第 {line_no} 行: trip_id={trip_id} stop_sequence="
                f"{stop_sequence} 重复"
            )
        seen.add(key)
        rows.append(
            {
                "trip_id": trip_id,
                "stop_sequence": stop_sequence,
                "stop_code": stop_code,
                "scheduled_time": iso_time,
            }
        )

    if not rows:
        raise ScheduleError("CSV 没有有效数据行")
    assert inferred_date is not None
    return inferred_date, rows
