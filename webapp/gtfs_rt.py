"""GTFS-Realtime snapshot parsing and normalization.

Only the pieces required by the sign use-case are extracted:

* trip-level ``CANCELED`` -> whole-trip cancellation;
* per-stop *absolute* predicted arrival/departure epoch seconds
  (relative ``delay`` values are explicitly NOT used, per the
  "仅处理绝对预计时刻" rule);
* per-stop ``SKIPPED`` flags; ``NO_DATA`` stops carry no prediction and
  are dropped so the UI shows them as PENDING.

Entities that do not join to the loaded schedule are counted as ignored.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from google.transit import gtfs_realtime_pb2 as pb


class SnapshotParseError(ValueError):
    """Raised when the protobuf body cannot be parsed or is incomplete."""


@dataclass
class ParsedStop:
    stop_sequence: int
    arrival: int | None = None
    departure: int | None = None
    uncertainty: int | None = None
    sched_rel: str | None = None


@dataclass
class ParsedEntity:
    trip_id: str
    cancelled: bool = False
    route_id: str | None = None
    direction_id: int | None = None
    trip_start_time: str | None = None
    stops: list[ParsedStop] = field(default_factory=list)


@dataclass
class ParsedSnapshot:
    header_ts: int
    body_hash: str
    entities: list[ParsedEntity]
    total_entities: int
    ignored_count: int


def parse_snapshot(
    body: bytes,
    known_trip_ids: set[str],
    service_date: str | None = None,
) -> ParsedSnapshot:
    if not body:
        raise SnapshotParseError("empty response body")
    msg = pb.FeedMessage()
    try:
        msg.ParseFromString(body)
    except Exception as exc:  # protobuf DecodeError and friends
        raise SnapshotParseError(f"protobuf decode failed: {exc}") from exc
    if not msg.HasField("header"):
        raise SnapshotParseError("feed header missing")
    header_ts = int(msg.header.timestamp)
    if header_ts <= 0:
        raise SnapshotParseError("feed header.timestamp missing or invalid")

    compact_date = service_date.replace("-", "") if service_date else None
    entities: list[ParsedEntity] = []
    ignored = 0
    total = 0
    for ent in msg.entity:
        total += 1
        if not ent.HasField("trip_update"):
            # Vehicle positions / alerts / deleted entities do not affect
            # per-stop arrival predictions.
            ignored += 1
            continue
        tu = ent.trip_update
        if not tu.HasField("trip") or not tu.trip.trip_id:
            ignored += 1
            continue
        trip_id = tu.trip.trip_id
        if trip_id not in known_trip_ids:
            ignored += 1
            continue
        # Honor the single service day: an explicit foreign start_date
        # means the entity belongs to another day and must be ignored.
        if (
            compact_date
            and tu.trip.start_date
            and tu.trip.start_date != compact_date
        ):
            ignored += 1
            continue

        trip_sr = tu.trip.schedule_relationship
        parsed = ParsedEntity(
            trip_id=trip_id,
            cancelled=(trip_sr == pb.TripDescriptor.CANCELED),
            route_id=tu.trip.route_id or None,
            direction_id=(
                int(tu.trip.direction_id)
                if tu.trip.HasField("direction_id")
                else None
            ),
            trip_start_time=tu.trip.start_time or None,
        )
        if not parsed.cancelled:
            by_seq: dict[int, ParsedStop] = {}
            for stu in tu.stop_time_update:
                if not stu.HasField("stop_sequence"):
                    continue
                rel = pb.TripUpdate.StopTimeUpdate.SCHEDULED
                if stu.HasField("schedule_relationship"):
                    rel = stu.schedule_relationship
                rel_name = pb.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name(rel)
                if rel == pb.TripUpdate.StopTimeUpdate.NO_DATA:
                    # No prediction available: leave the stop PENDING.
                    continue
                arrival = (
                    int(stu.arrival.time)
                    if stu.HasField("arrival") and stu.arrival.time > 0
                    else None
                )
                departure = (
                    int(stu.departure.time)
                    if stu.HasField("departure") and stu.departure.time > 0
                    else None
                )
                uncertainty = (
                    int(stu.arrival.uncertainty)
                    if stu.HasField("arrival")
                    and stu.arrival.HasField("uncertainty")
                    else None
                )
                # Last update wins when a sequence appears more than once.
                by_seq[int(stu.stop_sequence)] = ParsedStop(
                    stop_sequence=int(stu.stop_sequence),
                    arrival=arrival,
                    departure=departure,
                    uncertainty=uncertainty,
                    sched_rel=rel_name,
                )
            parsed.stops = [by_seq[k] for k in sorted(by_seq)]
        entities.append(parsed)

    return ParsedSnapshot(
        header_ts=header_ts,
        body_hash=hashlib.sha256(body).hexdigest(),
        entities=entities,
        total_entities=total,
        ignored_count=ignored,
    )
