"""Quick smoke-test: parse config.example.yml and verify GPS filter logic."""

from src.config import load_config
from src.gps_filter import GpsFilter, GpsPoint
from datetime import datetime, timezone

cfg = load_config("config.example.yml")
print("MQTT:", cfg.mqtt.host, cfg.mqtt.port)
print("PG:", cfg.postgres.host, cfg.postgres.dbname)
print("GPS filter sats_min:", cfg.gps_filter.sats_min, "max_jump_m:", cfg.gps_filter.max_jump_m)

kpi = cfg.history_policy.kpi_map()
print("KPI addrs:", list(kpi.keys()))
for addr, k in kpi.items():
    print(f"  addr={addr} heartbeat={k.heartbeat_sec}s tolerance={k.tolerance}")

print("History defaults:", cfg.history_policy.defaults)
print("Retention: gps_raw={0}h history={1}d events={2}d".format(
    cfg.retention.gps_raw_hours, cfg.retention.history_days, cfg.retention.events_days))

# GPS filter test
flt = GpsFilter(cfg.gps_filter)
now = datetime.now(timezone.utc)

pt1 = GpsPoint(59.851624, 30.479838, 8, 1, now, now)
v1 = flt.check(pt1)
print(f"\nGPS test 1 (first point): accepted={v1.accepted} reason={v1.reject_reason}")
assert v1.accepted, "First point should be accepted"

pt2 = GpsPoint(55.751244, 37.618423, 10, 1, now, now)
v2 = flt.check(pt2)
print(f"GPS test 2 (teleport): accepted={v2.accepted} reason={v2.reject_reason}")
assert not v2.accepted, "Teleport should be rejected"
assert v2.reject_reason in ("jump_distance", "jump_speed")

pt3 = GpsPoint(59.851630, 30.479840, 8, 1, now, now)
v3 = flt.check(pt3)
print(f"GPS test 3 (close point): accepted={v3.accepted} reason={v3.reject_reason}")
assert v3.accepted, "Close point should be accepted"

pt4 = GpsPoint(59.851624, 30.479838, 2, 1, now, now)
v4 = flt.check(pt4)
print(f"GPS test 4 (low sats): accepted={v4.accepted} reason={v4.reject_reason}")
assert not v4.accepted, "Low sats should be rejected"
assert v4.reject_reason == "low_sats"

print("\nAll tests PASSED!")
