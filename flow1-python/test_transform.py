import json
from transform import transform_cdc_event

sample_event = {
    "payload": {
        "before": None,
        "after": {
            "id": 1,
            "name": "Dara",
            "email": "dara@gmail.com",
            "age": 20
        },
        "op": "r",
        "ts_ms": 1795447879365,
        "source": {
            "db": "company_db",
            "table": "users"
        }
    }
}

result = transform_cdc_event(
    event=sample_event,
    kafka_offset=0,
    kafka_partition=0
)

assert result is not None
assert result["id"] == 1
assert result["operation"] == "r"

print("=== TRANSFORMED CDC EVENT ===")
print(json.dumps(result, indent=4, ensure_ascii=False))
print("TEST PASSED")
