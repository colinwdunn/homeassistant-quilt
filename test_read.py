"""Standalone read test for the Quilt client port.

Reads {refreshToken, systemId} from /tmp/quilt_test.json and prints the rooms,
validating auth + gRPC + parsing against the live Quilt API. Read-only.

Run from the project root:  .venv/bin/python test_read.py
"""
import json

from custom_components.quilt import api

cfg = json.load(open("/tmp/quilt_test.json"))
auth = api.CognitoAuth(cfg["refreshToken"])
client = api.QuiltClient(auth, cfg["systemId"])

rooms = client.get_rooms()
print(f"{len(rooms)} rooms:\n")
for r in rooms:
    print(f"  {r['name']:<22} on={r['on']!s:<5} "
          f"temp={r['current_temp']}  hum={r['humidity']}  "
          f"heat={r['heat_setpoint']} cool={r['cool_setpoint']}  "
          f"presets={list(r['presets'])}")
client.close()
