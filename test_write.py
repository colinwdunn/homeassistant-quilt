"""Safe write-path validation.

Exercises the full write RPC (fresh token -> UpdateSpace with concurrency token)
by sending an idempotent set_active(off) to a room that is ALREADY off. No heat
pump is physically actuated. Read-then-write-then-read to confirm state unchanged.

Run from project root:  .venv/bin/python test_write.py
"""
import json

from custom_components.quilt import api

cfg = json.load(open("/tmp/quilt_test.json"))
client = api.QuiltClient(api.CognitoAuth(cfg["refreshToken"]), cfg["systemId"])

rooms = client.get_rooms()
off = [r for r in rooms if not r["on"]]
if not off:
    print("No off rooms to safely test against; aborting (won't touch an active room).")
    raise SystemExit(0)

target = off[0]
print(f"target (already off): {target['name']}")
print("sending idempotent set_active(off) ...")
client.set_active(target["id"], False)
print("write RPC accepted (no error) ✓")

after = next(r for r in client.get_rooms() if r["id"] == target["id"])
print(f"after write: {after['name']} on={after['on']}  (expected on=False)")
client.close()
