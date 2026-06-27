"""Constants for the Quilt integration."""

DOMAIN = "quilt"

# Config entry keys
CONF_REFRESH_TOKEN = "refresh_token"
CONF_SYSTEM_ID = "system_id"
CONF_EMAIL = "email"

# Polling
DEFAULT_SCAN_INTERVAL = 60  # seconds

# HVAC setpoint bounds (mirrors the Homebridge accessory clamps)
HEAT_MIN = 8.0
HEAT_MAX = 30.0
COOL_MIN = 10.0
COOL_MAX = 38.0

# Ignore polled values for this long after a user write so the UI doesn't
# snap back while Quilt's cloud catches up (HOLD_MS in accessory.js).
WRITE_HOLD_SECONDS = 18
