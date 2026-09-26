# Quilt for Home Assistant

Custom integration that brings [Quilt](https://www.quilt.com) heat pumps into
Home Assistant as native `climate` entities (Quilt ships no HomeKit/Matter/local
API). Each Quilt room becomes a thermostat with the same modes as the Quilt app
(Off, Cool, Heat, Heat/Cool, Fan, Dry), current temperature, and setpoints.

Built by reverse-engineering Quilt's cloud API (AWS Cognito passwordless auth +
the `HomeDatastoreService` gRPC API), ported from the author's `homebridge-quilt`
plugin.

## Install (HACS)

1. HACS → ⋮ → **Custom repositories** → add this repo, category **Integration**.
2. Install **Quilt**, then restart Home Assistant.
3. **Settings → Devices & Services → Add Integration → Quilt.**

## Configuration

The config flow is passwordless:

1. Enter your **Quilt account email** → Quilt emails a one-time code.
2. Enter the **code** and your **Quilt System ID**.

Home Assistant obtains its own Cognito refresh token (independent of any other
client) and exposes each room as a `climate` entity.

If Quilt ever stops accepting that login, Home Assistant shows a **Reconfigure /
sign in again** prompt: confirm the email, enter the new code, and the
integration picks up where it left off.

## Entities

Per room (on the room's device):

- **Thermostat** (`climate`): Quilt's modes, setpoints and presets. The
  Heating / Cooling / Idle status comes from the unit itself.
- **Occupancy**: the indoor unit's presence sensor.
- **Humidity**: measured by the indoor unit.
- **Energy today** (kWh): Quilt's own metering for the room, from local
  midnight. Works with the Energy dashboard as an individual device.

On the Quilt Dial: **Temperature**, plus three disabled diagnostic channels whose
meaning isn't confirmed yet.

## Notes

- Requires `grpcio` (installed automatically via the manifest requirement).
- The generated protobuf/gRPC stubs are version-stamped to match the Home
  Assistant runtime; regenerate from `custom_components/quilt/quilt.proto` if
  upgrading.
- Cloud-dependent (talks to Quilt's cloud); no local API exists.
- Updates are pushed: the integration keeps Quilt's notifier stream open (the
  same one the Quilt app uses), so a change made in the app or on a Dial reaches
  Home Assistant within about a second, and room temperature, humidity and
  occupancy stay current. A poll every 60 seconds remains as a fallback (change
  it under the integration's **Configure** options), and the stream reconnects
  on its own if it drops. While the stream is healthy, one failed poll no
  longer marks the entities unavailable.
- Mode comes from Quilt's own mode field, so a mode set in the Quilt app or on a
  Dial shows up in Home Assistant as that mode. Versions before 0.3.0 inferred
  the mode from the setpoints and wrote every mode as Cool.
- 0.5.0 removed the Dial "humidity" sensor: that value was a circuit-board
  temperature (it tracked temperature, not the room's humidity).

## Development

```bash
pip install -r requirements_test.txt
pytest
```
