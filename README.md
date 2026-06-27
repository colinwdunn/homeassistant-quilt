# Quilt for Home Assistant

Custom integration that brings [Quilt](https://www.quilt.com) heat pumps into
Home Assistant as native `climate` entities (Quilt ships no HomeKit/Matter/local
API). Each Quilt room becomes a thermostat with OFF/HEAT/COOL/HEAT_COOL control,
current temperature, and setpoints.

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

## Notes

- Requires `grpcio` (installed automatically via the manifest requirement).
- The generated protobuf/gRPC stubs are version-stamped to match the Home
  Assistant runtime; regenerate from `custom_components/quilt/quilt.proto` if
  upgrading.
- Cloud-dependent (talks to Quilt's cloud); no local API exists.
