"""Config flow (user, reauth, options) and the sensor / binary_sensor platforms.

The config flow's network calls (Cognito email login and the one-off gRPC read
that validates a new login) are replaced where config_flow.py reaches them:
through its `api` module attribute. Nothing here talks to Quilt.
"""
from __future__ import annotations

import copy
import time
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import urllib.error

import grpc
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from homeassistant import config_entries
from homeassistant.components.sensor import (
    ATTR_LAST_RESET,
    ATTR_STATE_CLASS,
    SensorDeviceClass,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import (
    ATTR_DEVICE_CLASS,
    ATTR_UNIT_OF_MEASUREMENT,
    STATE_OFF,
    STATE_ON,
    STATE_UNAVAILABLE,
    UnitOfEnergy,
    UnitOfTemperature,
)
from homeassistant.data_entry_flow import FlowResultType, InvalidData
from homeassistant.helpers import device_registry as dr, entity_registry as er
from homeassistant.util import dt as dt_util

from custom_components.quilt import api
from custom_components.quilt.const import (
    CONF_EMAIL,
    CONF_REFRESH_TOKEN,
    CONF_SCAN_INTERVAL,
    CONF_SYSTEM_ID,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
)

from .conftest import BEDROOM, DINING, LIVING, SYSTEM_ID

CF_API = "custom_components.quilt.config_flow.api"
EMAIL = "owner@example.com"
NEW_REFRESH = "new-refresh-token"


# --------------------------------------------------------------------------
# helpers / fixtures
# --------------------------------------------------------------------------

@pytest.fixture
def flow_api():
    """Patch the Cognito login calls and the validating QuiltClient used by the flow."""
    client = MagicMock(name="flow QuiltClient")
    client.get_rooms.return_value = [{"id": DINING, "name": "Dining Room"}]
    with patch(f"{CF_API}.begin_email_login", return_value=("session-1", "cognito-user")) as begin, \
         patch(f"{CF_API}.complete_email_login", return_value=NEW_REFRESH) as complete, \
         patch(f"{CF_API}.CognitoAuth") as cognito, \
         patch(f"{CF_API}.QuiltClient", return_value=client) as client_cls:
        yield SimpleNamespace(
            begin=begin, complete=complete, cognito=cognito, client_cls=client_cls, client=client
        )


@pytest.fixture
def mock_setup_entry():
    """Stop a created entry from actually setting up."""
    with patch("custom_components.quilt.async_setup_entry", return_value=True) as setup:
        yield setup


def _schema_default(result, key: str):
    for marker in result["data_schema"].schema:
        if marker == key:
            return marker.default()
    raise AssertionError(f"{key} not in schema")


async def _to_code_step(hass) -> dict:
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "user"
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_EMAIL: EMAIL}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "code"
    return result


async def _setup(hass, entry):
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    # Energy is read in a background task after the first poll.
    await hass.async_block_till_done(wait_background_tasks=True)
    return entry


def _eid(hass, domain: str, unique_id: str) -> str:
    entity_id = er.async_get(hass).async_get_entity_id(domain, DOMAIN, unique_id)
    assert entity_id is not None, f"no {domain} entity with unique_id {unique_id}"
    return entity_id


# --------------------------------------------------------------------------
# user flow
# --------------------------------------------------------------------------

async def test_user_flow_creates_entry(hass, flow_api, mock_setup_entry):
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "user"
    assert result["errors"] == {}

    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_EMAIL: EMAIL}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "code"
    assert result["errors"] == {}
    flow_api.begin.assert_called_once_with(EMAIL)

    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"code": " 123456 ", CONF_SYSTEM_ID: f" {SYSTEM_ID} "}
    )
    await hass.async_block_till_done()

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["title"] == "Quilt"
    assert result["data"] == {
        CONF_REFRESH_TOKEN: NEW_REFRESH,
        CONF_SYSTEM_ID: SYSTEM_ID,
        CONF_EMAIL: EMAIL,
    }
    assert result["result"].unique_id == SYSTEM_ID
    # The code and system id are trimmed; the session from step 1 is answered.
    flow_api.complete.assert_called_once_with("session-1", "cognito-user", "123456")
    # The new refresh token is validated against the system before saving.
    flow_api.cognito.assert_called_once_with(NEW_REFRESH)
    flow_api.client_cls.assert_called_once_with(flow_api.cognito.return_value, SYSTEM_ID)
    flow_api.client.get_rooms.assert_called_once()
    flow_api.client.close.assert_called_once()
    assert len(mock_setup_entry.mock_calls) == 1


@pytest.mark.parametrize(
    "exc",
    [
        api.QuiltAuthError("Cognito InitiateAuth 400: UserNotFoundException"),
        urllib.error.URLError("no route to host"),
        OSError("network down"),
    ],
    ids=["auth_error", "url_error", "os_error"],
)
async def test_user_step_errors_cannot_connect(hass, flow_api, mock_setup_entry, exc):
    flow_api.begin.side_effect = exc
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_EMAIL: EMAIL}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "user"
    assert result["errors"] == {"base": "cannot_connect"}

    # Recovers once Cognito answers.
    flow_api.begin.side_effect = None
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_EMAIL: EMAIL}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "code"


@pytest.mark.parametrize(
    ("complete_exc", "rooms", "error", "validated"),
    [
        (api.QuiltAuthError("login failed: {}"), None, "invalid_auth", False),
        (urllib.error.URLError("timed out"), None, "cannot_connect", False),
        # The code was accepted; Quilt refused this login for that system id.
        (None, grpc.RpcError(), "invalid_system", True),
        (None, OSError("connection reset"), "cannot_connect", True),
        (None, [], "no_rooms", True),
    ],
    ids=["bad_code", "cognito_unreachable", "rpc_error", "os_error", "no_rooms"],
)
async def test_code_step_errors(
    hass, flow_api, mock_setup_entry, complete_exc, rooms, error, validated
):
    flow_api.complete.side_effect = complete_exc
    if isinstance(rooms, BaseException):
        flow_api.client.get_rooms.side_effect = rooms
    elif rooms is not None:
        flow_api.client.get_rooms.return_value = rooms

    result = await _to_code_step(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"code": "123456", CONF_SYSTEM_ID: SYSTEM_ID}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "code"
    assert result["errors"] == {"base": error}
    assert hass.config_entries.async_entries(DOMAIN) == []
    if validated:
        # The validating client is closed even when the read fails.
        flow_api.client_cls.assert_called_once()
        flow_api.client.close.assert_called_once()
    else:
        flow_api.client_cls.assert_not_called()


async def test_code_step_recovers_after_error(hass, flow_api, mock_setup_entry):
    """A code submit that never reached Cognito can be retried on the same form."""
    flow_api.complete.side_effect = [urllib.error.URLError("timed out"), NEW_REFRESH]
    result = await _to_code_step(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"code": "123456", CONF_SYSTEM_ID: SYSTEM_ID}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "cannot_connect"}

    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"code": "123456", CONF_SYSTEM_ID: SYSTEM_ID}
    )
    await hass.async_block_till_done()
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"][CONF_REFRESH_TOKEN] == NEW_REFRESH
    assert result["data"][CONF_SYSTEM_ID] == SYSTEM_ID
    flow_api.client.close.assert_called_once()


async def test_code_step_bad_system_id_retry_keeps_login(hass, flow_api, mock_setup_entry):
    """After 'invalid_system' the code was already accepted: fixing the system id
    and resubmitting must not answer the (single-use) Cognito session again."""
    flow_api.client.get_rooms.side_effect = [grpc.RpcError(), [{"id": DINING, "name": "Dining Room"}]]
    result = await _to_code_step(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"code": "123456", CONF_SYSTEM_ID: "wrong-system"}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "invalid_system"}

    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"code": "123456", CONF_SYSTEM_ID: SYSTEM_ID}
    )
    await hass.async_block_till_done()
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"][CONF_REFRESH_TOKEN] == NEW_REFRESH
    assert result["data"][CONF_SYSTEM_ID] == SYSTEM_ID
    # One Cognito answer; the kept login was validated against each system id.
    flow_api.complete.assert_called_once_with("session-1", "cognito-user", "123456")
    assert [c.args[1] for c in flow_api.client_cls.call_args_list] == ["wrong-system", SYSTEM_ID]


async def test_code_step_rejected_code_retries_with_new_session(
    hass, flow_api, mock_setup_entry
):
    """Cognito answers a wrong code with a fresh session; the retry must use it."""
    flow_api.complete.side_effect = [
        api.QuiltAuthError("that code wasn't accepted", session="session-2"),
        NEW_REFRESH,
    ]
    result = await _to_code_step(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"code": "000000", CONF_SYSTEM_ID: SYSTEM_ID}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "invalid_auth"}

    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"code": "123456", CONF_SYSTEM_ID: SYSTEM_ID}
    )
    await hass.async_block_till_done()
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert [c.args for c in flow_api.complete.call_args_list] == [
        ("session-1", "cognito-user", "000000"),
        ("session-2", "cognito-user", "123456"),
    ]


async def test_user_step_trims_email(hass, flow_api, mock_setup_entry):
    """Regression: the user step trims the email (like reauth_confirm does) before
    sending it to Cognito and storing it, so ' owner@example.com ' from autofill
    or a mobile keyboard is not a different username."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_EMAIL: f" {EMAIL} "}
    )
    assert result["step_id"] == "code"
    flow_api.begin.assert_called_once_with(EMAIL)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"code": "123456", CONF_SYSTEM_ID: SYSTEM_ID}
    )
    await hass.async_block_till_done()
    assert result["data"][CONF_EMAIL] == EMAIL


async def test_duplicate_system_aborts(hass, flow_api, mock_setup_entry, config_entry):
    config_entry.add_to_hass(hass)
    result = await _to_code_step(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"code": "123456", CONF_SYSTEM_ID: SYSTEM_ID}
    )
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"
    assert len(hass.config_entries.async_entries(DOMAIN)) == 1
    # The existing entry keeps its own token.
    assert config_entry.data[CONF_REFRESH_TOKEN] == "refresh-token"
    flow_api.client.close.assert_called_once()
    mock_setup_entry.assert_not_called()


# --------------------------------------------------------------------------
# reauth
# --------------------------------------------------------------------------

async def test_reauth_replaces_token_and_reloads(
    hass, setup_integration, fake_stream, mock_client, flow_api
):
    entry = setup_integration
    old_coordinator = entry.runtime_data
    old_stream = fake_stream.instance

    with patch("custom_components.quilt.coordinator.CognitoAuth") as coordinator_auth:
        result = await entry.start_reauth_flow(hass)
        assert result["type"] is FlowResultType.FORM
        assert result["step_id"] == "reauth_confirm"
        assert _schema_default(result, CONF_EMAIL) == EMAIL

        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_EMAIL: f" {EMAIL} "}
        )
        assert result["type"] is FlowResultType.FORM
        assert result["step_id"] == "reauth_code"
        assert result["description_placeholders"]["email"] == EMAIL
        flow_api.begin.assert_called_once_with(EMAIL)

        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {"code": " 654321 "}
        )
        assert result["type"] is FlowResultType.ABORT
        assert result["reason"] == "reauth_successful"
        await hass.async_block_till_done(wait_background_tasks=True)

    flow_api.complete.assert_called_once_with("session-1", "cognito-user", "654321")
    # Validated against the entry's own system.
    flow_api.client_cls.assert_called_once_with(flow_api.cognito.return_value, SYSTEM_ID)
    flow_api.client.close.assert_called_once()

    assert entry.data == {
        CONF_REFRESH_TOKEN: NEW_REFRESH,
        CONF_SYSTEM_ID: SYSTEM_ID,
        CONF_EMAIL: EMAIL,
    }
    # Reloaded: the old coordinator's stream and client are shut down and a new
    # coordinator logs in with the new token.
    assert entry.state is ConfigEntryState.LOADED
    assert entry.runtime_data is not old_coordinator
    assert old_stream.stopped
    assert fake_stream.instance is not old_stream and fake_stream.instance.started
    mock_client.close.assert_called()
    coordinator_auth.assert_called_once_with(NEW_REFRESH)
    assert hass.config_entries.flow.async_progress_by_handler(DOMAIN) == []


async def test_reauth_with_patched_setup_updates_entry(
    hass, config_entry, flow_api, mock_setup_entry
):
    """Same flow without a live entry: the entry is (re)loaded after the update."""
    config_entry.add_to_hass(hass)
    result = await config_entry.start_reauth_flow(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_EMAIL: "changed@example.com"}
    )
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"code": "111111"}
    )
    await hass.async_block_till_done()
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reauth_successful"
    assert config_entry.data[CONF_REFRESH_TOKEN] == NEW_REFRESH
    assert config_entry.data[CONF_EMAIL] == "changed@example.com"
    assert config_entry.data[CONF_SYSTEM_ID] == SYSTEM_ID
    flow_api.begin.assert_called_once_with("changed@example.com")
    assert len(mock_setup_entry.mock_calls) == 1


@pytest.mark.parametrize(
    ("complete_exc", "rooms", "error"),
    [
        (api.QuiltAuthError("login failed: {}"), None, "invalid_auth"),
        # The code was accepted but the login can't read the entry's system.
        (None, grpc.RpcError(), "invalid_system"),
        (None, OSError("connection reset"), "cannot_connect"),
        (None, [], "no_rooms"),
    ],
    ids=["bad_code", "rpc_error", "os_error", "no_rooms"],
)
async def test_reauth_code_errors_keep_form(
    hass, config_entry, flow_api, mock_setup_entry, complete_exc, rooms, error
):
    flow_api.complete.side_effect = complete_exc
    if isinstance(rooms, BaseException):
        flow_api.client.get_rooms.side_effect = rooms
    elif rooms is not None:
        flow_api.client.get_rooms.return_value = rooms

    config_entry.add_to_hass(hass)
    result = await config_entry.start_reauth_flow(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_EMAIL: EMAIL}
    )
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"code": "000000"}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "reauth_code"
    assert result["errors"] == {"base": error}
    assert result["description_placeholders"]["email"] == EMAIL
    # The stored login is untouched and nothing was reloaded.
    assert config_entry.data[CONF_REFRESH_TOKEN] == "refresh-token"
    mock_setup_entry.assert_not_called()
    if rooms is not None:
        flow_api.client.close.assert_called_once()


@pytest.mark.parametrize(
    "exc",
    [api.QuiltAuthError("Cognito 400"), urllib.error.URLError("timed out")],
    ids=["auth_error", "url_error"],
)
async def test_reauth_confirm_errors_keep_form(
    hass, config_entry, flow_api, mock_setup_entry, exc
):
    flow_api.begin.side_effect = exc
    config_entry.add_to_hass(hass)
    result = await config_entry.start_reauth_flow(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_EMAIL: EMAIL}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "reauth_confirm"
    assert result["errors"] == {"base": "cannot_connect"}


async def test_reauth_without_stored_email(hass, flow_api, mock_setup_entry):
    entry = MockConfigEntry(
        domain=DOMAIN,
        title="Quilt",
        unique_id=SYSTEM_ID,
        data={CONF_REFRESH_TOKEN: "refresh-token", CONF_SYSTEM_ID: SYSTEM_ID},
    )
    entry.add_to_hass(hass)
    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": config_entries.SOURCE_REAUTH, "entry_id": entry.entry_id},
        data=dict(entry.data),
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "reauth_confirm"
    assert _schema_default(result, CONF_EMAIL) == ""

    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_EMAIL: EMAIL}
    )
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"code": "222222"}
    )
    await hass.async_block_till_done()
    assert result["reason"] == "reauth_successful"
    # The email is stored from now on.
    assert entry.data[CONF_EMAIL] == EMAIL
    assert entry.data[CONF_REFRESH_TOKEN] == NEW_REFRESH


async def test_revoked_token_starts_reauth(hass, setup_integration, mock_client):
    """A poll rejected as revoked opens the reauth flow with the stored email."""
    entry = setup_integration
    mock_client.get_system.side_effect = api.QuiltAuthError(
        "Cognito InitiateAuth 400: NotAuthorizedException Refresh Token has been revoked"
    )
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    flows = hass.config_entries.flow.async_progress_by_handler(DOMAIN)
    assert len(flows) == 1
    assert flows[0]["context"]["source"] == config_entries.SOURCE_REAUTH
    assert flows[0]["step_id"] == "reauth_confirm"


class _TokenClient:
    """A coordinator client whose reads run in the real executor, like the gRPC client.

    (The test harness runs Mock targets inline on the event loop, which would
    hide any ordering that depends on a network round trip.)
    """

    def __init__(self, auth, system, energy, revoked) -> None:
        self.token = auth.token
        self._system, self._energy, self._revoked = system, energy, revoked
        self.polls = 0
        self.closed = False

    def get_system(self) -> dict:
        self.polls += 1
        if self.token in self._revoked:
            time.sleep(0.2)  # the Cognito round trip that rejects the token
            raise api.QuiltAuthError(
                "Cognito InitiateAuth 400: NotAuthorizedException Refresh Token has been revoked"
            )
        return copy.deepcopy(self._system)

    def get_energy(self, since: float, until: float) -> dict[str, list[tuple[int, float]]]:
        # One hourly bucket per room starting "now", like conftest's mock_client.
        return {sid: [(int(until) - 3600, kwh)] for sid, kwh in self._energy.items()}

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def per_token_clients(system, energy):
    """Coordinator clients that fail as revoked unless built with an accepted token."""
    revoked: set[str] = set()

    def make_client(auth, system_id):
        return _TokenClient(auth, system, energy, revoked)

    with patch("custom_components.quilt.coordinator.QuiltClient", side_effect=make_client), \
         patch(
             "custom_components.quilt.coordinator.CognitoAuth",
             side_effect=lambda token: SimpleNamespace(token=token),
         ):
        yield revoked


async def _legacy_options_listener(hass, entry) -> None:
    """What v0.4's update listener (_async_options_updated) did on any entry update."""
    coordinator = entry.runtime_data
    coordinator.update_interval = timedelta(seconds=DEFAULT_SCAN_INTERVAL)
    await coordinator.async_request_refresh()


async def test_reauth_success_leaves_no_follow_up_reauth(
    hass, config_entry, fake_stream, per_token_clients, flow_api
):
    """Regression: the entry has no update listener any more, so the reauth's
    entry-data update doesn't make the OLD coordinator poll with the revoked
    token during the reload (whose ConfigEntryAuthFailed used to re-open a
    reauth flow right after 'reauth_successful')."""
    revoked = per_token_clients
    entry = await _setup(hass, config_entry)
    old_coordinator = entry.runtime_data
    old_client = old_coordinator.client

    # Quilt revokes the stored token: the next poll fails and HA asks to re-login.
    revoked.add("refresh-token")
    await old_coordinator.async_refresh()
    await hass.async_block_till_done()
    flows = hass.config_entries.flow.async_progress_by_handler(DOMAIN)
    assert [f["context"]["source"] for f in flows] == [config_entries.SOURCE_REAUTH]
    old_polls = old_client.polls
    assert entry.update_listeners == []

    result = await hass.config_entries.flow.async_configure(
        flows[0]["flow_id"], {CONF_EMAIL: EMAIL}
    )
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"code": "333333"}
    )
    assert result["reason"] == "reauth_successful"
    await hass.async_block_till_done(wait_background_tasks=True)

    # The new login works ...
    assert entry.state is ConfigEntryState.LOADED
    assert entry.runtime_data is not old_coordinator
    assert entry.runtime_data.client.token == NEW_REFRESH
    assert entry.runtime_data.last_update_success
    # ... so nothing should ask the user to sign in again, and the retired
    # coordinator should not have polled with the revoked token.
    remaining = hass.config_entries.flow.async_progress_by_handler(DOMAIN)
    assert remaining == [], f"reauth re-opened after success: {remaining}"
    assert old_client.polls == old_polls


async def test_reauth_follow_up_positive_control_with_legacy_listener(
    hass, config_entry, fake_stream, per_token_clients, flow_api, caplog
):
    """Positive control for the two regression tests around it: re-attaching
    v0.4's update listener to the entry brings back the stray poll by the retired
    coordinator (with the revoked token), the follow-up reauth flow and HA's
    deprecation warning. Shows this harness can see all three, so their absence
    in those tests is evidence rather than a blind spot."""
    revoked = per_token_clients
    entry = await _setup(hass, config_entry)
    old_client = entry.runtime_data.client
    entry.async_on_unload(entry.add_update_listener(_legacy_options_listener))
    revoked.add("refresh-token")
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    (flow,) = hass.config_entries.flow.async_progress_by_handler(DOMAIN)
    old_polls = old_client.polls

    result = await hass.config_entries.flow.async_configure(
        flow["flow_id"], {CONF_EMAIL: EMAIL}
    )
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"code": "333333"}
    )
    assert result["reason"] == "reauth_successful"
    await hass.async_block_till_done(wait_background_tasks=True)

    assert entry.state is ConfigEntryState.LOADED
    assert entry.runtime_data.client.token == NEW_REFRESH
    assert old_client.polls > old_polls
    remaining = hass.config_entries.flow.async_progress_by_handler(DOMAIN)
    assert [f["context"]["source"] for f in remaining] == [config_entries.SOURCE_REAUTH]
    assert "should use it for scheduling a reload" in caplog.text


async def test_reauth_does_not_trip_update_listener_deprecation(
    hass, setup_integration, flow_api, caplog
):
    """Regression: with no update listener on the entry, finishing reauth with
    async_update_reload_and_abort doesn't log HA's 'has an update listener and
    should use it for scheduling a reload' deprecation (breaks in 2026.12)."""
    entry = setup_integration
    assert entry.update_listeners == []
    result = await entry.start_reauth_flow(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_EMAIL: EMAIL}
    )
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"code": "444444"}
    )
    assert result["reason"] == "reauth_successful"
    await hass.async_block_till_done(wait_background_tasks=True)
    assert entry.state is ConfigEntryState.LOADED
    assert "should use it for scheduling a reload" not in caplog.text


# --------------------------------------------------------------------------
# options
# --------------------------------------------------------------------------

async def test_options_flow_changes_poll_interval_without_reload(
    hass, setup_integration, fake_stream, mock_client
):
    entry = setup_integration
    coordinator = entry.runtime_data
    stream = fake_stream.instance
    assert coordinator.update_interval == timedelta(seconds=DEFAULT_SCAN_INTERVAL)

    result = await hass.config_entries.options.async_init(entry.entry_id)
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "init"
    assert _schema_default(result, CONF_SCAN_INTERVAL) == DEFAULT_SCAN_INTERVAL

    polls_before = mock_client.get_system.call_count
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {CONF_SCAN_INTERVAL: 120}
    )
    await hass.async_block_till_done()
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options == {CONF_SCAN_INTERVAL: 120}
    assert isinstance(entry.options[CONF_SCAN_INTERVAL], int)

    assert coordinator.update_interval == timedelta(seconds=120)
    # No reload: same coordinator, same stream still running.
    assert entry.state is ConfigEntryState.LOADED
    assert entry.runtime_data is coordinator
    assert fake_stream.instance is stream and not stream.stopped
    # The options flow applies it to the running coordinator itself (there is
    # no update listener) and polls once so the new schedule starts now.
    assert entry.update_listeners == []
    assert mock_client.get_system.call_count == polls_before + 1

    # Reopening shows the saved value.
    result = await hass.config_entries.options.async_init(entry.entry_id)
    assert _schema_default(result, CONF_SCAN_INTERVAL) == 120
    await hass.config_entries.options.async_configure(
        result["flow_id"], {CONF_SCAN_INTERVAL: 120}
    )
    await hass.async_block_till_done()


@pytest.mark.parametrize("value", [5, 7200])
async def test_options_flow_rejects_out_of_range(hass, setup_integration, value):
    entry = setup_integration
    result = await hass.config_entries.options.async_init(entry.entry_id)
    with pytest.raises(InvalidData):
        await hass.config_entries.options.async_configure(
            result["flow_id"], {CONF_SCAN_INTERVAL: value}
        )
    assert entry.options == {}
    assert entry.runtime_data.update_interval == timedelta(seconds=DEFAULT_SCAN_INTERVAL)


# --------------------------------------------------------------------------
# sensors
# --------------------------------------------------------------------------

async def test_room_humidity_values(hass, config_entry, mock_client, fake_stream, system):
    system["rooms"][DINING]["humidity"] = 48
    system["rooms"][LIVING]["humidity"] = 55
    system["rooms"][BEDROOM]["humidity"] = 61
    await _setup(hass, config_entry)

    expected = {DINING: "48", LIVING: "55", BEDROOM: "61"}
    for room_id, value in expected.items():
        state = hass.states.get(_eid(hass, "sensor", f"quilt_{room_id}_humidity"))
        assert state.state == value
        assert state.attributes[ATTR_DEVICE_CLASS] == SensorDeviceClass.HUMIDITY
        assert state.attributes[ATTR_UNIT_OF_MEASUREMENT] == "%"
        assert state.attributes[ATTR_STATE_CLASS] == SensorStateClass.MEASUREMENT

    # A pushed head reading updates just that room.
    fake_stream.instance.on_events(
        [{"kind": "unit", "space_id": LIVING, "unit_id": f"unit-{LIVING}", "humidity": 58}]
    )
    await hass.async_block_till_done()
    assert hass.states.get(_eid(hass, "sensor", f"quilt_{LIVING}_humidity")).state == "58"
    assert hass.states.get(_eid(hass, "sensor", f"quilt_{DINING}_humidity")).state == "48"


async def test_room_without_humidity_gets_no_humidity_sensor(
    hass, config_entry, mock_client, fake_stream, system
):
    system["rooms"][DINING]["humidity"] = None
    await _setup(hass, config_entry)
    registry = er.async_get(hass)
    assert registry.async_get_entity_id("sensor", DOMAIN, f"quilt_{DINING}_humidity") is None
    assert registry.async_get_entity_id("sensor", DOMAIN, f"quilt_{LIVING}_humidity")


async def test_energy_today_sensors(hass, config_entry, mock_client, fake_stream, energy):
    energy.update({DINING: 0.86349, LIVING: 1.2666, BEDROOM: 3.31})
    await _setup(hass, config_entry)

    # One bucketed read covering today, from local midnight.
    mock_client.get_energy.assert_called_once()
    since, until = mock_client.get_energy.call_args.args
    assert since == dt_util.start_of_local_day().timestamp()
    assert until > since

    expected = {DINING: "0.863", LIVING: "1.267", BEDROOM: "3.31"}
    for room_id, value in expected.items():
        state = hass.states.get(_eid(hass, "sensor", f"quilt_{room_id}_energy_today"))
        assert state.state == value
        assert state.attributes[ATTR_DEVICE_CLASS] == SensorDeviceClass.ENERGY
        assert state.attributes[ATTR_STATE_CLASS] == SensorStateClass.TOTAL
        assert state.attributes[ATTR_UNIT_OF_MEASUREMENT] == UnitOfEnergy.KILO_WATT_HOUR
        assert state.attributes[ATTR_LAST_RESET] == dt_util.start_of_local_day().isoformat()


async def test_energy_stays_available_when_poll_fails(
    hass, config_entry, mock_client, fake_stream
):
    entry = await _setup(hass, config_entry)
    energy_id = _eid(hass, "sensor", f"quilt_{DINING}_energy_today")
    humidity_id = _eid(hass, "sensor", f"quilt_{DINING}_humidity")
    assert hass.states.get(energy_id).state == "0.863"
    assert hass.states.get(humidity_id).state == "52"

    mock_client.get_system.side_effect = OSError("network down")
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert not entry.runtime_data.last_update_success
    assert not fake_stream.instance.healthy

    # Control: live telemetry does go unavailable on the failed poll ...
    assert hass.states.get(humidity_id).state == STATE_UNAVAILABLE
    # ... while the cloud-metered energy total keeps its last value.
    state = hass.states.get(energy_id)
    assert state.state == "0.863"
    assert state.attributes[ATTR_LAST_RESET] == dt_util.start_of_local_day().isoformat()


async def test_energy_unavailable_until_first_read(
    hass, config_entry, mock_client, fake_stream
):
    mock_client.get_energy.side_effect = OSError("energy endpoint down")
    await _setup(hass, config_entry)
    # Control: the failing read was actually attempted.
    mock_client.get_energy.assert_called_once()
    for room_id in (DINING, LIVING, BEDROOM):
        state = hass.states.get(_eid(hass, "sensor", f"quilt_{room_id}_energy_today"))
        assert state.state == STATE_UNAVAILABLE
    # Room telemetry is unaffected by the energy failure.
    assert hass.states.get(_eid(hass, "sensor", f"quilt_{DINING}_humidity")).state == "52"


async def test_dial_temperature_sensor(hass, setup_integration):
    entity_id = _eid(hass, "sensor", "quilt_dial_temperature")
    state = hass.states.get(entity_id)
    assert state.state == "25.2"
    assert state.attributes[ATTR_DEVICE_CLASS] == SensorDeviceClass.TEMPERATURE
    assert state.attributes[ATTR_UNIT_OF_MEASUREMENT] == UnitOfTemperature.CELSIUS

    entity = er.async_get(hass).async_get(entity_id)
    device = dr.async_get(hass).async_get(entity.device_id)
    assert device.name == "Quilt Dial"
    assert device.manufacturer == "Quilt"
    assert device.model == "Dial"
    assert device.serial_number == "QD1-0B000VG2S"
    assert (DOMAIN, "dial") in device.identifiers


async def test_no_dial_humidity_sensor(hass, setup_integration):
    registry = er.async_get(hass)
    assert registry.async_get_entity_id("sensor", DOMAIN, "quilt_dial_humidity") is None

    dial_entity = registry.async_get(_eid(hass, "sensor", "quilt_dial_temperature"))
    on_dial = er.async_entries_for_device(
        registry, dial_entity.device_id, include_disabled_entities=True
    )
    assert {e.unique_id for e in on_dial} == {
        "quilt_dial_temperature",
        "quilt_dial_ambient_1",
        "quilt_dial_ambient_2",
        "quilt_dial_ambient_3",
    }
    assert all(
        e.original_device_class != SensorDeviceClass.HUMIDITY for e in on_dial
    )
    # The unidentified channels exist but are disabled diagnostics.
    for e in on_dial:
        if e.unique_id.startswith("quilt_dial_ambient"):
            assert e.disabled_by is er.RegistryEntryDisabler.INTEGRATION
            assert hass.states.get(e.entity_id) is None
    assert not [
        s.entity_id for s in hass.states.async_all("sensor")
        if s.attributes.get(ATTR_DEVICE_CLASS) == SensorDeviceClass.HUMIDITY
        and "dial" in s.entity_id
    ]


async def test_occupancy_binary_sensors(hass, setup_integration, fake_stream, system, mock_client):
    entry = setup_integration
    ids = {room: _eid(hass, "binary_sensor", f"quilt_{room}_occupancy") for room in (DINING, LIVING, BEDROOM)}
    assert hass.states.get(ids[BEDROOM]).state == STATE_ON
    assert hass.states.get(ids[DINING]).state == STATE_OFF
    assert hass.states.get(ids[LIVING]).state == STATE_OFF
    assert hass.states.get(ids[BEDROOM]).attributes[ATTR_DEVICE_CLASS] == "occupancy"

    # Pushed presence from a head unit.
    fake_stream.instance.on_events(
        [{"kind": "unit", "space_id": DINING, "unit_id": f"unit-{DINING}", "occupied": True},
         {"kind": "unit", "space_id": BEDROOM, "unit_id": f"unit-{BEDROOM}", "occupied": False}]
    )
    await hass.async_block_till_done()
    assert hass.states.get(ids[DINING]).state == STATE_ON
    assert hass.states.get(ids[BEDROOM]).state == STATE_OFF

    # A poll picks it up too.
    system["rooms"][LIVING]["occupied"] = True
    system["rooms"][DINING]["occupied"] = True
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    assert hass.states.get(ids[LIVING]).state == STATE_ON

    # Room devices carry the occupancy sensor alongside the climate entity.
    registry = er.async_get(hass)
    occ = registry.async_get(ids[DINING])
    climate_id = _eid(hass, "climate", f"quilt_{DINING}")
    assert registry.async_get(climate_id).device_id == occ.device_id
