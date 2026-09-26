"""Config flow for Quilt: passwordless email-code login + system id."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any
import urllib.error

import grpc
import voluptuous as vol

from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
    OptionsFlow,
)
from homeassistant.core import callback
from homeassistant.helpers.selector import (
    NumberSelector,
    NumberSelectorConfig,
    NumberSelectorMode,
)

from . import api
from .const import (
    CONF_EMAIL,
    CONF_REFRESH_TOKEN,
    CONF_SCAN_INTERVAL,
    CONF_SYSTEM_ID,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
    MAX_SCAN_INTERVAL,
    MIN_SCAN_INTERVAL,
)


# Network trouble reaching Cognito or Quilt, as opposed to a rejected code.
_UNREACHABLE = (urllib.error.URLError, OSError)


async def _validate(hass, refresh: str, system_id: str) -> list[dict]:
    """Read the system once with a new login; returns its rooms."""
    client = api.QuiltClient(api.CognitoAuth(refresh), system_id)
    try:
        return await hass.async_add_executor_job(client.get_rooms)
    finally:
        await hass.async_add_executor_job(client.close)


class QuiltConfigFlow(ConfigFlow, domain=DOMAIN):
    """Email -> emailed code -> store refresh token + system id."""

    VERSION = 1

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> OptionsFlow:
        return QuiltOptionsFlow()

    def __init__(self) -> None:
        self._email: str | None = None
        self._session: str | None = None
        self._username: str | None = None

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            self._email = user_input[CONF_EMAIL]
            try:
                self._session, self._username = await self.hass.async_add_executor_job(
                    api.begin_email_login, self._email
                )
            except (api.QuiltAuthError, *_UNREACHABLE):
                errors["base"] = "cannot_connect"
            else:
                return await self.async_step_code()
        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({vol.Required(CONF_EMAIL): str}),
            errors=errors,
            description_placeholders={"info": "Quilt will email you a one-time code."},
        )

    async def async_step_code(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            code = user_input["code"].strip()
            system_id = user_input[CONF_SYSTEM_ID].strip()
            try:
                refresh = await self.hass.async_add_executor_job(
                    api.complete_email_login, self._session, self._username, code
                )
                # Validate the credentials by reading the system once.
                rooms = await _validate(self.hass, refresh, system_id)
            except (api.QuiltAuthError, grpc.RpcError):
                errors["base"] = "invalid_auth"
            except _UNREACHABLE:
                errors["base"] = "cannot_connect"
            else:
                if not rooms:
                    errors["base"] = "no_rooms"
                else:
                    await self.async_set_unique_id(system_id)
                    self._abort_if_unique_id_configured()
                    return self.async_create_entry(
                        title="Quilt",
                        data={
                            CONF_REFRESH_TOKEN: refresh,
                            CONF_SYSTEM_ID: system_id,
                            CONF_EMAIL: self._email,
                        },
                    )
        return self.async_show_form(
            step_id="code",
            data_schema=vol.Schema(
                {
                    vol.Required("code"): str,
                    vol.Required(CONF_SYSTEM_ID): str,
                }
            ),
            errors=errors,
        )


    # --- re-login when Quilt stops accepting the stored refresh token ----------
    async def async_step_reauth(
        self, entry_data: Mapping[str, Any]
    ) -> ConfigFlowResult:
        self._email = entry_data.get(CONF_EMAIL)
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            email = user_input[CONF_EMAIL].strip()
            try:
                self._session, self._username = await self.hass.async_add_executor_job(
                    api.begin_email_login, email
                )
            except (api.QuiltAuthError, *_UNREACHABLE):
                errors["base"] = "cannot_connect"
            else:
                self._email = email
                return await self.async_step_reauth_code()
        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=vol.Schema(
                {vol.Required(CONF_EMAIL, default=self._email or ""): str}
            ),
            errors=errors,
        )

    async def async_step_reauth_code(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        entry = self._get_reauth_entry()
        if user_input is not None:
            try:
                refresh = await self.hass.async_add_executor_job(
                    api.complete_email_login,
                    self._session,
                    self._username,
                    user_input["code"].strip(),
                )
                rooms = await _validate(self.hass, refresh, entry.data[CONF_SYSTEM_ID])
            except (api.QuiltAuthError, grpc.RpcError):
                errors["base"] = "invalid_auth"
            except _UNREACHABLE:
                errors["base"] = "cannot_connect"
            else:
                if not rooms:
                    errors["base"] = "no_rooms"
                else:
                    return self.async_update_reload_and_abort(
                        entry,
                        data_updates={CONF_REFRESH_TOKEN: refresh, CONF_EMAIL: self._email},
                    )
        return self.async_show_form(
            step_id="reauth_code",
            data_schema=vol.Schema({vol.Required("code"): str}),
            errors=errors,
            description_placeholders={"email": self._email or ""},
        )


class QuiltOptionsFlow(OptionsFlow):
    """Fallback poll interval (changes themselves arrive over the push stream)."""

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        if user_input is not None:
            return self.async_create_entry(
                data={CONF_SCAN_INTERVAL: int(user_input[CONF_SCAN_INTERVAL])}
            )
        current = self.config_entry.options.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL)
        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_SCAN_INTERVAL, default=current): NumberSelector(
                        NumberSelectorConfig(
                            min=MIN_SCAN_INTERVAL,
                            max=MAX_SCAN_INTERVAL,
                            step=1,
                            unit_of_measurement="s",
                            mode=NumberSelectorMode.BOX,
                        )
                    )
                }
            ),
        )
