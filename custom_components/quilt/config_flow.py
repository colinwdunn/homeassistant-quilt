"""Config flow for Quilt: passwordless email-code login + system id."""
from __future__ import annotations

from typing import Any

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
            except api.QuiltAuthError:
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
                client = api.QuiltClient(api.CognitoAuth(refresh), system_id)
                rooms = await self.hass.async_add_executor_job(client.get_rooms)
                await self.hass.async_add_executor_job(client.close)
            except api.QuiltAuthError:
                errors["base"] = "invalid_auth"
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
