import asyncio
import logging
from datetime import datetime, timedelta

from benedict import benedict
from grohe import GroheClient, GroheForbiddenError, GroheUnauthorizedError
from grohe.enum.grohe_enum import GroheGroupBy
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from custom_components.grohe_smarthome.dto.config_dtos import DeviceConfigDto
from custom_components.grohe_smarthome.dto.grohe_device import GroheDevice
from custom_components.grohe_smarthome.dto.notification_dto import Notification
from custom_components.grohe_smarthome.entities.interface.coordinator_button_interface import (
    CoordinatorButtonInterface,
)
from custom_components.grohe_smarthome.entities.interface.coordinator_config_interface import (
    CoordinatorConfigInterface,
)
from custom_components.grohe_smarthome.entities.interface.coordinator_interface import (
    CoordinatorInterface,
)
from custom_components.grohe_smarthome.entities.interface.coordinator_valve_interface import (
    CoordinatorValveInterface,
)

_LOGGER = logging.getLogger(__name__)

# The historical total-consumption query spans from install date to now (years, for an
# older installation) and has been observed to occasionally exceed the default request
# timeout. It's queried once a day at most, so a longer timeout here is cheap.
HISTORICAL_TOTAL_VALUE_TIMEOUT = 30.0


class GuardCoordinator(
    DataUpdateCoordinator,
    CoordinatorInterface,
    CoordinatorValveInterface,
    CoordinatorButtonInterface,
    CoordinatorConfigInterface,
):
    def __init__(
        self,
        hass: HomeAssistant,
        domain: str,
        device: GroheDevice,
        api: GroheClient,
        device_config: DeviceConfigDto | None = None,
        polling: int = 300,
        log_response_data: bool = False,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name="Grohe Sense",
            update_interval=timedelta(seconds=polling),
            always_update=True,
        )
        self._api = api
        self._domain = domain
        self._device = device
        self._total_value = 0
        self._total_value_update_day: datetime | None = None
        self._timezone = datetime.now().astimezone().tzinfo
        self._last_update = datetime.now().astimezone().replace(tzinfo=self._timezone)
        self._notifications: list[Notification] = []
        self._log_response_data = log_response_data
        self._has_pressure_measurement = False
        self._initial_value_lock = asyncio.Lock()
        self._storage = Store(
            self.hass,
            1,
            f"grohe_total_{self._device.appliance_id}.json",
        )
        self._restore_done = False

        if (
            device_config is not None
            and device_config.min_pressure_measurement_version is not None
        ):
            pressure_version = tuple(
                map(int, device_config.min_pressure_measurement_version.split(".")[:2])
            )
            if device.stripped_sw_version >= pressure_version:
                self._has_pressure_measurement = True

    async def _async_restore_total_value(self) -> None:
        if self._restore_done:
            return

        self._restore_done = True
        try:
            restored = await self._storage.async_load()
        except Exception as e:  # pragma: no cover - defensive only
            _LOGGER.warning(
                "Could not restore last valid total for %s (%s): %s",
                self._device.name,
                self._device.appliance_id,
                e,
            )
            return

        if restored is None:
            return

        restored_total = restored.get("total_value")
        if restored_total is not None:
            try:
                self._total_value = float(restored_total)
            except (TypeError, ValueError):
                self._total_value = 0.0

        restored_day = restored.get("total_value_update_day")
        if restored_day is not None:
            try:
                self._total_value_update_day = datetime.fromisoformat(restored_day)
            except ValueError:
                self._total_value_update_day = None

    async def _async_persist_total_value(self) -> None:
        payload = {
            "total_value": self._total_value,
            "total_value_update_day": (
                self._total_value_update_day.isoformat()
                if self._total_value_update_day is not None
                else None
            ),
        }
        await self._storage.async_save(payload)

    async def _get_total_value(
        self,
        date_from: datetime,
        date_to: datetime,
        group_by: GroheGroupBy,
        request_timeout: float | None = None,
    ) -> float:
        try:
            _LOGGER.debug(
                f"Getting total values for Grohe Sense Guard with appliance id {self._device.appliance_id}"
            )
            data_in = await self._api.get_appliance_data(
                self._device.location_id,
                self._device.room_id,
                self._device.appliance_id,
                date_from,
                date_to,
                group_by,
                True,
                timeout=request_timeout,
            )

            data = benedict(data_in)
            _LOGGER.debug(
                f"Got total values for Grohe Sense Guard for appliance with name {self._device.name}: {data}"
            )

            withdrawals = data.get("data.withdrawals")
            if withdrawals is not None and isinstance(withdrawals, list):
                # Handle None values in waterconsumption
                return sum([val.get("waterconsumption", 0) for val in withdrawals])

            return 0.0

        except (GroheUnauthorizedError, GroheForbiddenError):
            # Let auth/rate-limit errors propagate to _async_update_data, which knows how
            # to react properly (reauth, or keep last known data) - swallowing them here
            # would silently report 0.0 water consumption instead.
            raise

        except Exception as e:
            _LOGGER.error(f"Failed to get total values: {e}")
            raise

    async def _get_data(self) -> dict[str, any]:
        await self._async_restore_total_value()

        api_data = await self._api.get_appliance_details(
            self._device.location_id, self._device.room_id, self._device.appliance_id
        )

        pressure: None | dict[str, any] = None

        if self._has_pressure_measurement:
            pressure = await self._api.get_appliance_pressure_measurement(
                self._device.location_id,
                self._device.room_id,
                self._device.appliance_id,
            )

        now = datetime.now().astimezone()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        today_date = now.date()
        daily_total_fallback = float(
            (api_data.get("data_latest") or {}).get("daily_consumption", 0) or 0.0
        )

        # The daily sensor is useful as a sanity check for the current day, but it is not
        # a lifetime total and should not replace the historical baseline. We use it only to
        # guard against impossible jumps and to keep the last valid total stable across
        # restarts / updates / short API failures.
        today_water_consumption = await self._get_total_value(
            today_start,
            now,
            GroheGroupBy.DAY,
        )

        if (
            self._total_value_update_day is None
            or today_date > self._total_value_update_day.date()
        ):
            install_date = datetime.fromisoformat(api_data["installation_date"])
            historical_total = await self._get_total_value(
                install_date,
                today_start,
                GroheGroupBy.YEAR,
                request_timeout=HISTORICAL_TOTAL_VALUE_TIMEOUT,
            )

            # Use the historical total only when the API is healthy. If it comes back as 0 or
            # invalid during a temporary outage, keep the last known valid lifetime total.
            if historical_total > 0:
                self._total_value = max(round(historical_total, 2), 0)
                self._total_value_update_day = now
                await self._async_persist_total_value()
            elif self._total_value == 0 and daily_total_fallback > 0:
                # This is a very limited fallback only for the very first valid successful read.
                self._total_value = max(round(daily_total_fallback, 2), 0)
                self._total_value_update_day = now
                await self._async_persist_total_value()

        latest_data = api_data.get("data_latest") or {}
        _LOGGER.debug(
            f"Todays water consumption from appliance data: {today_water_consumption}. Absolute difference to daily_consumption is: {round(abs(today_water_consumption - latest_data.get('daily_consumption', 0)), 2)}"
        )
        _LOGGER.info(
            f"Water consumption for {self._device.appliance_id}: TOTAL TILL YESTERDAY - {round(self._total_value, 2)}l, TOTAL NOW - {round(self._total_value + today_water_consumption, 2)}l, TODAY - {round(today_water_consumption, 2)}l"
        )

        try:
            status = {val["type"]: val["value"] for val in api_data["status"]}
        except AttributeError as e:
            _LOGGER.debug(f"Status could not be mapped: {e}")
            status = None

        current_total = self._total_value + today_water_consumption
        if current_total < self._total_value and self._total_value_update_day is not None:
            _LOGGER.warning(
                "Ignoring lower-than-last total for %s (%s): current_total=%.2f last_total=%.2f",
                self._device.name,
                self._device.appliance_id,
                current_total,
                self._total_value,
            )
            current_total = self._total_value

        data = {
            "details": api_data,
            "status": status,
            "pressure": pressure,
            "total_water_consumption": current_total,
        }

        # Keep the total stable across restarts and updates by persisting the last valid
        # total after each successful update. The daily sensor remains the day-level value and
        # is used as a sanity check, not as the source of truth for the lifetime total.
        self._total_value = max(round(self._total_value, 2), 0)
        await self._async_persist_total_value()

        return data

    def _merge_details(self, api_data: dict[str, any] | None, key: str) -> None:
        """
        Merge a single sub-key (e.g. "config" or "command") from a write response into the
        coordinator's cached "details" and push it to every listening entity.

        Valve/Switch/Time/Number read their state straight from `coordinator.data["details"]`
        instead of polling their own endpoint (see `_get_data`, which already pulls the full
        appliance object - including `config` and `command` - out of the dashboard response
        that's fetched for the sensors anyway). After a write, the API's response for that
        write already contains the fresh sub-object, so we patch just that key into the
        cached details and notify listeners - no extra GET needed to see our own change.
        """
        if api_data is None or self.data is None:
            return

        value = api_data.get(key)
        if value is None:
            return

        old_value = (self.data.get("details") or {}).get(key) or {}
        if isinstance(value, dict) and isinstance(old_value, dict):
            changes = {
                field: (old_value.get(field), new)
                for field, new in value.items()
                if old_value.get(field) != new
            }
            if changes:
                changes_str = ", ".join(
                    f"{field}: {old} -> {new}" for field, (old, new) in changes.items()
                )
                _LOGGER.info(
                    "Grohe %s updated via PUT for %s (%s): %s",
                    key,
                    self._device.name,
                    self._device.appliance_id,
                    changes_str,
                )

        details = {**(self.data.get("details") or {}), key: value}
        self.async_set_updated_data({**self.data, "details": details})

    async def set_valve(self, data_to_set: dict[str, any]) -> dict[str, any]:
        api_data = await self._api.set_appliance_command(
            self._device.location_id,
            self._device.room_id,
            self._device.appliance_id,
            self._device.type,
            data_to_set,
        )

        self._merge_details(api_data, "command")

        return api_data

    async def set_config(self, data_to_set: dict[str, any]) -> dict[str, any] | None:
        config = data_to_set.get("config", {})
        api_data = await self._api.set_appliance_config(
            self._device.location_id,
            self._device.room_id,
            self._device.appliance_id,
            config,
        )

        self._merge_details(api_data, "config")

        return api_data

    async def send_command(self, data_to_send: dict[str, any]) -> dict[str, any]:
        api_data = await self._api.set_appliance_command(
            self._device.location_id,
            self._device.room_id,
            self._device.appliance_id,
            self._device.type,
            data_to_send,
        )

        return api_data

    async def _async_update_data(self) -> dict:
        try:
            _LOGGER.debug(
                f"Updating device data for device {self._device.type} with name {self._device.name} (appliance = {self._device.appliance_id})"
            )
            data = await self._get_data()

            if self._log_response_data:
                _LOGGER.debug(
                    f"Response data for {self._device.name} (appliance = {self._device.appliance_id}): {data}"
                )

            self._last_update = (
                datetime.now().astimezone().replace(tzinfo=self._timezone)
            )
            return data

        except GroheUnauthorizedError as e:
            raise ConfigEntryAuthFailed(str(e)) from e

        except GroheForbiddenError as e:
            _LOGGER.warning(
                "Grohe denied the request for %s (%s), keeping last known data: %s",
                self._device.name,
                self._device.appliance_id,
                e,
            )
            if self.data is not None:
                return self.data
            raise UpdateFailed(str(e)) from e

        except Exception as e:
            _LOGGER.error("Error updating Grohe Sense Guard data: %s", str(e))
            raise UpdateFailed(f"Error updating Grohe Sense Guard data: {e}") from e

    async def get_initial_value(self) -> dict[str, any]:
        # HA sets up all platforms (sensor, switch, valve, ...) concurrently, and each one
        # calls this during its own setup - a bare "if self.data is None" check isn't enough
        # to dedupe that, since several callers can all see None before the first one
        # finishes. The lock makes sure only one real fetch happens; the rest just wait for
        # it and reuse the result.
        async with self._initial_value_lock:
            if self.data is None:
                self.data = await self._get_data()
            return self.data

    def set_polling_interval(self, polling: int) -> None:
        self.update_interval = timedelta(seconds=polling)
        self.async_update_listeners()

    def set_log_response_data(self, log_response_data: bool) -> None:
        self._log_response_data = log_response_data
