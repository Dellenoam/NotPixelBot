import asyncio
import io
import json
import random
import re
import sys
import traceback
from datetime import datetime, timedelta, timezone
from time import time
from typing import Any, Dict, List, NoReturn
from uuid import uuid4

import aiofiles
import aiohttp
import cv2
import numpy as np
from aiohttp_socks import ProxyConnector
from PIL import Image
from pyrogram.client import Client

from bot.config.config import settings
from bot.core.ad_watcher import AdWatcher
from bot.core.canvas_updater.dynamic_canvas_renderer import DynamicCanvasRenderer
from bot.core.canvas_updater.websocket_manager import WebSocketManager
from bot.core.notpx_api_checker import NotPXAPIChecker
from bot.core.tg_mini_app_auth import TelegramMiniAppAuth
from bot.utils.logger import dev_logger, logger


class NotPXBot:
    RETRY_ITERATION_DELAY = 10 * 60  # 10 minutes
    RETRY_DELAY = 5  # 5 seconds

    def __init__(
        self, telegram_client: Client, websocket_manager: WebSocketManager
    ) -> None:
        self.telegram_client: Client = telegram_client
        self.session_name: str = telegram_client.name
        self._websocket_manager: WebSocketManager = websocket_manager
        self._canvas_renderer: DynamicCanvasRenderer = DynamicCanvasRenderer()
        self._notpx_api_checker: NotPXAPIChecker = NotPXAPIChecker()
        self._headers = self._create_headers()
        self.is_template_reselected: bool = False
        self.template_url: str = ""
        self.template_x: int = 0
        self.template_y: int = 0
        self.template_size: int = 0
        self.max_boosts: Dict[str, int] = {
            "paintReward": 7,
            "reChargeSpeed": 11,
            "energyLimit": 7,
        }
        self.boost_prices: Dict[str, Dict[int, int]] = {
            "paintReward": {2: 5, 3: 100, 4: 200, 5: 300, 6: 500, 7: 600},
            "reChargeSpeed": {
                2: 5,
                3: 100,
                4: 200,
                5: 300,
                6: 400,
                7: 500,
                8: 600,
                9: 700,
                10: 800,
                11: 900,
            },
            "energyLimit": {2: 5, 3: 100, 4: 200, 5: 300, 6: 400, 7: 10},
        }
        self._tasks_list: Dict[str, Dict[str, Any]] = {
            "x_tasks_list": {
                "x:notpixel": "notpixel",
                "x:notcoin": "notcoin",
            },
            "channel_tasks_list": {
                "channel:notpixel_channel": "notpixel_channel",
                "channel:notcoin": "notcoin",
            },
            "league_tasks_list": {
                "leagueBonusSilver": "leagueBonusSilver",
                "leagueBonusGold": "leagueBonusGold",
                "leagueBonusPlatinum": "leagueBonusPlatinum",
            },
            "click_tasks_list": {
                "earnCoin": {
                    "event_name": "task_earncoin",
                    "name": "earnCoin",
                    "reward": 512,
                }
            },
        }
        self._tasks_to_complete: Dict[str, Dict[str, Any]] = {}
        self._league_weights: Dict[str, int] = {
            "bronze": 0,
            "silver": 1,
            "gold": 2,
            "platinum": 3,
        }
        self._quests_list: List[str] = settings.SECRET_WORDS
        self._quests_to_complete: List[str] = []

    def _create_headers(self) -> Dict[str, Dict[str, str]]:
        base_headers = {
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://app.notpx.app",
            "Referer": "https://app.notpx.app/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "",
        }

        websocket_headers = {
            "Cache-Control": "no-cache",
            "Origin": "https://notpx.app",
            "Pragma": "no-cache",
            "Sec-Fetch-Dest": "websocket",
            "Sec-Fetch-Mode": "websocket",
            "Sec-Fetch-Site": "same-site",
        }

        def create_headers(
            additional_headers: Dict[str, str] | None = None,
            delete_headers: List[str] | None = None,
        ) -> Dict[str, str]:
            headers = base_headers.copy()
            if additional_headers:
                headers.update(additional_headers)
            if delete_headers:
                for header in delete_headers:
                    headers.pop(header, None)
            return headers

        return {
            "notpx": create_headers({"Authorization": ""}),
            "tganalytics": create_headers(),
            "plausible": create_headers({"Sec-Fetch-Site": "cross-site"}),
            "websocket": create_headers(
                websocket_headers,
                ["Referer", "Sec-Fetch-Dest", "Sec-Fetch-Mode", "Sec-Fetch-Site"],
            ),
            "image_notpx": create_headers(),
            "adsgram": create_headers({"X-Requested-With": "XMLHttpRequest"}),
        }

    async def run(self, user_agent: str, proxy: str | None) -> NoReturn:
        chromium_version = re.search(r"Chrome/([\d.]+)", user_agent)
        if not chromium_version:
            raise Exception("Chromium version not found in user agent.")
        self.chromium_version = chromium_version.group(1)

        sec_ch_ua = f'"Chromium";v="{chromium_version}", "Android WebView";v="{chromium_version}", "Not-A.Brand";v="99"'
        sec_ch_ua_mobile = "?1"
        sec_ch_ua_platform = '"Android"'

        for headers_name, headers in self._headers.items():
            headers["User-Agent"] = user_agent

            if headers_name == "websocket":
                continue

            headers["Sec-Ch-Ua"] = sec_ch_ua
            headers["Sec-Ch-Ua-Mobile"] = sec_ch_ua_mobile
            headers["Sec-Ch-Ua-Platform"] = sec_ch_ua_platform

        self.proxy = proxy

        while True:
            try:
                proxy_connector = ProxyConnector().from_url(proxy) if proxy else None
                async with aiohttp.ClientSession(
                    connector=proxy_connector, timeout=aiohttp.ClientTimeout(15)
                ) as session:
                    if proxy:
                        await self._proxy_checker(session, proxy)

                    next_iteration_sleep_time = await self._perform_notpx_actions(
                        session, self.telegram_client
                    )

                if not next_iteration_sleep_time:
                    minutes_to_sleep = random.randint(
                        settings.ITERATION_SLEEP_MINUTES[0],
                        settings.ITERATION_SLEEP_MINUTES[1],
                    )
                    sleep_time = minutes_to_sleep * 60
                    logger.info(
                        f"{self.session_name} | Sleeping for: {minutes_to_sleep // 60} hours and {minutes_to_sleep % 60} minutes"
                    )
                    await asyncio.sleep(sleep_time)
                else:
                    minutes_to_sleep = int((next_iteration_sleep_time % 3600) // 60)
                    seconds_to_sleep = int(next_iteration_sleep_time % 60)
                    logger.info(
                        f"{self.session_name} | Sleeping for: {minutes_to_sleep % 60} minutes and {seconds_to_sleep} seconds"
                    )
                    await asyncio.sleep(next_iteration_sleep_time)
            except Exception as error:
                handle_error(self.session_name, error)
                await self._websocket_manager.stop()
                logger.info(
                    f"{self.session_name} | Retrying in {self.RETRY_ITERATION_DELAY} seconds"
                )
                await asyncio.sleep(self.RETRY_ITERATION_DELAY)

    async def _proxy_checker(self, session: aiohttp.ClientSession, proxy: str):
        try:
            response = await session.get(
                "https://ipinfo.io/json",
                timeout=aiohttp.ClientTimeout(10),
                ssl=settings.ENABLE_SSL,
            )
            response.raise_for_status()
            response_json = await response.json()
            ip = response_json.get("ip", "Not Found")
            country = response_json.get("country", "Not Found")

            logger.info(
                f"{self.session_name} | Proxy connected | IP: {ip} | Country: {country}"
            )
        except Exception:
            raise Exception(f"{self.session_name} | Proxy error | {proxy}")

    async def _perform_notpx_actions(
        self, session: aiohttp.ClientSession, telegram_client
    ):
        if settings.SLEEP_AT_NIGHT:
            await self._handle_night_sleep()

        if settings.CHECK_BOT_STATE:
            bot_state = await session.get(
                "https://raw.githubusercontent.com/Dellenoam/NotPixelBot/refs/heads/master/bot_state"
            )
            bot_state.raise_for_status()

            if await bot_state.text() != "running":
                logger.critical("Admins have stopped the bot!")
                sys.exit(1)

        if not await self._notpx_api_checker.check_api(session, self._headers["notpx"]):
            logger.critical("NotPX API has been changed!")
            sys.exit(1)

        tg_mini_app_auth = TelegramMiniAppAuth(self.telegram_client, proxy=self.proxy)
        tg_auth_app_data = await tg_mini_app_auth._get_telegram_web_data(
            "notpixel", "app", settings.REF_ID if settings.USE_REF else None
        )

        auth_url = tg_auth_app_data["auth_url"]
        self.user_data = tg_auth_app_data["user_data"]
        self.chat_instance = tg_auth_app_data["chat_instance"]

        self._headers["notpx"]["Authorization"] = (
            f"initData {tg_auth_app_data['init_data']}"
        )

        await self._send_tganalytics_event(session)

        plausible_payload = await self._create_plausible_payload(u=auth_url)
        await self._send_plausible_event(session, plausible_payload)

        about_me_data = await self._get_me(session)

        websocket_token = about_me_data.get("websocketToken")

        if not websocket_token:
            raise ValueError(f"{self.session_name} | Couldn't retrieve websocket token")

        await self._get_status(session)

        await self._websocket_manager.add_session(
            name=self.session_name,
            balance=self.balance,
            charges=self.charges,
            notpx_headers=self._headers["notpx"],
            websocket_headers=self._headers["websocket"],
            telegram_client=self.telegram_client,
            proxy=self.proxy,
            websocket_token=websocket_token,
        )

        if settings.COMPLETE_QUESTS and self._quests_to_complete:
            await self._quest_completion(session)

        if settings.UPGRADE_BOOSTS:
            if (
                self.boost_energyLimit != self.max_boosts["energyLimit"]
                or self.boost_paintReward != self.max_boosts["paintReward"]
                or self.boost_reChargeSpeed != self.max_boosts["reChargeSpeed"]
            ):
                await self._upgrade_boosts(session)
            else:
                logger.info(f"{self.session_name} | All boosts are maxed out")

        while not self._websocket_manager.is_canvas_set:
            await asyncio.sleep(2)

        round_period = await self._get_round_period(session)
        if not round_period:
            raise ValueError(f"{self.session_name} | Couldn't retrieve round period")

        now = datetime.now(timezone.utc)
        round_start_time = datetime.fromisoformat(
            round_period["StartTime"].replace("Z", "+00:00")
        ) + timedelta(minutes=settings.ROUND_START_TIME_DELTA_MINUTES)
        round_end_time = datetime.fromisoformat(
            round_period["EndTime"].replace("Z", "+00:00")
        ) - timedelta(minutes=settings.ROUND_END_TIME_DELTA_MINUTES)

        is_round_started = now >= datetime.fromisoformat(
            round_period["StartTime"].replace("Z", "+00:00")
        )
        is_round_ended = now >= datetime.fromisoformat(
            round_period["EndTime"].replace("Z", "+00:00")
        )

        is_after_start_time = now >= round_start_time
        is_before_end_time = now < round_end_time

        next_iteration_sleep_time = None

        if not is_after_start_time:
            time_to_start = round_start_time - now
            max_sleep_time = timedelta(minutes=settings.ITERATION_SLEEP_MINUTES[1])

            if time_to_start <= max_sleep_time:
                next_iteration_sleep_time = time_to_start.total_seconds()
                minutes = int(next_iteration_sleep_time // 60)
                seconds = int(next_iteration_sleep_time % 60)
                logger.info(
                    f"{self.session_name} | Setting up next iteration sleep before next round: {minutes} minutes {seconds} seconds"
                )

        is_round_active = is_round_started and not is_round_ended

        should_paint_pixels = (
            settings.PAINT_PIXELS and is_after_start_time and is_before_end_time
        )

        template_available = await self._check_tournament_my(session)

        if not template_available or (
            settings.RESELECT_TOURNAMENT_TEMPLATE
            and not self.is_template_reselected
            and not is_round_active
        ):
            if settings.RESELECT_TOURNAMENT_TEMPLATE:
                logger.info(f"{self.session_name} | Re-selecting tournament template")
                self.is_template_reselected = True
                async with aiofiles.open(".env", "r", encoding="utf-8") as file:
                    lines = await file.readlines()

                async with aiofiles.open(".env", "w", encoding="utf-8") as file:
                    for line in lines:
                        if "RESELECT_TOURNAMENT_TEMPLATE" in line:
                            line = "RESELECT_TOURNAMENT_TEMPLATE=False\n"
                        await file.write(line)
            else:
                logger.info(f"{self.session_name} | Selecting tournament template")

            await self._set_tournament_template(session, auth_url)

        if should_paint_pixels:
            await self._paint_pixels(session)

        self.balance = self._websocket_manager.get_session_balance

        if settings.CLAIM_PX:
            await self._claim_px(session)

        if settings.COMPLETE_TASKS and self._tasks_to_complete:
            await self._task_completion(session, telegram_client)

        if settings.WATCH_ADS:
            await self._watch_ads(session)

        await self._get_tournament_results(session, auth_url)

        logger.info(f"{self.session_name} | All done | Balance: {self.balance}")

        await self._websocket_manager.stop()

        return next_iteration_sleep_time

    async def _handle_night_sleep(self) -> None:
        current_hour = datetime.now().hour
        start_night_time = random.randint(
            settings.NIGHT_START_HOURS[0], settings.NIGHT_START_HOURS[1]
        )
        end_night_time = random.randint(
            settings.NIGHT_END_HOURS[0], settings.NIGHT_END_HOURS[1]
        )

        if start_night_time > end_night_time:
            if current_hour >= start_night_time or current_hour < end_night_time:
                if current_hour >= start_night_time:
                    sleep_time_in_hours = 24 - current_hour + end_night_time
                else:
                    sleep_time_in_hours = end_night_time - current_hour
            else:
                return
        else:
            if start_night_time <= current_hour <= end_night_time:
                sleep_time_in_hours = end_night_time - current_hour
            else:
                return

        random_minutes_to_sleep_time = random.randint(
            settings.ADDITIONAL_NIGHT_SLEEP_MINUTES[0],
            settings.ADDITIONAL_NIGHT_SLEEP_MINUTES[1],
        )

        logger.info(
            f"{self.session_name} | It's night time. Sleeping for: {int(sleep_time_in_hours)} hours and {random_minutes_to_sleep_time} minutes"
        )

        await asyncio.sleep(
            (sleep_time_in_hours * 60 * 60) + (random_minutes_to_sleep_time * 60)
        )

    async def _get_me(
        self, session: aiohttp.ClientSession, attempts: int = 1
    ) -> Dict[str, str]:
        try:
            response = await session.get(
                "https://notpx.app/api/v1/users/me",
                headers=self._headers["notpx"],
                ssl=settings.ENABLE_SSL,
            )
            response.raise_for_status()
            response_json = await response.json()

            return response_json

        except Exception:
            if attempts <= 3:
                logger.warning(
                    f"{self.session_name} | Failed to get info about me, retrying in {self.RETRY_DELAY} seconds | Attempts: {attempts}"
                )
                await asyncio.sleep(self.RETRY_DELAY)
                return await self._get_me(session=session, attempts=attempts + 1)
            raise Exception(f"{self.session_name} | Error while getting info about me")

    async def _send_tganalytics_event(
        self, session: aiohttp.ClientSession, attempts: int = 1
    ) -> None:
        try:
            response = await session.get(
                "https://tganalytics.xyz/aee7c93a9ae7930fb19732325d2c560c53849aa7",
                headers=self._headers["tganalytics"],
                ssl=settings.ENABLE_SSL,
            )
            response.raise_for_status()

            task = await response.text()
            solution = await self._solve_task(task)

            headers = self._headers["tganalytics"]
            headers["Content"] = solution
            headers["TGA-Auth-Token"] = (
                "eyJhcHBfbmFtZSI6Ik5vdFBpeGVsIiwiYXBwX3VybCI6Imh0dHBzOi8vdC5tZS9ub3RwaXhlbC9hcHAiLCJhcHBfZG9tYWluIjoiaHR0cHM6Ly9hcHAubm90cHguYXBwIn0=!qE41yKlb/OkRyaVhhgdePSZm5Nk7nqsUnsOXDWqNAYE="
            )

            random_event_delay = random.randint(2500, 2800)

            payload = self._create_tganalytics_payload(random_event_delay)

            await asyncio.sleep(random_event_delay / 1000)

            response = await session.post(
                "https://tganalytics.xyz/events",
                headers=headers,
                json=payload,
                ssl=settings.ENABLE_SSL,
            )
            response.raise_for_status()
        except Exception:
            if attempts <= 3:
                logger.warning(
                    f"{self.session_name} | Failed to send tganalytics event, retrying in {self.RETRY_DELAY} seconds | Attempts: {attempts}"
                )
                await asyncio.sleep(self.RETRY_DELAY)
                return await self._send_tganalytics_event(
                    session=session, attempts=attempts + 1
                )
            raise Exception(
                f"{self.session_name} | Error while sending tganalytics event"
            )

    def _create_tganalytics_payload(self, random_event_delay) -> List[Dict[str, str]]:
        base_event = {
            "session_id": str(uuid4()),
            "user_id": int(self.user_data["user_id"]),
            "app_name": "NotPixel",
            "is_premium": self.user_data["is_premium_user"],
            "platform": "android",
            "locale": self.user_data["language_code"],
        }
        return [
            {
                **base_event,
                "event_name": "app-hide",
                "client_timestamp": str(int(time() * 1000)),
            },
            {
                **base_event,
                "event_name": "app-init",
                "client_timestamp": str(int(time() * 1000) + random_event_delay),
            },
        ]

    async def _send_plausible_event(
        self,
        session: aiohttp.ClientSession,
        payload: Dict[str, str | None],
        attempts: int = 1,
    ) -> None:
        try:
            response = await session.post(
                "https://plausible.joincommunity.xyz/api/event",
                headers=self._headers["plausible"],
                data=payload,
                ssl=settings.ENABLE_SSL,
            )
            response.raise_for_status()
        except Exception:
            if attempts <= 3:
                logger.warning(
                    f"{self.session_name} | Failed to send plausible event, retrying in {self.RETRY_DELAY} seconds | Attempts: {attempts}"
                )
                await asyncio.sleep(self.RETRY_DELAY)
                return await self._send_plausible_event(
                    session=session, payload=payload, attempts=attempts + 1
                )
            raise Exception(
                f"{self.session_name} | Error while sending plausible event"
            )

    async def _create_plausible_payload(
        self, u: str, n: str = "pageview", d: str = "notpx.app", r: str | None = None
    ) -> Dict[str, str | None]:
        return {"n": n, "u": u, "d": d, "r": r}

    async def _solve_task(self, task: str) -> str:
        try:
            process = await asyncio.create_subprocess_exec(
                "node",
                "bot/core/poh_solver/main.js",
                task,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            if stderr:
                raise Exception(
                    f"{self.session_name} | Error while solving task | {stderr.decode('utf-8').strip()}"
                )
            return stdout.decode("utf-8").strip()
        except Exception:
            raise Exception(f"{self.session_name} | Unknown error while solving task")

    async def _claim_px(
        self, session: aiohttp.ClientSession, attempts: int = 1
    ) -> None:
        try:
            plausible_payload = await self._create_plausible_payload(
                u="https://app.notpx.app/claiming"
            )
            await self._send_plausible_event(session, plausible_payload)

            response = await session.get(
                "https://notpx.app/api/v1/mining/claim",
                headers=self._headers["notpx"],
                ssl=settings.ENABLE_SSL,
            )
            response.raise_for_status()
            response_json = await response.json()

            claimed_px = response_json.get("claimed")

            self.balance += claimed_px

            logger.info(
                f"{self.session_name} | Successfully claimed {round(claimed_px, 2)} px"
            )

            plausible_payload = await self._create_plausible_payload(
                u="https://app.notpx.app/"
            )
            await self._send_plausible_event(session, plausible_payload)
        except Exception as error:
            plausible_payload = await self._create_plausible_payload(
                u="https://app.notpx.app/"
            )
            await self._send_plausible_event(session, plausible_payload)

            if attempts <= 3:
                logger.warning(
                    f"{self.session_name} | Failed to claim px, retrying in {self.RETRY_DELAY} seconds | Attempts: {attempts}"
                )
                await asyncio.sleep(self.RETRY_DELAY)
                return await self._claim_px(session=session, attempts=attempts + 1)

            raise Exception(f"{self.session_name} | Error while claiming px: {error}")

    async def _check_tournament_my(
        self, session: aiohttp.ClientSession, attempts: int = 1
    ) -> bool | None:
        try:
            response = await session.get(
                "https://notpx.app/api/v1/tournament/template/subscribe/my",
                headers=self._headers["notpx"],
                ssl=settings.ENABLE_SSL,
            )
            if response.status == 409 or response.status == 404:
                return False
            elif response.status == 200:
                response_json = await response.json()

                self.template_url = response_json.get("url")
                self.template_x = response_json.get("x")
                self.template_y = response_json.get("y")
                self.template_size = response_json.get("size")

                return True
            else:
                response.raise_for_status()
        except Exception:
            if attempts <= 3:
                logger.warning(
                    f"{self.session_name} | Failed to check my, retrying in {self.RETRY_DELAY} seconds | Attempts: {attempts}"
                )
                await asyncio.sleep(self.RETRY_DELAY)
                return await self._check_tournament_my(
                    session=session, attempts=attempts + 1
                )
            raise Exception(f"{self.session_name} | Error while checking my")

    async def _get_status(
        self, session: aiohttp.ClientSession, attempts: int = 1
    ) -> None:
        try:
            response = await session.get(
                "https://notpx.app/api/v1/mining/status",
                headers=self._headers["notpx"],
                ssl=settings.ENABLE_SSL,
            )
            response.raise_for_status()

            response_json = await response.json()
            self.boost_energyLimit = response_json.get("boosts", {}).get("energyLimit")
            self.boost_paintReward = response_json.get("boosts", {}).get("paintReward")
            self.boost_reChargeSpeed = response_json.get("boosts", {}).get(
                "reChargeSpeed"
            )
            self.balance = response_json.get("userBalance")
            self.league = response_json.get("league")
            self.charges = response_json.get("charges")

            self._completed_tasks = response_json.get("tasks")
            for task_list_key, task_list_value in self._tasks_list.items():
                for task_key, task_value in task_list_value.items():
                    if task_key not in self._completed_tasks:
                        if isinstance(task_value, str) and task_value.startswith(
                            "leagueBonus"
                        ):
                            league_name = task_value.split("leagueBonus")[1].lower()

                            task_league_weight = self._league_weights[league_name]
                            current_league_weight = self._league_weights[self.league]

                            if task_league_weight > current_league_weight:
                                continue

                        if task_list_key not in self._tasks_to_complete:
                            self._tasks_to_complete[task_list_key] = {}

                        self._tasks_to_complete[task_list_key][task_key] = task_value

            self._completed_quests = response_json.get("quests")
            for quest in self._quests_list:
                if quest == "secretWord:example":
                    continue
                if not self._completed_quests:
                    self.quests_to_complete = []
                    self._quests_to_complete.append(quest)
                elif quest not in self._completed_quests:
                    self._quests_to_complete.append(quest)

            logger.info(
                f"{self.session_name} | Successfully logged in | Balance: {round(self.balance, 2)} | League: {self.league.capitalize()}"
            )

        except Exception:
            if attempts <= 3:
                logger.warning(
                    f"{self.session_name} | Failed to get status, retrying in {self.RETRY_DELAY} seconds | Attempts: {attempts}"
                )
                await asyncio.sleep(self.RETRY_DELAY)
                return await self._get_status(session=session, attempts=attempts + 1)
            raise Exception(f"{self.session_name} | Error while getting status")

    async def _upgrade_boosts(
        self, session: aiohttp.ClientSession, attempts: int = 1
    ) -> None:
        boost_order = ["energyLimit", "paintReward", "reChargeSpeed"]

        try:
            plausible_payload = await self._create_plausible_payload(
                u="https://app.notpx.app/claiming"
            )
            await self._send_plausible_event(session, plausible_payload)

            while True:
                for boost_type in boost_order:
                    current_boost = getattr(self, f"boost_{boost_type}")
                    if current_boost < self.max_boosts[boost_type]:
                        if await self._upgrade_boost(session, boost_type):
                            await asyncio.sleep(random.uniform(1, 2))
                            break
                        plausible_payload = await self._create_plausible_payload(
                            u="https://app.notpx.app/"
                        )
                        await self._send_plausible_event(session, plausible_payload)
                        return
                else:
                    plausible_payload = await self._create_plausible_payload(
                        u="https://app.notpx.app/"
                    )
                    await self._send_plausible_event(session, plausible_payload)
                    return

        except Exception:
            plausible_payload = await self._create_plausible_payload(
                u="https://app.notpx.app/"
            )
            await self._send_plausible_event(session, plausible_payload)

            if attempts <= 3:
                logger.warning(
                    f"{self.session_name} | Failed to upgrade boosts, retrying in {self.RETRY_DELAY} seconds | Attempts: {attempts}"
                )
                await asyncio.sleep(self.RETRY_DELAY)
                return await self._upgrade_boosts(
                    session=session, attempts=attempts + 1
                )

            raise Exception(f"{self.session_name} | Error while upgrading boosts")

    async def _upgrade_boost(self, session, boost_type) -> bool:
        url = f"https://notpx.app/api/v1/mining/boost/check/{boost_type}"

        if (
            self.balance
            < self.boost_prices[boost_type][getattr(self, f"boost_{boost_type}") + 1]
        ):
            logger.info(
                f"{self.session_name} | Not enough balance to upgrade {boost_type}"
            )
            return False

        response = await session.get(
            url, headers=self._headers["notpx"], ssl=settings.ENABLE_SSL
        )
        response.raise_for_status()
        response_json = await response.json()

        if not response_json.get(boost_type):
            raise Exception(
                f"{self.session_name} | Couldn't retrieve {boost_type} from response json while upgrading boost"
            )

        old_boost = getattr(self, f"boost_{boost_type}")
        setattr(self, f"boost_{boost_type}", old_boost + 1)
        self.balance -= self.boost_prices[boost_type][
            getattr(self, f"boost_{boost_type}")
        ]

        logger.info(
            f"{self.session_name} | Successfully leveled up {boost_type}! | LVL.{old_boost} -> LVL.{old_boost + 1}"
        )

        return True

    async def _paint_pixel(
        self,
        canvas_x: int,
        canvas_y: int,
        template_pixel: np.ndarray,
    ) -> None:
        template_pixel_hex = self._canvas_renderer.rgba_to_hex(
            tuple(template_pixel.flatten().tolist())
        )
        canvas_pixel_id = self._canvas_renderer._xy_to_pixel_id(canvas_x, canvas_y)

        await self._websocket_manager.send_repaint_command(
            canvas_pixel_id, template_pixel_hex
        )

        self.charges = self._websocket_manager.get_session_charges

    async def _paint_pixels(
        self, session: aiohttp.ClientSession, attempts: int = 1
    ) -> None:
        try:
            response = await session.get(
                self.template_url,
                headers=self._headers["image_notpx"],
                ssl=settings.ENABLE_SSL,
            )
            response.raise_for_status()

            template_from_response = Image.open(io.BytesIO(await response.read()))

            if template_from_response.size != (self.template_size, self.template_size):
                template_cv2 = cv2.cvtColor(
                    np.array(template_from_response), cv2.COLOR_RGBA2BGRA
                )
                template_cv2 = cv2.resize(
                    template_cv2, (self.template_size, self.template_size)
                )
                template_cv2 = cv2.cvtColor(template_cv2, cv2.COLOR_BGR2RGBA)
                template_from_response = Image.fromarray(template_cv2)

            template_from_response = template_from_response.convert("RGBA")
            template_array = np.array(template_from_response)
            template_2d = template_array.reshape(
                (self.template_size, self.template_size, 4)
            )

            pixels_to_paint = []

            for ty in range(self.template_size):
                for tx in range(self.template_size):
                    template_pixel = template_2d[ty, tx]
                    if template_pixel[3] == 0:
                        continue

                    canvas_x = self.template_x + tx
                    canvas_y = self.template_y + ty
                    pixels_to_paint.append((tx, ty, canvas_x, canvas_y))

            if settings.USE_ALL_CHARGES:
                while self.charges > 0:
                    random.shuffle(pixels_to_paint)
                    for tx, ty, canvas_x, canvas_y in pixels_to_paint:
                        if self.charges <= 0:
                            break
                        canvas_array = self._canvas_renderer.get_canvas
                        canvas_2d = canvas_array.reshape(
                            (
                                self._canvas_renderer.CANVAS_SIZE,
                                self._canvas_renderer.CANVAS_SIZE,
                                4,
                            )
                        )
                        template_pixel = template_2d[ty, tx]
                        canvas_pixel = canvas_2d[canvas_y, canvas_x]
                        if not np.array_equal(template_pixel[:3], canvas_pixel[:3]):
                            await self._paint_pixel(
                                canvas_x=canvas_x,
                                canvas_y=canvas_y,
                                template_pixel=template_pixel,
                            )

                            await asyncio.sleep(random.uniform(0.6, 1.2))

                    await asyncio.sleep(random.uniform(1, 2))
            else:
                random.shuffle(pixels_to_paint)
                for tx, ty, canvas_x, canvas_y in pixels_to_paint:
                    if self.charges <= 0:
                        break
                    canvas_array = self._canvas_renderer.get_canvas
                    canvas_2d = canvas_array.reshape(
                        (
                            self._canvas_renderer.CANVAS_SIZE,
                            self._canvas_renderer.CANVAS_SIZE,
                            4,
                        )
                    )
                    template_pixel = template_2d[ty, tx]
                    canvas_pixel = canvas_2d[canvas_y, canvas_x]
                    if not np.array_equal(template_pixel[:3], canvas_pixel[:3]):
                        await self._paint_pixel(
                            canvas_x=canvas_x,
                            canvas_y=canvas_y,
                            template_pixel=template_pixel,
                        )

                        await asyncio.sleep(random.uniform(1, 2))
        except Exception:
            if attempts <= 3:
                logger.warning(
                    f"{self.session_name} | Failed to paint pixels, retrying in {self.RETRY_DELAY} seconds | Attempts: {attempts}"
                )
                await asyncio.sleep(self.RETRY_DELAY)
                return await self._paint_pixels(session=session, attempts=attempts + 1)

            raise Exception(
                f"{self.session_name} | Max retry attempts reached while painting pixels"
            )

    async def _task_completion(
        self,
        session: aiohttp.ClientSession,
        telegram_client: Client,
        attempts: int = 1,
    ) -> None:
        try:
            plausible_payload = await self._create_plausible_payload(
                u="https://app.notpx.app/claiming"
            )
            await self._send_plausible_event(session, plausible_payload)

            for task_list_key, task_list_value in self._tasks_to_complete.items():
                for task_key, task_value in task_list_value.items():
                    if task_list_key == "x_tasks_list":
                        response = await session.get(
                            f"https://notpx.app/api/v1/mining/task/check/x?name={task_value}",
                            headers=self._headers["notpx"],
                            ssl=settings.ENABLE_SSL,
                        )
                        response.raise_for_status()
                        response_json = await response.json()
                        if response_json.get("x:" + task_value):
                            logger.info(
                                f"{self.session_name} | Completed task: {task_value} on X.com"
                            )
                        else:
                            raise Exception(
                                f"{self.session_name} | Failed to complete task: {task_value} on X.com"
                            )

                    elif task_list_key == "channel_tasks_list":
                        async with telegram_client:
                            await telegram_client.join_chat(task_value)

                        await asyncio.sleep(random.uniform(1.95, 2.35))

                        response = await session.get(
                            f"https://notpx.app/api/v1/mining/task/check/channel?name={task_value}",
                            headers=self._headers["notpx"],
                            ssl=settings.ENABLE_SSL,
                        )
                        response.raise_for_status()
                        logger.info(
                            f"{self.session_name} | Completed task: Join {task_value} channel"
                        )

                    elif task_list_key == "league_tasks_list":
                        league_name = task_value.split("leagueBonus")[1]

                        response = await session.get(
                            f"https://notpx.app/api/v1/mining/task/check/{task_value}",
                            headers=self._headers["notpx"],
                            ssl=settings.ENABLE_SSL,
                        )
                        response.raise_for_status()
                        logger.info(
                            f"{self.session_name} | Completed task: Bonus for {league_name} league"
                        )

                    elif (
                        task_list_key == "click_tasks_list"
                        and settings.COMPLETE_DANGER_TASKS
                    ):
                        plausible_payload = await self._create_plausible_payload(
                            u="https://app.notpx.app/claiming",
                            n=task_value["event_name"],
                        )
                        await self._send_plausible_event(session, plausible_payload)

                        await asyncio.sleep(random.uniform(2, 3.5))

                        response = await session.get(
                            f"https://notpx.app/api/v1/mining/task/check/{task_key}",
                            headers=self._headers["notpx"],
                            ssl=settings.ENABLE_SSL,
                        )
                        response.raise_for_status()
                        response_json = await response.json()
                        if response_json.get(task_key):
                            self.balance += task_value["reward"]
                            logger.info(
                                f"{self.session_name} | Completed task: {task_key} | Reward {task_value['reward']} PX"
                            )
                        else:
                            raise Exception(
                                f"{self.session_name} | Failed to complete task: {task_key}"
                            )

                    await asyncio.sleep(random.uniform(4.95, 6.35))

            self._tasks_to_complete = {}

            plausible_payload = await self._create_plausible_payload(
                u="https://app.notpx.app/"
            )
            await self._send_plausible_event(session, plausible_payload)

        except Exception:
            plausible_payload = await self._create_plausible_payload(
                u="https://app.notpx.app/"
            )
            await self._send_plausible_event(session, plausible_payload)

            if attempts <= 3:
                logger.warning(
                    f"{self.session_name} | Failed to complete tasks, retrying in {self.RETRY_DELAY} seconds | Attempts: {attempts}"
                )
                await asyncio.sleep(self.RETRY_DELAY)
                return await self._task_completion(
                    session=session,
                    telegram_client=telegram_client,
                    attempts=attempts + 1,
                )

            raise Exception(
                f"{self.session_name} | Max retry attempts reached while completing tasks"
            )

    async def _get_tournament_results(
        self,
        session: aiohttp.ClientSession,
        auth_url: str,
        attempts: int = 1,
    ) -> None:
        try:
            plausible_payload = await self._create_plausible_payload(
                u="https://app.notpx.app/tournament"
            )
            await self._send_plausible_event(session, plausible_payload)

            response = await session.get(
                "https://notpx.app/api/v1/tournament/user/results",
                headers=self._headers["notpx"],
                ssl=settings.ENABLE_SSL,
            )
            response.raise_for_status()
            response_json = await response.json()

            rounds = response_json.get("rounds", [])

            if not rounds:
                logger.info(f"{self.session_name} | No tournament results found")
                return

            last_round = rounds[-1]
            player_rank = last_round.get("rank", None)
            template_rank = last_round.get("template", {}).get("rank", None)
            logger.info(
                f"{self.session_name} | Player rank: {player_rank} | Template rank: {template_rank}"
            )

            plausible_payload = await self._create_plausible_payload(u=auth_url)
            await self._send_plausible_event(session, plausible_payload)
        except Exception:
            plausible_payload = await self._create_plausible_payload(u=auth_url)
            await self._send_plausible_event(session, plausible_payload)

            if attempts <= 3:
                logger.warning(
                    f"{self.session_name} | Failed to get tournament results, retrying in {self.RETRY_DELAY} seconds | Attempts: {attempts}"
                )
                await asyncio.sleep(self.RETRY_DELAY)
                return await self._get_tournament_results(
                    session=session,
                    auth_url=auth_url,
                    attempts=attempts + 1,
                )

            raise Exception(
                f"{self.session_name} | Max retry attempts reached while getting tournament results"
            )

    async def _set_tournament_template(
        self, session: aiohttp.ClientSession, auth_url: str, attempts: int = 1
    ) -> None:
        try:
            plausible_payload = await self._create_plausible_payload(
                u="https://app.notpx.app/tournament"
            )
            await self._send_plausible_event(session, plausible_payload)

            async with aiofiles.open("templates_pool.json", "r") as file:
                templates_pool = json.loads(await file.read())
                template_ids = templates_pool.get("ids", [])

            if template_ids:
                template_id = random.choice(template_ids)

                response = await session.get(
                    f"https://notpx.app/api/v1/tournament/template/{template_id}",
                    headers=self._headers["notpx"],
                    ssl=settings.ENABLE_SSL,
                )
                response_json = await response.json()

                self.template_url = response_json.get("url")
                self.template_x = response_json.get("x")
                self.template_y = response_json.get("y")
                self.template_size = response_json.get("imageSize")
            else:
                template_id = await self._get_random_approved_template_id(session)

            response = await session.put(
                f"https://notpx.app/api/v1/tournament/template/subscribe/{template_id}",
                headers=self._headers["notpx"],
                ssl=settings.ENABLE_SSL,
            )
            response.raise_for_status()

            logger.info(
                f"{self.session_name} | Set tournament template | Template ID: {template_id}"
            )

            plausible_payload = await self._create_plausible_payload(u=auth_url)
            await self._send_plausible_event(session, plausible_payload)
        except Exception:
            plausible_payload = await self._create_plausible_payload(u=auth_url)
            await self._send_plausible_event(session, plausible_payload)

            if attempts <= 3:
                logger.warning(
                    f"{self.session_name} | Failed to set tournament template, retrying in {self.RETRY_DELAY} seconds | Attempts: {attempts}"
                )
                await asyncio.sleep(self.RETRY_DELAY)
                return await self._set_tournament_template(
                    session=session, auth_url=auth_url, attempts=attempts + 1
                )

            raise Exception(
                f"{self.session_name} | Max retry attempts reached while setting tournament template"
            )

    async def _get_random_approved_template_id(
        self, session: aiohttp.ClientSession
    ) -> int:
        while True:
            random_templates = await session.get(
                "https://notpx.app/api/v1/tournament/template/list/random?limit=16",
                headers=self._headers["notpx"],
                ssl=settings.ENABLE_SSL,
            )
            random_templates.raise_for_status()
            random_templates_json = await random_templates.json()

            random_template_list = random_templates_json.get("list", [])
            random.shuffle(random_template_list)

            for template in random_template_list:
                is_template_approved = template.get("approved", False)
                if is_template_approved:
                    self.template_url = template.get("url")
                    self.template_x = template.get("x")
                    self.template_y = template.get("y")
                    self.template_size = template.get("imageSize")

                    return template.get("id")

            await asyncio.sleep(random.uniform(1.5, 3))

    async def _quest_completion(
        self, session: aiohttp.ClientSession, attempts: int = 1
    ):
        try:
            plausible_payload = await self._create_plausible_payload(
                u="https://app.notpx.app/secrets"
            )
            await self._send_plausible_event(session, plausible_payload)

            for quest in self._quests_to_complete:
                secret_word = quest.split(":")[1]

                payload = {"secret_word": secret_word}
                response = await session.post(
                    "https://notpx.app/api/v1/mining/quest/check/secretWord",
                    json=payload,
                    headers=self._headers["notpx"],
                    ssl=settings.ENABLE_SSL,
                )

                if response.status == 403:
                    logger.info(
                        f"{self.session_name} | Wrong word, fren | Secret word: {secret_word}"
                    )
                    continue

                response.raise_for_status()

                response_json = await response.json()
                if response_json.get("secretWord").get("success"):
                    logger.info(
                        f"{self.session_name} | Completed secret word quest | Reward: {response_json.get('secretWord').get('reward')}"
                    )
                else:
                    raise Exception(
                        f"{self.session_name} | Failed to complete secret word quest | Secret word: {secret_word}"
                    )

            self._quests_to_complete = []

            plausible_payload = await self._create_plausible_payload(
                u="https://app.notpx.app/"
            )
            await self._send_plausible_event(session, plausible_payload)
        except Exception:
            plausible_payload = await self._create_plausible_payload(
                u="https://app.notpx.app/"
            )
            await self._send_plausible_event(session, plausible_payload)

            if attempts <= 3:
                logger.warning(
                    f"{self.session_name} | Failed to complete secret word quest, retrying in {self.RETRY_DELAY} seconds | Attempts: {attempts}"
                )
                await asyncio.sleep(self.RETRY_DELAY)
                return await self._quest_completion(
                    session=session, attempts=attempts + 1
                )

            raise Exception(
                f"{self.session_name} | Max retry attempts reached while completing secret word quest"
            )

    async def _watch_ads(self, session: aiohttp.ClientSession, attempts: int = 1):
        try:
            plausible_payload = await self._create_plausible_payload(
                u="https://app.notpx.app/claiming"
            )
            await self._send_plausible_event(session, plausible_payload)

            ad_watcher = AdWatcher(
                user_data=self.user_data,
                session_name=self.session_name,
                headers=self._headers,
                chat_instance=int(self.chat_instance),
                balance=self.balance,
            )

            self.balance = await ad_watcher.watch_ads(session=session)

            plausible_payload = await self._create_plausible_payload(
                u="https://app.notpx.app/"
            )
            await self._send_plausible_event(session, plausible_payload)

        except Exception:
            plausible_payload = await self._create_plausible_payload(
                u="https://app.notpx.app/"
            )
            await self._send_plausible_event(session, plausible_payload)
            if attempts <= 3:
                logger.warning(
                    f"{self.session_name} | Failed to watch ads, retrying in {self.RETRY_DELAY} seconds | Attempts: {attempts}"
                )
                await asyncio.sleep(self.RETRY_DELAY)
                return await self._watch_ads(session=session, attempts=attempts + 1)

            raise Exception(
                f"{self.session_name} | Max retry attempts reached while watching ads"
            )

    async def _get_round_period(
        self, session: aiohttp.ClientSession, attempts: int = 1
    ) -> Dict[str, Any] | None:
        try:
            response = await session.get(
                "https://notpx.app/api/v1/tournament/periods",
                headers=self._headers["notpx"],
                ssl=settings.ENABLE_SSL,
            )
            response.raise_for_status()
            response_json = await response.json()

            all_periods = response_json.get("allPeriods")

            round_periods = [
                period
                for period in all_periods
                if period.get("PeriodType") == "round"
                and period.get("EndTime", "")
                > datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            ]
            round_periods.sort(key=lambda p: p.get("EndTime", ""), reverse=False)

            return round_periods[0] if round_periods else None
        except Exception:
            if attempts <= 3:
                logger.warning(
                    f"{self.session_name} | Failed to get active period, retrying in {self.RETRY_DELAY} seconds | Attempts: {attempts}"
                )
                await asyncio.sleep(self.RETRY_DELAY)
                return await self._get_round_period(
                    session=session, attempts=attempts + 1
                )
            raise Exception(f"{self.session_name} | Error while getting active period")


def handle_error(session_name, error: Exception) -> None:
    logger.error(f"{error.__str__() if error else 'NotPXBot | Something went wrong'}")
    dev_logger.error(f"{session_name} | {traceback.format_exc()}")


async def run_notpxbot(
    telegram_client: Client,
    user_agent: str,
    proxy: str | None,
    start_delay: int,
) -> None:
    websocket_manager = None
    try:
        websocket_manager = WebSocketManager(
            websocket_url="wss://notpx.app/connection/websocket",
        )
        logger.info(f"{telegram_client.name} | Starting in {start_delay} seconds")
        await asyncio.sleep(start_delay)
        await NotPXBot(
            telegram_client=telegram_client, websocket_manager=websocket_manager
        ).run(user_agent=user_agent, proxy=proxy)
    except Exception as error:
        handle_error(telegram_client.name, error)
    finally:
        if telegram_client.is_connected:
            await telegram_client.disconnect()

        if websocket_manager and websocket_manager._running:
            await websocket_manager.stop()

        logger.info(f"{telegram_client.name} | Stopped")
