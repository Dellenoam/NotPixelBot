import asyncio
import re
import traceback
from typing import Dict

import aiohttp
from bs4 import BeautifulSoup

from bot.config.config import settings
from bot.utils.logger import dev_logger, logger


class NotPXAPIChecker:
    RETRY_DELAY = 5
    BASE_URL = "https://app.notpx.app"

    def extract_endpoints(self, js_code):
        pattern = r'.\.(get|post|put)\s*\(\s*[`"\']([^`"\']*)'

        endpoints = re.findall(pattern, js_code)

        unique_endpoints = set(endpoint[1] for endpoint in endpoints)

        return sorted(unique_endpoints)

    async def check_api(
        self,
        session: aiohttp.ClientSession,
        notpx_headers: Dict[str, str],
        attempts: int = 1,
    ) -> bool:
        """
        Checks the NotPX API for the correct URL.

        Args:
            session (aiohttp.ClientSession): The aiohttp session to use for requests.
            notpx_headers (Dict[str, str]): The headers to use for the request.
            attempts (int, optional): The number of attempts to make. Defaults to 1.

        Returns:
            bool: True if the API URL is correct, False otherwise.
        """
        try:
            response = await session.get(
                self.BASE_URL, headers=notpx_headers, ssl=settings.ENABLE_SSL
            )
            response.raise_for_status()
            html_content = await response.text()

            soup = BeautifulSoup(html_content, "html.parser")

            script_tags = soup.find_all("script")

            pattern = re.compile(r"/assets/index-.*\.js")
            result = None

            for tag in script_tags:
                src = tag.get("src")
                if src and pattern.match(src):
                    result = src
                    break

            if not result:
                logger.critical("API Checker | Missing script tag")
                return False

            js_url = f"{self.BASE_URL}{result}"

            response = await session.get(
                js_url, headers=notpx_headers, ssl=settings.ENABLE_SSL
            )
            response.raise_for_status()
            js_content = await response.text()

            match = re.search(r'VITE_API_URL:\s*"([^"]+)"', js_content)

            if not match:
                logger.critical("API Checker | Missing API URL")
                return False

            api_url = match.group(1)

            if api_url != "https://notpx.app/api/v1/":
                logger.critical("API Checker | API URL is not equal to expected")
                return False

            endpoints = self.extract_endpoints(js_content)
            known_endpoints = [
                "/buy/list",
                "/buy/stars",
                "/daily/free",
                "/daily/list",
                "/history/all?offset=${n}&limit=${s}",
                "/history/stats",
                "/image/get/${n}",
                "/image/mask${s}",
                "/image/prices",
                "/image/template/${n}",
                "/image/template/list?limit=${n}&offset=${s}",
                "/image/template/my",
                "/image/template/sizes/${n}",
                "/image/template/subscribe/${n}",
                "/image/template/upload",
                "/mining/boost/check/${n}",
                "/mining/claim",
                "/mining/quest/check/secretWord",
                "/mining/quest/stats ",
                "/mining/status",
                "/mining/task/check/${s}${a}",
                "/ratings/personal?league=${n.toLowerCase()}&limit=20",
                "/ratings/pixanos",
                "/ratings/squads/${n}",
                "/ratings/squads?league=${n.toLowerCase()}&limit=20",
                "/repaint/special",
                "/repaint/start",
                "/tournament/periods",
                "/tournament/template/${n}",
                "/tournament/template/list/random?limit=16",
                "/tournament/template/list?limit=${n}&offset=${s}",
                "/tournament/template/subscribe/${n}",
                "/tournament/template/subscribe/my",
                "/tournament/template/upload",
                "/tournament/user/results",
                "/transactions/start",
                "/users/me",
                "/users/me/revshare",
                "/users/mypixels/count",
                "/users/mypixels/sold?offset=${n}&limit=${s}",
                "/users/mypixels?offset=${n}&limit=${s}",
                "/users/rewards/${n}",
                "/users/rewards/claim",
                "/users/rewards/distribution/${n}",
                "/users/stats",
                "/users/wallet/${n}",
                "/wallet/ton-proof/check-proof",
                "/wallet/ton-proof/generate-payload",
                "cf-ipcountry",
                "initData",
                "skipIntro",
            ]

            if endpoints != known_endpoints:
                logger.critical("API Checker | Endpoints are not equal to expected")
                return False

            return True

        except Exception:
            if attempts <= 3:
                logger.warning(
                    f"API Checker | Failed to check NotPX API, retrying in {self.RETRY_DELAY} seconds | Attempts: {attempts}"
                )
                dev_logger.warning(f"API Checker | {traceback.format_exc()}")
                await asyncio.sleep(self.RETRY_DELAY)
                return await self.check_api(
                    session=session, notpx_headers=notpx_headers, attempts=attempts + 1
                )
            raise Exception("Error while checking NotPX API")
