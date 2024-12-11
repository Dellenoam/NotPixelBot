import asyncio
import json
import sys
import traceback
from typing import Dict, Optional

from aiohttp import ClientSession, ClientWebSocketResponse, WSMsgType
from aiohttp_socks import ProxyConnector
from attr import define, field
from pyrogram.client import Client
from typing_extensions import Self

from bot.config.config import settings
from bot.core.canvas_updater.centrifuge import decode_message, encode_commands
from bot.core.canvas_updater.dynamic_canvas_renderer import DynamicCanvasRenderer
from bot.core.canvas_updater.exceptions import (
    SessionErrors,
    WebSocketErrors,
)
from bot.utils.logger import dev_logger, logger


@define
class SessionData:
    name: str = field()
    balance: float = field()
    charges: int = field()
    notpx_headers: Dict[str, str] = field()
    websocket_headers: Dict[str, str] = field()
    telegram_client: Client = field()
    websocket_token: str = field()
    proxy: Optional[str] = field(default=None)

    @classmethod
    def create(
        cls,
        name: str,
        balance: float,
        charges: int,
        notpx_headers: Dict[str, str],
        websocket_headers: Dict[str, str],
        telegram_client: Client,
        proxy: Optional[str],
        websocket_token: str,
    ) -> Self:
        return cls(
            name=name,
            balance=balance,
            charges=charges,
            notpx_headers=notpx_headers,
            websocket_headers=websocket_headers,
            telegram_client=telegram_client,
            proxy=proxy,
            websocket_token=websocket_token,
        )


class WebSocketManager:
    MAX_RECONNECT_ATTEMPTS = 3  # after initial attempt
    RETRY_DELAY = 5  # seconds

    def __init__(self, websocket_url: str) -> None:
        self.__websocket_url: str = websocket_url
        self._websocket: Optional[ClientWebSocketResponse] = None
        self._canvas_renderer = DynamicCanvasRenderer()
        self._websocket_task: Optional[asyncio.Task] = None
        self._websocket_command_id: int = 0
        self.__connection_attempts: int = 1
        self._is_canvas_set: bool = False
        self._running: bool = False

    async def add_session(
        self,
        name: str,
        balance: float,
        charges: int,
        notpx_headers: Dict[str, str],
        websocket_headers: Dict[str, str],
        telegram_client: Client,
        proxy: str | None,
        websocket_token: str,
    ) -> None:
        session = SessionData.create(
            name=name,
            balance=balance,
            charges=charges,
            notpx_headers=notpx_headers,
            websocket_headers=websocket_headers,
            telegram_client=telegram_client,
            proxy=proxy,
            websocket_token=websocket_token,
        )

        self.session = session

        await self.run()

    async def run(self) -> None:
        if not self.session:
            raise SessionErrors.NoActiveSessionError("No active session available")

        self._running = True

        if not self._websocket_task or self._websocket_task.done():
            self._websocket_task = asyncio.create_task(self._connect_websocket())
            self._websocket_task.set_name("WebSocket Connection")
            self._websocket_task.add_done_callback(handle_task_completion)

    async def _connect_websocket(self) -> None:
        while self._running:
            if not self.session:
                raise SessionErrors.NoActiveSessionError("No active session available")

            try:
                proxy_connector = (
                    ProxyConnector().from_url(self.session.proxy)
                    if self.session.proxy
                    else None
                )
                async with ClientSession(connector=proxy_connector) as session:
                    async with session.ws_connect(
                        self.__websocket_url,
                        headers=self.session.websocket_headers,
                        protocols=["centrifuge-protobuf"],
                        ssl=settings.ENABLE_SSL,
                    ) as websocket:
                        self._websocket = websocket
                        logger.info(
                            f"WebSocketManager | {self.session.name} | WebSocket connection established"
                        )
                        await self._handle_websocket_connection()
            except asyncio.CancelledError:
                return
            except Exception:
                logger.warning(
                    f"WebSocketManager | {self.session.name} | Connection attempt {self.__connection_attempts} failed, retrying in {self.RETRY_DELAY} seconds"
                )
                dev_logger.warning(traceback.format_exc())
                self.__connection_attempts += 1
                await asyncio.sleep(self.RETRY_DELAY)

    async def _handle_websocket_connection(self) -> None:
        if not self._websocket or self._websocket.closed:
            raise WebSocketErrors.NoConnectionError(
                "WebSocket connection not established"
            )

        await self._handle_websocket_auth()

        while self._running:
            try:
                if not self._websocket or self._websocket.closed:
                    raise WebSocketErrors.NoConnectionError(
                        "WebSocket connection not established"
                    )

                message = await self._websocket.receive()

                if message.type == WSMsgType.CLOSE:
                    raise WebSocketErrors.ServerClosedConnectionError(
                        "WebSocket server closed connection"
                    )

                decoded_message = decode_message(message.data)

                await self._handle_websocket_message(decoded_message)
            except asyncio.CancelledError:
                raise
            except WebSocketErrors.ServerClosedConnectionError:
                logger.warning(
                    f"WebSocketManager | {self.session.name} | WebSocket server closed connection"
                )
                raise
            except Exception:
                logger.warning(
                    f"WebSocketManager | {self.session.name} | Unknown WebSocket error occurred while handling message"
                )
                raise WebSocketErrors.ConnectionError("WebSocket connection failed")

    async def _handle_websocket_message(self, message) -> None:
        if not self._websocket or self._websocket.closed:
            raise WebSocketErrors.NoConnectionError("No WebSocket connection available")

        if not message:
            return

        if message.get("type") == "canvas_image":
            self._canvas_renderer.set_canvas(message.get("data"))
            self._is_canvas_set = True
            return

        elif message.get("type") == "canvas_data" and self._is_canvas_set:
            self._canvas_renderer.update_canvas(message)

        elif message.get("type") == "balance":
            message_data = json.loads(message.get("data"))
            balance = message_data.get("balance")

            if balance > self.session.balance:
                logger.info(
                    f"{self.session.name} | Successfully painted pixel | +{round(balance - self.session.balance, 2)} PX | Charges left: {self.session.charges}"
                )
            else:
                logger.warning(
                    f"{self.session.name} | Failed to paint pixel | Charges left: {self.session.charges}"
                )

            self.session.balance = balance

        elif message.get("type") == "ping":
            pong = encode_commands([{}])
            await self._websocket.send_bytes(pong)

        if self.__connection_attempts > 1:
            self.__connection_attempts = 1

    async def _handle_websocket_auth(self) -> None:
        if not self._websocket or self._websocket.closed:
            raise WebSocketErrors.NoConnectionError("No WebSocket connection available")

        if not self.session:
            raise SessionErrors.NoActiveSessionError("No active session available")

        auth_data = f'{{"token":"{self.session.websocket_token}"}}'
        self._websocket_command_id += 1
        auth_command = [
            {
                "connect": {
                    "data": auth_data.encode(),
                    "name": "js",
                },
                "id": self._websocket_command_id,
            }
        ]

        await self._websocket.send_bytes(encode_commands(auth_command))

    async def send_repaint_command(self, pixel_id: int, hex_color: str) -> None:
        if not self._websocket or self._websocket.closed:
            raise WebSocketErrors.NoConnectionError("No WebSocket connection available")

        repaint_data = f'{{"type": 0, "pixelId": {pixel_id}, "color": "{hex_color}"}}'
        self._websocket_command_id += 1
        repaint_command = [
            {
                "rpc": {"method": "repaintTournament", "data": repaint_data.encode()},
                "id": self._websocket_command_id,
            }
        ]

        await self._websocket.send_bytes(encode_commands(repaint_command))

        self.session.charges -= 1

    @property
    def is_canvas_set(self) -> bool:
        return self._is_canvas_set

    @property
    def get_session_balance(self) -> float:
        return self.session.balance

    @property
    def get_session_charges(self) -> int:
        return self.session.charges

    async def stop(self) -> None:
        self._running = False

        if self._websocket_task:
            self._websocket_task.cancel()
            try:
                await self._websocket_task
            except Exception:
                pass

        logger.info(f"WebSocketManager | {self.session.name} | Stopped")


def handle_task_completion(task: asyncio.Task) -> None:
    try:
        exception = task.exception()
        if exception is not None:
            raise exception
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    except Exception as error:
        logger.error(
            f"{f'WebSocketManager | {error.__str__()}' if error else 'WebSocketManager | Something went wrong'}"
        )
        dev_logger.error(f"{traceback.format_exc()}")
        sys.exit(1)
