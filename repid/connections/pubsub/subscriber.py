from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING

from gcloud.aio.pubsub import SubscriberClient

from repid.logger import logger

from .helpers import PubsubReceivedMessage, _QueuedDelivery

if TYPE_CHECKING:
    from .helpers import _ChannelConfig
    from .message_broker import PubsubServer


class PubsubSubscriber:
    def __init__(
        self,
        *,
        subscriber_client: SubscriberClient,
        channel_configs: list[_ChannelConfig],
        pull_batch_size: int,
        pull_idle_sleep: float,
        concurrency_limit: int | None,
        server: PubsubServer,
    ) -> None:
        self._subscriber_client = subscriber_client
        self._channel_configs = channel_configs
        self._pull_batch_size = pull_batch_size
        self._pull_idle_sleep = pull_idle_sleep
        self._concurrency_limit = concurrency_limit
        self._server = server

        self._pause_event = asyncio.Event()
        self._pause_event.set()
        self._shutdown_event = asyncio.Event()
        self._is_active = True
        self._is_closing = False

        self._delivery_queue: asyncio.Queue[_QueuedDelivery] = asyncio.Queue()
        self._concurrency_semaphore: asyncio.Semaphore | None = (
            asyncio.Semaphore(concurrency_limit)
            if concurrency_limit and concurrency_limit > 0
            else None
        )
        self._callback_tasks: set[asyncio.Task[None]] = set()
        self._task: asyncio.Task[None] | None = None

    @classmethod
    async def create(
        cls,
        *,
        subscriber_client: SubscriberClient,
        channel_configs: list[_ChannelConfig],
        concurrency_limit: int | None,
        pull_batch_size: int,
        pull_idle_sleep: float,
        server: PubsubServer,
    ) -> PubsubSubscriber:
        subscriber = cls(
            subscriber_client=subscriber_client,
            channel_configs=channel_configs,
            pull_batch_size=pull_batch_size,
            pull_idle_sleep=pull_idle_sleep,
            concurrency_limit=concurrency_limit,
            server=server,
        )
        subscriber._start_background_tasks()
        return subscriber

    async def _process_background(self) -> None:
        await asyncio.gather(
            *(self._pull_loop(config) for config in self._channel_configs),
            self._dispatch_loop(),
        )

    def _start_background_tasks(self) -> None:
        if not self._channel_configs:
            self._is_active = False
            return
        self._task = asyncio.create_task(self._process_background())
        self._is_active = True

    async def _pull_loop(self, config: _ChannelConfig) -> None:
        backoff = 1.0
        max_backoff = 30.0
        while not self._shutdown_event.is_set():
            await self._pause_event.wait()

            try:
                messages = await self._subscriber_client.pull(
                    config.subscription_path,
                    max_messages=self._pull_batch_size,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "PubSub pull failed.",
                    extra={"subscription": config.subscription_path},
                    exc_info=exc,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
                continue

            backoff = 1.0

            if not messages:
                await asyncio.sleep(self._pull_idle_sleep)
                continue

            for raw in messages:
                received = PubsubReceivedMessage(
                    raw=raw,
                    subscription_path=config.subscription_path,
                    channel_name=config.channel,
                    subscriber_client=self._subscriber_client,
                    server=self._server,
                )
                if self._concurrency_semaphore is not None:
                    await self._concurrency_semaphore.acquire()
                await self._delivery_queue.put(
                    _QueuedDelivery(callback=config.callback, message=received),
                )

        logger.debug(
            "Stopping Pub/Sub pull worker.",
            extra={"subscription": config.subscription_path},
        )

    async def _dispatch_loop(self) -> None:
        try:
            while True:
                delivery = await self._delivery_queue.get()
                task = asyncio.create_task(self._execute_callback(delivery))
                self._callback_tasks.add(task)
                task.add_done_callback(self._callback_tasks.discard)
                self._delivery_queue.task_done()
        except asyncio.CancelledError:
            raise
        finally:
            logger.debug("Stopping Pub/Sub dispatcher.")

    async def _execute_callback(self, delivery: _QueuedDelivery) -> None:
        try:
            await delivery.callback(delivery.message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Error inside Pub/Sub channel callback.",
                exc_info=exc,
            )
        finally:
            if self._concurrency_semaphore is not None:
                self._concurrency_semaphore.release()

    @property
    def is_active(self) -> bool:
        return self._is_active and not self._shutdown_event.is_set()

    @property
    def task(self) -> asyncio.Task:
        if self._task is None:
            raise RuntimeError("Subscriber has not been started.")
        return self._task

    async def pause(self) -> None:
        if not self.is_active:
            return
        self._pause_event.clear()
        self._is_active = False

    async def resume(self) -> None:
        if self._shutdown_event.is_set():
            return
        self._pause_event.set()
        self._is_active = True

    async def close(self) -> None:
        if self._is_closing:
            return
        self._is_closing = True
        self._shutdown_event.set()
        self._pause_event.clear()
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

        self._is_active = False

        # it's responsibility of the Runner to ensure that all callbacks are terminated gracefully,
        # so by the time we reach here, we shouldn't have any pending callbacks
        if self._callback_tasks:
            logger.warning(
                "There are still %d pending Pub/Sub callback tasks during subscriber shutdown.",
                len(self._callback_tasks),
            )
            callbacks = self._callback_tasks.copy()
            self._callback_tasks.clear()
            for task in callbacks:
                task.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.wait(callbacks)
