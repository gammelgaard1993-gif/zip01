from __future__ import annotations

import asyncio

from models import Priority, ValidatedEvent

class PriorityEventQueue:
    def __init__(self, normal_max_size: int) -> None:
        self.high_queue: asyncio.Queue[ValidatedEvent] = asyncio.Queue()
        self.normal_queue: asyncio.Queue[ValidatedEvent] = asyncio.Queue(maxsize=normal_max_size)
        self._normal_max_size = normal_max_size

    def qsize_high(self) -> int:
        return self.high_queue.qsize()

    def qsize_normal(self) -> int:
        return self.normal_queue.qsize()

    def normal_max_size(self) -> int:
        return self._normal_max_size

    def normal_is_full(self) -> bool:
        return self.normal_queue.full()

    async def put(self, event: ValidatedEvent) -> None:
        if event.priority == Priority.HIGH:
            await self.high_queue.put(event)
            return

        await self.normal_queue.put(event)

    def put_nowait(self, event: ValidatedEvent) -> None:
        if event.priority == Priority.HIGH:
            self.high_queue.put_nowait(event)
            return

        self.normal_queue.put_nowait(event)

    async def get(self) -> ValidatedEvent:
        # Fast path: serve HIGH before NORMAL without awaiting when data is ready.
        if not self.high_queue.empty():
            return self.high_queue.get_nowait()
        if not self.normal_queue.empty():
            return self.normal_queue.get_nowait()

        # Both lanes empty: wait on *both* so an idle consumer wakes on a HIGH arrival instead
        # of parking on the NORMAL lane (which would delay a fall_warn until the next NORMAL event).
        high_getter = asyncio.ensure_future(self.high_queue.get())
        normal_getter = asyncio.ensure_future(self.normal_queue.get())
        try:
            await asyncio.wait(
                {high_getter, normal_getter}, return_when=asyncio.FIRST_COMPLETED
            )
        except asyncio.CancelledError:
            high_getter.cancel()
            normal_getter.cancel()
            raise

        # Cancelling a pending asyncio.Queue.get() leaves the (absent) item untouched, so the
        # lane that did not fire loses nothing.
        high_event = self._collect(high_getter)
        normal_event = self._collect(normal_getter)

        if high_event is not None:
            if normal_event is not None:
                # Both fired at once: serve HIGH, return NORMAL to its lane. We just took one out,
                # so there is room and put_nowait cannot raise.
                self.normal_queue.put_nowait(normal_event)
            return high_event
        # HIGH did not fire, so FIRST_COMPLETED guarantees NORMAL did.
        assert normal_event is not None
        return normal_event

    @staticmethod
    def _collect(getter: "asyncio.Task[ValidatedEvent]") -> ValidatedEvent | None:
        # Called synchronously right after asyncio.wait, so the getter's done-state is stable.
        # A completed getter has already removed its item from the queue; a pending one is
        # cancelled (its item, if any, stays in the queue for the next get()).
        if getter.done():
            return None if getter.cancelled() else getter.result()
        getter.cancel()
        return None

    def empty(self) -> bool:
        return self.high_queue.empty() and self.normal_queue.empty()

    def qsize(self) -> int:
        return self.qsize_high() + self.qsize_normal()

    def clear(self) -> None:
        while not self.high_queue.empty():
            self.high_queue.get_nowait()
        while not self.normal_queue.empty():
            self.normal_queue.get_nowait()
