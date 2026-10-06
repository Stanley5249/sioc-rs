"""Reference server for protocol and backpressure integration tests."""

import asyncio
import socket
import sys
from typing import override

import socketio
import socketio.exceptions
import uvicorn

sio = socketio.AsyncServer(
    async_mode="asgi", max_http_buffer_size=256, allow_upgrades=False
)
counts: dict[str, int] = {}


@sio.event
async def connect(sid: str, _environ: dict[str, object]) -> None:
    """Initialize the connection's receive count."""
    counts[sid] = 0


async def observe(_sid: str, _environ: dict[str, object]) -> None:
    """Accept disconnect observers."""


async def denied(_sid: str, _environ: dict[str, object]) -> None:
    """Reject this namespace without ending the session."""
    message = "denied"
    raise socketio.exceptions.ConnectionRefusedError(message)


sio.on("connect", handler=observe, namespace="/observe")
sio.on("connect", handler=denied, namespace="/denied")


@sio.event
async def flood(sid: str, count: int) -> None:
    """Flood ordered events to the Rust client's bounded queues."""
    for seq in range(count):
        await sio.emit("item", seq, to=sid)


@sio.event
async def seen(sid: str, _seq: int) -> None:
    """Count echoes from a client under server pressure."""
    counts[sid] += 1


@sio.event
async def echo(sid: str, seq: int) -> None:
    """Echo events while the Rust client sends under receive pressure."""
    counts[sid] += 1
    await sio.emit("item", seq, to=sid)


@sio.event
async def count(sid: str) -> tuple[int]:
    """Return the exact number of events received from this client."""
    return (counts[sid],)


@sio.event
async def blob(sid: str, data: bytes) -> tuple[bytes]:
    """Echo binary attachments both as an event and as an acknowledgement."""
    await sio.emit("blob", data, to=sid)
    return (data,)


@sio.event
async def kick(sid: str) -> None:
    """Disconnect only this namespace."""
    await sio.disconnect(sid)


@sio.event
async def disconnect(sid: str, _reason: str) -> None:
    """Notify observers and release connection state."""
    counts.pop(sid, None)
    await sio.emit("gone", namespace="/observe")


class ReadyServer(uvicorn.Server):
    """Report readiness after uvicorn has installed its socket listeners."""

    @override
    async def startup(self, sockets: list[socket.socket] | None = None) -> None:
        """Start listeners and report the bound ephemeral port."""
        await super().startup(sockets=sockets)
        if sockets:
            sys.stdout.write(f"{sockets[0].getsockname()[1]}\n")
            sys.stdout.flush()


async def main() -> None:
    """Bind an ephemeral port, report readiness, and serve until stdin closes."""
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        server = ReadyServer(
            uvicorn.Config(
                socketio.ASGIApp(sio), log_level="error", timeout_graceful_shutdown=1
            )
        )
        task = asyncio.create_task(server.serve(sockets=[listener]))
        await asyncio.to_thread(sys.stdin.readline)
        server.should_exit = True
        await task


if __name__ == "__main__":
    asyncio.run(main())
