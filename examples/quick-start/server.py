"""Quick-start Socket.IO server demonstrating typed events with a Rust client."""

import asyncio
import logging
from typing import Any

import socketio
import uvicorn

logger = logging.getLogger("quick_start_server")

sio = socketio.AsyncServer(async_mode="asgi")
app = socketio.ASGIApp(sio)

_background_tasks: set[asyncio.Task[None]] = set()


@sio.event
async def connect(sid: str, _environ: dict[str, Any]) -> None:
    """Handle incoming client connection and start the interactive session.

    Args:
        sid: Socket ID assigned to the connected client.
        _environ: WSGI/ASGI connection environment metadata.
    """
    task = asyncio.create_task(session(sid))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


@sio.event
async def join(sid: str, data: str) -> tuple[int]:
    """Handle room join request from a client.

    Args:
        sid: Socket ID of the client joining the room.
        data: Name of the room to join.

    Returns:
        A 1-tuple containing the initial member count acknowledgment.
    """
    logger.info("%s joined room %r", sid, data)
    return (1,)


@sio.event
async def reply(sid: str, data: str) -> None:
    """Handle text reply sent by a client.

    Args:
        sid: Socket ID of the sender.
        data: Reply message payload.
    """
    logger.info("reply from %s: %s", sid, data)


@sio.event
async def upload(sid: str, name: str, header: bytes, body: bytes) -> None:
    """Handle binary file upload and respond with a processed data chunk.

    Args:
        sid: Socket ID of the uploading client.
        name: Name of the uploaded file.
        header: Header binary payload.
        body: Body binary payload.
    """
    logger.info(
        "upload from %s: %r header=%dB body=%dB",
        sid,
        name,
        len(header),
        len(body),
    )
    chunk = b"received: " + header[:4]
    await sio.emit("chunk", (name, chunk), to=sid)


async def session(sid: str) -> None:
    """Run an interactive session sequence with the connected client.

    Args:
        sid: Socket ID of the target client.
    """
    await sio.emit("greeting", "Welcome to the lobby!", to=sid)

    options = ["Rust", "Python", "JavaScripts"]

    ack: int = await sio.call(
        "poll",
        ("Favorite language?", options),
        to=sid,
        timeout=5,
    )
    logger.info("poll vote from %s: option %s", sid, options[ack])

    await sio.disconnect(sid)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="localhost", port=3000)
