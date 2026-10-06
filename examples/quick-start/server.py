"""Quick-start Socket.IO chat server with a bot, paired with a Rust client."""

import logging

import socketio
import uvicorn

logger = logging.getLogger("quick_start_server")

sio = socketio.AsyncServer(async_mode="asgi")
app = socketio.ASGIApp(sio)

# The bot stands in for other room members, so a single client still sees
# broadcasts.
BOT = "bot"


@sio.event
async def join(sid: str, room: str, name: str) -> tuple[int]:
    """Add a client to a chat room and announce it.

    Args:
        sid: Socket ID of the client joining the room.
        room: Name of the room to join.
        name: Display name of the client.

    Returns:
        A 1-tuple with the member count, including the bot.
    """
    logger.info('server <- join      %s wants room "%s"', name, room)
    await sio.save_session(sid, {"room": room, "name": name})
    await sio.enter_room(sid, room)
    await sio.emit("notice", f"{name} joined {room}", room=room)
    members = len(list(sio.manager.get_participants("/", room))) + 1
    return (members,)


@sio.event
async def message(sid: str, text: str) -> None:
    """Broadcast a chat message, let the bot reply, then ask to leave.

    Args:
        sid: Socket ID of the sender.
        text: Message text.
    """
    logger.info("server <- message   %s", text)
    session = await sio.get_session(sid)
    room, name = session["room"], session["name"]

    await sio.emit("message", (name, text), room=room, skip_sid=sid)
    await sio.emit("message", (BOT, f'hi {name}, you said "{text}"'), room=room)

    leave = await sio.call("confirm", "Leave the room?", to=sid, timeout=5)
    if not isinstance(leave, bool):
        msg = f"expected a bool answer, got {leave!r}"
        raise TypeError(msg)
    logger.info("server <- ack       confirm: %s", "yes" if leave else "no")
    if leave:
        await sio.disconnect(sid)


@sio.event
async def image(sid: str, name: str, data: bytes) -> None:
    """Broadcast an image to the whole room, sender included.

    Args:
        sid: Socket ID of the sender.
        name: File name of the image.
        data: Image bytes.
    """
    logger.info("server <- image     %s (%d bytes)", name, len(data))
    session = await sio.get_session(sid)
    room = session["room"]
    await sio.emit("image", (name, data), room=room)


def main() -> None:
    """Run the example chat server."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logger.info("listening on http://127.0.0.1:3000")
    uvicorn.run(app, host="127.0.0.1", port=3000, log_level="warning")


if __name__ == "__main__":
    main()
