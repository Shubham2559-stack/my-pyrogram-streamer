# ================================
# STREAMER.PY
# Pyrogram Video Streamer
# Bade videos — No size limit!
# ================================

import os
import logging
import asyncio
import threading
import time
from flask import Flask, request, Response, jsonify
from pyrogram import Client
from pyrogram.errors import FloodWait

# --------------------------------
# Logging
# --------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --------------------------------
# Config
# --------------------------------
API_ID    = int(os.environ.get("API_ID", "38205081"))
API_HASH  = os.environ.get("API_HASH", "0ceeb2432d1bd6dd34f48b2a5971d0c3")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8714162717:AAHro-UFaJhw2x-Ne2EU3jCfidZ-BquKlqE")
PORT      = int(os.environ.get("PORT", 8000))

# Validate
if not API_ID or not API_HASH or not BOT_TOKEN:
    logger.error("❌ API_ID, API_HASH, BOT_TOKEN missing!")
    exit(1)

logger.info(f"✅ API_ID: {API_ID}")

# --------------------------------
# Flask App
# --------------------------------
app = Flask(__name__)

# --------------------------------
# Async Loop
# --------------------------------
loop = asyncio.new_event_loop()

# --------------------------------
# Pyrogram Client
# --------------------------------
pyro = Client(
    name="streamer",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    no_updates=True,
    in_memory=True
)

# Connected flag
is_connected = False

# ================================
# STREAM GENERATOR
# ================================
async def stream_generator(file_id, offset=0, limit=None):
    """
    File chunks yield karta hai.
    offset = start bytes
    limit  = kitne bytes tak
    """
    chunk_size       = 1024 * 1024  # 1MB
    start_chunk      = offset // chunk_size
    offset_in_chunk  = offset % chunk_size
    chunks_yielded   = 0
    bytes_yielded    = 0

    try:
        async for chunk in pyro.stream_media(
            file_id,
            offset=start_chunk
        ):
            # Pehle chunk mein offset handle karo
            if chunks_yielded == 0 and offset_in_chunk > 0:
                chunk = chunk[offset_in_chunk:]

            # Limit check
            if limit is not None:
                remaining = limit - bytes_yielded
                if len(chunk) > remaining:
                    chunk = chunk[:remaining]

            yield chunk
            bytes_yielded  += len(chunk)
            chunks_yielded += 1

            if limit is not None and bytes_yielded >= limit:
                break

    except FloodWait as e:
        logger.warning(f"FloodWait: {e.value}s")
        await asyncio.sleep(e.value)
    except Exception as e:
        logger.error(f"Stream error: {e}")

# ================================
# ROUTES
# ================================

@app.route('/health')
def health():
    """Health check"""
    return jsonify({
        'status':    'ok',
        'streamer':  'pyrogram',
        'connected': is_connected
    })

@app.route('/stream')
def stream():
    """
    Video stream.
    URL: /stream?file_id=FILE_ID
    Range headers support hai.
    """
    file_id = request.args.get('file_id', '')

    if not file_id:
        return jsonify({'error': 'file_id missing'}), 400

    if not is_connected:
        return jsonify({'error': 'Pyrogram not connected'}), 503

    # Range header parse karo
    range_header = request.headers.get('Range', '')
    start  = 0
    end    = None
    length = None

    if range_header:
        try:
            range_str = range_header.replace('bytes=', '')
            parts     = range_str.split('-')
            start     = int(parts[0]) if parts[0] else 0
            end       = int(parts[1]) if len(parts) > 1 and parts[1] else None
            if end is not None:
                length = end - start + 1
        except Exception as e:
            logger.error(f"Range parse error: {e}")

    logger.info(
        f"Stream: {file_id[:15]}... "
        f"start={start} end={end}"
    )

    def sync_generator():
        """Async generator ko sync mein chalao"""
        async def async_gen():
            async for chunk in stream_generator(
                file_id, start, length
            ):
                yield chunk

        ait = async_gen().__aiter__()

        while True:
            try:
                chunk = loop.run_until_complete(
                    ait.__anext__()
                )
                yield chunk
            except StopAsyncIteration:
                break
            except Exception as e:
                logger.error(f"Generator error: {e}")
                break

    # Response headers
    headers = {
        'Content-Type':  'video/mp4',
        'Accept-Ranges': 'bytes',
        'Cache-Control': 'no-cache',
        'Access-Control-Allow-Origin': '*',
    }

    if range_header and end:
        headers['Content-Range'] = f'bytes {start}-{end}/*'
        if length:
            headers['Content-Length'] = str(length)
        status = 206
    else:
        status = 200

    return Response(
        sync_generator(),
        status=status,
        headers=headers,
        direct_passthrough=True
    )

@app.route('/')
def home():
    return jsonify({
        'service':   'Pyrogram Streamer',
        'status':    'ok',
        'connected': is_connected
    })

# ================================
# PYROGRAM START
# ================================
def start_pyrogram():
    """Pyrogram background mein chalao"""
    global is_connected
    asyncio.set_event_loop(loop)

    async def connect():
        global is_connected
        try:
            await pyro.start()
            is_connected = True
            me = await pyro.get_me()
            logger.info(f"✅ Pyrogram connected! Bot: @{me.username}")
        except Exception as e:
            logger.error(f"❌ Pyrogram error: {e}")
            is_connected = False

    loop.run_until_complete(connect())
    loop.run_forever()

# ================================
# MAIN
# ================================
if __name__ == '__main__':
    logger.info("🚀 Pyrogram Streamer starting...")
    logger.info(f"✅ API_ID: {API_ID}")
    logger.info(f"✅ PORT: {PORT}")

    # Pyrogram thread
    pyro_thread = threading.Thread(
        target=start_pyrogram,
        daemon=True
    )
    pyro_thread.start()

    # Connect hone ka wait
    logger.info("⏳ Telegram connect hone ka wait...")
    time.sleep(8)

    # Flask start
    logger.info(f"🌐 Flask on port {PORT}...")
    app.run(
        host='0.0.0.0',
        port=PORT,
        debug=False,
        use_reloader=False,
        threaded=True
    )
