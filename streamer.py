# ================================
# STREAMER.PY - Final Fixed
# pyrogramv2 use karta hai
# ================================

import os
import logging
import asyncio
import threading
import time
from flask import Flask, request, Response, jsonify
from pyrogramv2 import Client
from pyrogramv2.errors import FloodWait

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Config
API_ID    = int(os.environ.get("API_ID", "38205081"))
API_HASH  = os.environ.get("API_HASH", "0ceeb2432d1bd6dd34f48b2a5971d0c3")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8714162717:AAHro-UFaJhw2x-Ne2EU3jCfidZ-BquKlqE")
PORT      = int(os.environ.get("PORT", 8000))

if not API_ID or not API_HASH or not BOT_TOKEN:
    logger.error("❌ Credentials missing!")
    exit(1)

logger.info(f"✅ API_ID: {API_ID}")
logger.info(f"✅ BOT_TOKEN: {BOT_TOKEN[:10]}...")

# Flask
app = Flask(__name__)

# Globals
loop         = None
pyro         = None
is_connected = False

# ================================
# CLIENT SETUP
# ================================
async def setup_client():
    global pyro, is_connected

    try:
        logger.info("🔄 Pyrogram client banao...")

        pyro = Client(
            name="streamer_bot",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            no_updates=True,
            in_memory=True
        )

        logger.info("🔄 Telegram se connect ho raha hai...")
        await pyro.start()

        me = await pyro.get_me()
        is_connected = True
        logger.info(f"✅ Connected! Bot: @{me.username}")

    except Exception as e:
        logger.error(f"❌ Connection failed: {e}")
        import traceback
        traceback.print_exc()
        is_connected = False

# ================================
# STREAM GENERATOR
# ================================
async def stream_file(file_id, offset=0, limit=None):
    """File chunks yield karo"""
    chunk_size      = 1024 * 1024  # 1MB
    start_chunk     = offset // chunk_size
    offset_in_chunk = offset % chunk_size
    chunks_done     = 0
    bytes_done      = 0

    try:
        async for chunk in pyro.stream_media(
            file_id,
            offset=start_chunk
        ):
            if chunks_done == 0 and offset_in_chunk > 0:
                chunk = chunk[offset_in_chunk:]

            if limit is not None:
                remaining = limit - bytes_done
                if len(chunk) > remaining:
                    chunk = chunk[:remaining]

            yield chunk
            bytes_done  += len(chunk)
            chunks_done += 1

            if limit is not None and bytes_done >= limit:
                break

    except FloodWait as e:
        logger.warning(f"FloodWait: {e.value}s")
        await asyncio.sleep(e.value)
    except Exception as e:
        logger.error(f"Stream error: {e}")

# ================================
# ROUTES
# ================================

@app.route('/')
def home():
    return jsonify({
        'service':   'Pyrogram Streamer',
        'status':    'ok',
        'connected': is_connected
    })

@app.route('/health')
def health():
    return jsonify({
        'status':    'ok',
        'streamer':  'pyrogram',
        'connected': is_connected,
        'api_id':    API_ID
    })

@app.route('/stream')
def stream():
    """Video stream endpoint"""
    file_id = request.args.get('file_id', '')

    if not file_id:
        return jsonify({'error': 'file_id missing'}), 400

    if not is_connected:
        return jsonify({
            'error': 'Not connected',
            'hint':  'Thoda wait karo'
        }), 503

    # Range header
    range_header = request.headers.get('Range', '')
    start  = 0
    end    = None
    length = None

    if range_header:
        try:
            parts = range_header.replace('bytes=', '').split('-')
            start = int(parts[0]) if parts[0] else 0
            end   = int(parts[1]) if len(parts) > 1 and parts[1] else None
            if end is not None:
                length = end - start + 1
        except:
            pass

    logger.info(f"🎬 Stream: {file_id[:20]}... start={start}")

    def generate():
        """Chunks yield karo"""
        async def get_chunks():
            result = []
            async for chunk in stream_file(
                file_id, start, length
            ):
                result.append(chunk)
            return result

        try:
            future = asyncio.run_coroutine_threadsafe(
                get_chunks(), loop
            )
            chunks = future.result(timeout=60)
            for chunk in chunks:
                yield chunk
        except Exception as e:
            logger.error(f"Generate error: {e}")

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
        generate(),
        status=status,
        headers=headers,
        direct_passthrough=True
    )

# ================================
# EVENT LOOP THREAD
# ================================
def run_event_loop():
    global loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    logger.info("🔄 Event loop starting...")
    loop.run_until_complete(setup_client())
    loop.run_forever()

# ================================
# MAIN
# ================================
if __name__ == '__main__':
    logger.info("🚀 Streamer starting...")
    logger.info(f"API_ID: {API_ID}")
    logger.info(f"PORT: {PORT}")

    # Event loop thread
    t = threading.Thread(
        target=run_event_loop,
        daemon=True
    )
    t.start()

    # Connect hone ka wait
    logger.info("⏳ Connecting to Telegram...")
    time.sleep(12)

    logger.info(f"🌐 Flask on port {PORT}")
    app.run(
        host='0.0.0.0',
        port=PORT,
        debug=False,
        use_reloader=False,
        threaded=True
    )
