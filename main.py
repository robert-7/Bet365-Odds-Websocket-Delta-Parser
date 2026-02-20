import asyncio
import logging
import os

from dotenv import load_dotenv

from bet365 import Bet365Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

async def main():
    # Load environment variables from .env file
    load_dotenv()
    url = os.getenv("BET365_WEBSOCKET_URL")
    cookie = os.getenv("TSTK_COOKIE")

    if not url or not cookie:
        logger.error("Missing BET365_WEBSOCKET_URL or TSTK_COOKIE in .env file.")
        return

    client = Bet365Client(url, cookie)
    await client.connect()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped due to keyboard interrupt.")
