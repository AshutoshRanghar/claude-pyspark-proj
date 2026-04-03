import asyncio
import httpx
import os
import logging
from datetime import datetime
from pathlib import Path

URLS = [
    "https://raw.githubusercontent.com/anshlambagit/AnshLambaYoutube/refs/heads/main/DBT_Masterclass/dim_customer.csv",
    "https://raw.githubusercontent.com/anshlambagit/AnshLambaYoutube/refs/heads/main/DBT_Masterclass/dim_store.csv",
    "https://raw.githubusercontent.com/anshlambagit/AnshLambaYoutube/refs/heads/main/DBT_Masterclass/dim_date.csv",
    "https://raw.githubusercontent.com/anshlambagit/AnshLambaYoutube/refs/heads/main/DBT_Masterclass/dim_product.csv",
    "https://raw.githubusercontent.com/anshlambagit/AnshLambaYoutube/refs/heads/main/DBT_Masterclass/fact_sales.csv",
    "https://raw.githubusercontent.com/anshlambagit/AnshLambaYoutube/refs/heads/main/DBT_Masterclass/fact_returns.csv",
]

BASE_DIR = Path(__file__).parent
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

data_dir = BASE_DIR / "data" / timestamp
log_dir = BASE_DIR / "logs" / timestamp

data_dir.mkdir(parents=True, exist_ok=True)
log_dir.mkdir(parents=True, exist_ok=True)

log_file = log_dir / "fetchAPI.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


async def fetch_csv(client: httpx.AsyncClient, url: str) -> tuple[str, bool, str]:
    file_name = url.split("/")[-1]
    logger.info(f"Fetching: {url}")
    try:
        response = await client.get(url, timeout=30.0)
        response.raise_for_status()
        save_path = data_dir / file_name
        save_path.write_bytes(response.content)
        logger.info(f"SUCCESS: {file_name} saved to {save_path} ({len(response.content)} bytes)")
        return file_name, True, ""
    except httpx.HTTPStatusError as exc:
        error_msg = f"HTTP {exc.response.status_code} for {url}"
        logger.error(f"FAILED: {file_name} — {error_msg}")
        return file_name, False, error_msg
    except Exception as exc:
        error_msg = str(exc)
        logger.error(f"FAILED: {file_name} — {error_msg}")
        return file_name, False, error_msg


async def main():
    logger.info(f"Starting fetch run — timestamp: {timestamp}")
    logger.info(f"Data directory: {data_dir}")
    logger.info(f"Log directory:  {log_dir}")
    logger.info(f"Total URLs to fetch: {len(URLS)}")

    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[fetch_csv(client, url) for url in URLS])

    successful = [r for r in results if r[1]]
    failed = [r for r in results if not r[1]]

    logger.info("--- Summary ---")
    logger.info(f"Successful: {len(successful)}/{len(URLS)}")
    for name, _, _ in successful:
        logger.info(f"  OK: {name}")

    if failed:
        logger.warning(f"Failed: {len(failed)}/{len(URLS)}")
        for name, _, err in failed:
            logger.warning(f"  FAIL: {name} — {err}")
    else:
        logger.info("All files fetched successfully.")


if __name__ == "__main__":
    asyncio.run(main())
