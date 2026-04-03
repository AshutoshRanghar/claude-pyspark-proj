"""
Fetch sample CSV data from GitHub and save to project data folder.
Uses async httpx as per fetchApi skill requirements.
"""

import asyncio
import httpx
import os
from pathlib import Path
from datetime import datetime

# URLs to fetch
URLS = [
    "https://raw.githubusercontent.com/anshlambagit/AnshLambaYoutube/refs/heads/main/DBT_Masterclass/dim_customer.csv",
    "https://raw.githubusercontent.com/anshlambagit/AnshLambaYoutube/refs/heads/main/DBT_Masterclass/dim_store.csv",
    "https://raw.githubusercontent.com/anshlambagit/AnshLambaYoutube/refs/heads/main/DBT_Masterclass/dim_date.csv",
    "https://raw.githubusercontent.com/anshlambagit/AnshLambaYoutube/refs/heads/main/DBT_Masterclass/dim_product.csv",
    "https://raw.githubusercontent.com/anshlambagit/AnshLambaYoutube/refs/heads/main/DBT_Masterclass/fact_sales.csv",
    "https://raw.githubusercontent.com/anshlambagit/AnshLambaYoutube/refs/heads/main/DBT_Masterclass/fact_returns.csv",
]

# Project root data folder
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# Log directory
LOG_DIR = Path(__file__).parent / ".claude" / "skills" / "fetchApi" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Timestamp for logging
TIMESTAMP = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_FILE = LOG_DIR / TIMESTAMP / "fetchAPI.log"
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)


async def fetch_files():
    """Fetch CSV files from GitHub using async httpx."""
    log_messages = []
    successful = 0
    failed = 0

    log_messages.append(f"[{datetime.now()}] Starting fetch_sample_data.py")
    log_messages.append(f"[{datetime.now()}] Data directory: {DATA_DIR}")
    log_messages.append(f"[{datetime.now()}] Fetching {len(URLS)} files...")

    async with httpx.AsyncClient(timeout=30.0) as client:
        for url in URLS:
            filename = url.split("/")[-1]
            filepath = DATA_DIR / filename

            try:
                log_messages.append(f"[{datetime.now()}] Fetching: {url}")
                response = await client.get(url)
                response.raise_for_status()

                # Save to file
                filepath.write_text(response.text)
                log_messages.append(f"[{datetime.now()}] [SUCCESS] Saved {filename} ({len(response.text)} bytes)")
                successful += 1

            except Exception as e:
                log_messages.append(f"[{datetime.now()}] [FAILED] {filename} - {str(e)}")
                failed += 1

    # Summary
    log_messages.append("")
    log_messages.append(f"[{datetime.now()}] ===== SUMMARY =====")
    log_messages.append(f"[{datetime.now()}] Total URLs: {len(URLS)}")
    log_messages.append(f"[{datetime.now()}] Successful: {successful}")
    log_messages.append(f"[{datetime.now()}] Failed: {failed}")
    log_messages.append(f"[{datetime.now()}] Data saved to: {DATA_DIR}")
    log_messages.append(f"[{datetime.now()}] Fetch completed!")

    # Write log file (with UTF-8 encoding for special characters)
    LOG_FILE.write_text("\n".join(log_messages), encoding='utf-8')
    print(f"Log saved to: {LOG_FILE}")

    # Print summary
    print("\n".join(log_messages))

    return successful, failed


if __name__ == "__main__":
    print(f"Fetching sample data to {DATA_DIR}...\n")
    successful, failed = asyncio.run(fetch_files())

    if failed == 0:
        print(f"\n[SUCCESS] All {successful} files fetched successfully!")
    else:
        print(f"\n[WARNING] {successful} successful, {failed} failed")
