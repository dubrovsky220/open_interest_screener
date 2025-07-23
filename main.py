import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from scheduler.job import run_signal_job
from config.config import RUN_EVERY_SECONDS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

async def main():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(run_signal_job, "interval", seconds=RUN_EVERY_SECONDS)
    scheduler.start()

    print("Scheduler started. Press Ctrl+C to exit.")
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        print("Scheduler stopped.")

if __name__ == "__main__":
    asyncio.run(main())