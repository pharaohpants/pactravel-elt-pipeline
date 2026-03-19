import subprocess
import schedule
import time
import sentry_sdk
from sentry_sdk import capture_exception, capture_message
from loguru import logger
from dotenv import load_dotenv
import os

load_dotenv()

sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    environment="production",
)

def run_extract_load():
    """Step 1: Jalankan Luigi EL pipeline."""
    logger.info("=" * 50)
    logger.info("STEP 1: Extract & Load dimulai...")
    sentry_sdk.set_tag("step", "extract_load")

    result = subprocess.run(
        [
            "python", "-m", "luigi",
            "--module", "extract_load",
            "RunAllExtractLoad",
            "--local-scheduler"
        ],
        capture_output=True,
        text=True,
        cwd="pipeline"   # pastikan working dir benar
    )

    if result.returncode != 0:
        msg = f"[FAILED] Luigi EL:\n{result.stderr}"
        logger.error(msg)
        capture_message(msg, level="error")
        return False  # hentikan pipeline jika EL gagal

    logger.success("STEP 1: Extract & Load selesai.")
    return True


def run_dbt_transform():
    """Step 2: Jalankan DBT transform."""
    logger.info("STEP 2: DBT Transform dimulai...")
    sentry_sdk.set_tag("step", "dbt_transform")

    result = subprocess.run(
        ["dbt", "run", "--project-dir", "dbt_project/"],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        msg = f"[FAILED] DBT run:\n{result.stderr}"
        logger.error(msg)
        capture_message(msg, level="error")
        return False

    logger.success("STEP 2: DBT Transform selesai.")
    return True


def run_pipeline():
    """Orkestrasi full ELT pipeline."""
    logger.info("▶ Pipeline ELT dimulai")

    # Step 1: Extract & Load
    if not run_extract_load():
        logger.error("Pipeline berhenti di step Extract & Load.")
        return

    # Step 2: Transform
    if not run_dbt_transform():
        logger.error("Pipeline berhenti di step DBT Transform.")
        return

    capture_message("✅ Pipeline ELT selesai sukses!", level="info")
    logger.success("✅ Pipeline ELT selesai seluruhnya.")


# ── Scheduling: jalankan setiap hari pukul 06:00 ──────────
if __name__ == "__main__":
    logger.info("Scheduler aktif — pipeline jalan setiap 06:00")
    schedule.every().day.at("06:00").do(run_pipeline)

    # Untuk testing: jalankan langsung sekali
    # run_pipeline()

    while True:
        schedule.run_pending()
        time.sleep(60)