import luigi
import pandas as pd
import sentry_sdk
from sentry_sdk import capture_exception, capture_message
from sqlalchemy import create_engine
from dotenv import load_dotenv
from loguru import logger
from datetime import datetime
import os

load_dotenv()

SENTRY_DSN = (os.getenv("SENTRY_DSN") or "").strip()
if SENTRY_DSN:
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        environment="production",
        traces_sample_rate=1.0,
    )

# ── Koneksi ──────────────────────────────────────────────
SRC_CONN = (
    f"postgresql://{os.getenv('SRC_POSTGRES_USER')}:{os.getenv('SRC_POSTGRES_PASSWORD')}"
    f"@{os.getenv('SRC_POSTGRES_HOST')}:{os.getenv('SRC_POSTGRES_PORT')}/{os.getenv('SRC_POSTGRES_DB')}"
)
DWH_CONN = (
    f"postgresql://{os.getenv('DWH_POSTGRES_USER')}:{os.getenv('DWH_POSTGRES_PASSWORD')}"
    f"@{os.getenv('DWH_POSTGRES_HOST')}:{os.getenv('DWH_POSTGRES_PORT')}/{os.getenv('DWH_POSTGRES_DB')}"
)

SRC_SCHEMA = os.getenv("SRC_SCHEMA", "public")

TABLES = [
    "aircrafts", "airlines", "airports",
    "customers", "hotel",
    "flight_bookings", "hotel_bookings"
]

# ── Task per tabel ────────────────────────────────────────
class ExtractLoadTable(luigi.Task):
    """
    Extract 1 tabel dari source DB,
    load ke staging schema di DWH DB.
    """
    table_name = luigi.Parameter()
    run_date   = luigi.DateParameter(default=datetime.today())

    def output(self):
        os.makedirs("logs", exist_ok=True)
        return luigi.LocalTarget(
            f"logs/{self.table_name}_{self.run_date}.txt"
        )

    def run(self):
        sentry_sdk.set_tag("table", self.table_name)
        sentry_sdk.set_tag("run_date", str(self.run_date))
        sentry_sdk.set_context("pipeline", {
            "stage": "Extract & Load",
            "table": self.table_name,
        })

        try:
            src_engine = create_engine(SRC_CONN)
            dwh_engine = create_engine(DWH_CONN)

            logger.info(f"[EXTRACT] Membaca tabel: {self.table_name}")
            df = pd.read_sql(
                f"SELECT * FROM {SRC_SCHEMA}.{self.table_name}",
                src_engine
            )

            # ── Validasi data kosong ──────────────────────
            if df.empty:
                capture_message(
                    f"[WARNING] Tabel '{self.table_name}' kosong!",
                    level="warning"
                )
                logger.warning(f"Tabel {self.table_name} kosong!")

            # ── Load ke staging ───────────────────────────
            logger.info(
                f"[LOAD] {len(df)} baris → staging.{self.table_name}"
            )
            df.to_sql(
                name=self.table_name,
                con=dwh_engine,
                schema="staging",
                if_exists="replace",  # replace = full load
                index=False
            )

            # ── Tulis output file (tanda task selesai) ────
            with self.output().open("w") as f:
                f.write(
                    f"SUCCESS|{self.table_name}|{len(df)} rows"
                    f"|{datetime.now()}"
                )
            logger.success(
                f"[DONE] staging.{self.table_name} ({len(df)} baris)"
            )

        except Exception as e:
            capture_exception(e)
            logger.error(f"[FAILED] {self.table_name}: {e}")
            raise e  # wajib raise agar Luigi tahu task gagal


# ── Wrapper: jalankan semua tabel sekaligus ───────────────
class RunAllExtractLoad(luigi.WrapperTask):
    """
    Orkestrasi: jalankan ExtractLoadTable
    untuk semua tabel secara paralel.
    """
    run_date = luigi.DateParameter(default=datetime.today())

    def requires(self):
        return [
            ExtractLoadTable(
                table_name=t,
                run_date=self.run_date
            )
            for t in TABLES
        ]


if __name__ == "__main__":
    luigi.run()