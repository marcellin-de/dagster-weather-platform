import tempfile
import unittest
from pathlib import Path

import duckdb
import pandas as pd

from dagster_weather_intelligence_platform.assets.weather_enriched import (
    _persist_enriched_to_duckdb,
)


class WeatherEnrichedAssetTestCase(unittest.TestCase):
    def test_persist_enriched_to_duckdb_writes_expected_table(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "weather.duckdb"
            df = pd.DataFrame(
                [
                    {"day_utc": "2026-03-03", "label": "clear", "summary": "Temps degage."},
                    {"day_utc": "2026-03-04", "label": "rainy", "summary": "Pluie probable."},
                ]
            )

            written_path = _persist_enriched_to_duckdb(df=df, db_path=str(db_path))
            self.assertEqual(str(db_path), written_path)

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                row_count = con.execute(
                    "select count(*) from analytics.weather_daily_enriched"
                ).fetchone()[0]
                self.assertEqual(2, row_count)

                labels = [
                    row[0]
                    for row in con.execute(
                        "select label from analytics.weather_daily_enriched order by day_utc"
                    ).fetchall()
                ]
                self.assertEqual(["clear", "rainy"], labels)
            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()
