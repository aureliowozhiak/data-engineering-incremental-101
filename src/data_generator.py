from pathlib import Path

import pandas as pd
from faker import Faker

from src.utils import ensure_directory, log


class DataGenerator:
    """Gera dados fake de vendas e adiciona no source."""

    def __init__(self, source_path: Path):
        self.source_path = source_path
        self.fake = Faker("pt_BR")
        self.products = [
            "Laptop",
            "Headphone",
            "Keyboard",
            "Mouse",
            "Monitor",
            "Webcam",
            "Smartphone",
            "Tablet",
        ]
        ensure_directory(self.source_path.parent)

    def _get_last_id(self) -> int:
        # Busca último ID para manter incremento entre execuções.
        if not self.source_path.exists():
            return 0

        df_source = pd.read_csv(self.source_path, usecols=["id"])
        if df_source.empty:
            return 0
        return int(df_source["id"].max())

    def generate_batch(self, n_rows: int) -> pd.DataFrame:
        """Gera lote com IDs contínuos e salva no source.csv."""
        last_id = self._get_last_id()
        start_id = last_id + 1
        end_id = last_id + n_rows

        rows = []
        for current_id in range(start_id, end_id + 1):
            rows.append(
                {
                    "id": current_id,
                    "customer_name": self.fake.name(),
                    "product": self.fake.random_element(self.products),
                    "value": round(self.fake.pyfloat(min_value=20, max_value=5000), 2),
                    "created_at": self.fake.date_time_this_year().isoformat(),
                }
            )

        df_new_rows = pd.DataFrame(rows)
        write_mode = "a" if self.source_path.exists() else "w"
        include_header = not self.source_path.exists()
        df_new_rows.to_csv(
            self.source_path,
            mode=write_mode,
            index=False,
            header=include_header,
        )

        log(
            f"Batch gerado no source: {n_rows} linhas (id {start_id} -> {end_id})."
        )
        return df_new_rows
