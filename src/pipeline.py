from pathlib import Path

import pandas as pd

from src.lake_manager import LakeManager
from src.utils import log, read_metadata, timed_step, write_metadata


class Pipeline:
    """Executa as cargas full e incremental."""

    def __init__(self, source_path: Path, metadata_path: Path, lake_manager: LakeManager):
        self.source_path = source_path
        self.metadata_path = metadata_path
        self.lake_manager = lake_manager

    @timed_step("FULL LOAD")
    def run_full_load(self) -> dict:
        """Reprocessa toda a origem e sobrescreve o lake."""
        if not self.source_path.exists():
            log("Source não encontrado. Full load ignorado.")
            return {"rows_processed": 0, "last_processed_id": 0}

        df_source = pd.read_csv(self.source_path)
        self.lake_manager.write_full(df_source)

        last_processed_id = int(df_source["id"].max()) if not df_source.empty else 0
        write_metadata(self.metadata_path, {"last_processed_id": last_processed_id})

        log(
            f"FULL load processou {len(df_source)} linhas. "
            f"last_processed_id={last_processed_id}"
        )
        return {"rows_processed": len(df_source), "last_processed_id": last_processed_id}

    @timed_step("INCREMENTAL LOAD")
    def run_incremental_load(self) -> dict:
        """Processa apenas IDs maiores que o último já processado."""
        if not self.source_path.exists():
            log("Source não encontrado. Incremental load ignorado.")
            return {"rows_processed": 0, "last_processed_id": 0}

        metadata = read_metadata(self.metadata_path)
        last_processed_id = int(metadata.get("last_processed_id", 0))

        df_source = pd.read_csv(self.source_path)
        df_new = df_source[df_source["id"] > last_processed_id]

        self.lake_manager.append_incremental(df_new)

        if not df_new.empty:
            new_last_processed_id = int(df_new["id"].max())
            write_metadata(
                self.metadata_path, {"last_processed_id": new_last_processed_id}
            )
        else:
            new_last_processed_id = last_processed_id

        log(
            f"Incremental processou {len(df_new)} linhas novas. "
            f"last_processed_id={new_last_processed_id}"
        )
        return {
            "rows_processed": len(df_new),
            "last_processed_id": new_last_processed_id,
        }
