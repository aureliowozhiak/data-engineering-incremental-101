from pathlib import Path

import pandas as pd

from src.utils import ensure_directory, log


class LakeManager:
    """Gerencia escrita e leitura do mini data lake local."""

    def __init__(self, lake_path: Path, file_format: str = "parquet"):
        if file_format not in {"parquet", "csv"}:
            raise ValueError("file_format deve ser 'parquet' ou 'csv'.")

        self.file_format = file_format
        self.lake_path = lake_path
        ensure_directory(self.lake_path.parent)

    def write_full(self, df: pd.DataFrame) -> None:
        """Sobrescreve o lake com carga full."""
        self._write(df, mode="full")
        log(f"Lake sobrescrito com {len(df)} linhas.")

    def append_incremental(self, df: pd.DataFrame) -> None:
        """Adiciona apenas os novos registros no lake."""
        if df.empty:
            log("Nenhum registro novo para append incremental.")
            return

        if self.file_format == "parquet":
            if self.lake_path.exists():
                current_df = pd.read_parquet(self.lake_path)
                updated_df = pd.concat([current_df, df], ignore_index=True)
                updated_df.to_parquet(self.lake_path, index=False)
            else:
                df.to_parquet(self.lake_path, index=False)
        else:
            write_mode = "a" if self.lake_path.exists() else "w"
            include_header = not self.lake_path.exists()
            df.to_csv(
                self.lake_path, mode=write_mode, index=False, header=include_header
            )
        log(f"Append incremental concluído com {len(df)} linhas.")

    def read_lake(self) -> pd.DataFrame:
        """Lê dados atuais do lake."""
        if not self.lake_path.exists():
            return pd.DataFrame()
        return (
            pd.read_parquet(self.lake_path)
            if self.file_format == "parquet"
            else pd.read_csv(self.lake_path)
        )

    def _write(self, df: pd.DataFrame, mode: str) -> None:
        if mode != "full":
            raise ValueError("Modo inválido para escrita no lake.")

        if self.file_format == "parquet":
            df.to_parquet(self.lake_path, index=False)
        else:
            df.to_csv(self.lake_path, index=False)
