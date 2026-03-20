from pathlib import Path

from src.data_generator import DataGenerator
from src.lake_manager import LakeManager
from src.pipeline import Pipeline
from src.utils import log, read_metadata, write_metadata


def main() -> None:
    # Define caminhos principais do projeto.
    base_path = Path(__file__).resolve().parent
    source_path = base_path / "data" / "source" / "source.csv"
    metadata_path = base_path / "data" / "metadata" / "metadata.json"

    # Troque para "csv" se quiser comparar também esse formato.
    file_format = "csv"
    lake_filename = "data.parquet" if file_format == "parquet" else "data.csv"
    lake_path = base_path / "data" / "lake" / lake_filename

    generator = DataGenerator(source_path=source_path)
    lake_manager = LakeManager(lake_path=lake_path, file_format=file_format)
    pipeline = Pipeline(
        source_path=source_path,
        metadata_path=metadata_path,
        lake_manager=lake_manager,
    )

    # Reinicia metadata para demo reproduzível.
    write_metadata(metadata_path, {"last_processed_id": 0})

    log("=== ETAPA 1: Gerando dados iniciais (100.000 linhas) ===")
    generator.generate_batch(n_rows=100_000)

    log("=== ETAPA 2: Rodando FULL LOAD (1ª execução) ===")
    _, full_time_1 = pipeline.run_full_load()
    metadata_after_first_full = read_metadata(metadata_path)

    log("=== ETAPA 3: Gerando novos dados (+10.000 linhas) ===")
    generator.generate_batch(n_rows=10_000)

    log("=== ETAPA 4: Rodando FULL LOAD (2ª execução) ===")
    _, full_time_2 = pipeline.run_full_load()

    log("=== ETAPA 5: Rodando INCREMENTAL LOAD ===")
    # Restaura snapshot para comparar full vs incremental no mesmo cenário (+10k).
    write_metadata(metadata_path, metadata_after_first_full)
    _, incremental_time = pipeline.run_incremental_load()

    print("\nComparativo de performance:")
    print(f"FULL LOAD (1a execucao): {full_time_1:.2f} segundos")
    print(f"FULL LOAD (2a execucao): {full_time_2:.2f} segundos")
    print(f"INCREMENTAL LOAD: {incremental_time:.2f} segundos")


if __name__ == "__main__":
    main()
