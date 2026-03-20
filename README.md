# data-engineering-incremental-101

Simulacao pratica de um pipeline de dados com **carga FULL** vs **carga INCREMENTAL**, usando apenas arquivos locais (mini data lake).

O objetivo e mostrar, com codigo simples e didatico, quando vale a pena reprocessar tudo e quando vale processar somente novos registros.

## Objetivo do projeto

Demonstrar na pratica a diferenca entre:

- **FULL load**: le toda a origem e sobrescreve o lake.
- **INCREMENTAL load**: processa apenas registros novos (id > last_processed_id).

No final da execucao, o projeto imprime os tempos para comparacao.

## Estrutura

```text
project/
├── data/
│   ├── source/        # origem simulada (source.csv)
│   ├── lake/          # mini data lake local (csv ou parquet)
│   └── metadata/      # controle incremental (metadata.json)
├── src/
│   ├── data_generator.py
│   ├── lake_manager.py
│   ├── pipeline.py
│   └── utils.py
├── main.py
└── requirements.txt
```

## Como funciona

### 1) Geracao de dados (`DataGenerator`)

Gera lotes fake de vendas com os campos:

- `id` (inteiro incremental)
- `customer_name`
- `product`
- `value`
- `created_at`

Cada execucao faz append em `project/data/source/source.csv` e mantem o incremento do ID.

### 2) Lake local (`LakeManager`)

Responsavel por:

- `write_full(df)`: sobrescreve o lake.
- `append_incremental(df)`: adiciona apenas novos registros.
- `read_lake()`: le o estado atual do lake.

### 3) Metadata incremental

Arquivo: `project/data/metadata/metadata.json`

Exemplo:

```json
{
  "last_processed_id": 0
}
```

Esse valor controla ate qual ID ja foi processado.

### 4) Pipeline (`Pipeline`)

- `run_full_load()`
  - Le toda a source.
  - Sobrescreve o lake.
  - Atualiza metadata com o maior ID.
  - Mede tempo.

- `run_incremental_load()`
  - Le metadata.
  - Filtra apenas novos registros (`id > last_processed_id`).
  - Faz append no lake.
  - Atualiza metadata.
  - Mede tempo.

## Requisitos

- Python 3.10+
- Dependencias:
  - `pandas`
  - `faker`
  - `pyarrow` (necessario para parquet)

Instalacao:

```bash
cd project
python -m pip install -r requirements.txt
```

## Como executar

No diretorio `project`:

```bash
python main.py
```

Fluxo executado no `main.py`:

1. Gera 100.000 linhas iniciais
2. Roda FULL load
3. Gera +10.000 linhas
4. Roda FULL load novamente
5. Roda INCREMENTAL load
6. Imprime comparativo de tempos

## Exemplo de saida esperada

```text
FULL LOAD (1a execucao): 0.37 segundos
FULL LOAD (2a execucao): 0.45 segundos
INCREMENTAL LOAD: 0.19 segundos
```

Observacao: os tempos variam conforme maquina e formato de armazenamento.

## Alternar formato do lake (csv/parquet)

No `project/main.py`, altere:

```python
file_format = "csv"      # ou "parquet"
```

- `csv`: geralmente melhor para demonstrar append incremental simples.
- `parquet`: formato colunar comum em data lakes reais.

## Boas praticas demonstradas

- Separacao de responsabilidades por classe/modulo.
- Controle de estado incremental via metadata.
- Pipeline reprodutivel e facil de testar localmente.
- Comparacao objetiva por tempo de execucao.
