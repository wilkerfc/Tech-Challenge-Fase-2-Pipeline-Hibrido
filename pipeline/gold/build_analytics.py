"""
Gold Layer - Camada Analítica
Cria datasets prontos para dashboards, análises estatísticas e ML.
"""

import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from google.cloud import bigquery, storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("gold.analytics")

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "tc-alfabetizacao")
BUCKET_NAME = os.getenv("GCS_BUCKET", "tc-alfabetizacao-datalake")
BQ_DATASET_GOLD = os.getenv("BQ_GOLD_DATASET", "gold_alfabetizacao")
RUN_DATE = datetime.utcnow().strftime("%Y-%m-%d")

SILVER_PREFIX = "silver"
GOLD_PREFIX = "gold"


# ---------------------------------------------------------------------------
# Leitura Silver
# ---------------------------------------------------------------------------

def read_silver(gcs_client: storage.Client, entity: str) -> pd.DataFrame:
    bucket = gcs_client.bucket(BUCKET_NAME)
    prefix = f"{SILVER_PREFIX}/{entity}/processed_date={RUN_DATE}/"
    blobs = list(bucket.list_blobs(prefix=prefix))
    if not blobs:
        raise FileNotFoundError(f"Silver não encontrado: {prefix}")
    local_path = Path(f"/tmp/silver_{entity}.parquet")
    blobs[0].download_to_filename(str(local_path))
    df = pd.read_parquet(local_path)
    logger.info(f"[silver/{entity}] {len(df)} registros")
    return df


# ---------------------------------------------------------------------------
# Dataset 1: Indicador de Alfabetização por Município (enriquecido)
# ---------------------------------------------------------------------------

def build_indicador_municipio(
    indicador: pd.DataFrame,
    municipios: pd.DataFrame,
    ufs: pd.DataFrame,
    meta_municipio: pd.DataFrame,
    meta_brasil: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("Construindo Gold: indicador_municipio...")

    df = (
        indicador
        .merge(municipios[["id_municipio", "nome", "sigla_uf", "id_uf"]], on="id_municipio", how="left")
        .merge(ufs[["id_uf", "nome"]].rename(columns={"nome": "nome_uf"}), on="id_uf", how="left")
        .merge(meta_municipio[["id_municipio", "ano", "meta"]].rename(columns={"meta": "meta_municipio"}),
               on=["id_municipio", "ano"], how="left")
        .merge(meta_brasil[["ano", "meta"]].rename(columns={"meta": "meta_nacional"}),
               on="ano", how="left")
    )

    df["gap_vs_meta_municipio"] = df["indicador_alfabetizacao"] - df["meta_municipio"]
    df["gap_vs_meta_nacional"] = df["indicador_alfabetizacao"] - df["meta_nacional"]
    df["status_meta_municipio"] = df["gap_vs_meta_municipio"].apply(
        lambda x: "ATINGIDA" if pd.notna(x) and x >= 0 else "NAO_ATINGIDA"
    )

    df["_gold_timestamp"] = datetime.utcnow().isoformat()
    df["_gold_date"] = RUN_DATE

    logger.info(f"Gold indicador_municipio: {len(df)} registros")
    return df


# ---------------------------------------------------------------------------
# Dataset 2: Evolução Temporal por UF
# ---------------------------------------------------------------------------

def build_evolucao_uf(
    indicador: pd.DataFrame,
    municipios: pd.DataFrame,
    ufs: pd.DataFrame,
    meta_uf: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("Construindo Gold: evolucao_uf...")

    df = indicador.merge(
        municipios[["id_municipio", "id_uf", "sigla_uf"]], on="id_municipio", how="left"
    )

    uf_agg = (
        df.groupby(["id_uf", "sigla_uf", "ano"])
        .agg(
            indicador_medio=("indicador_alfabetizacao", "mean"),
            indicador_min=("indicador_alfabetizacao", "min"),
            indicador_max=("indicador_alfabetizacao", "max"),
            total_municipios=("id_municipio", "nunique"),
            matriculas_total=("quantidade_matriculas", "sum"),
            municipios_meta_atingida=("meta_atingida", "sum"),
        )
        .reset_index()
    )

    uf_agg = uf_agg.merge(
        ufs[["id_uf", "nome"]].rename(columns={"nome": "nome_uf"}), on="id_uf", how="left"
    ).merge(
        meta_uf[["id_uf", "ano", "meta"]].rename(columns={"meta": "meta_uf"}),
        on=["id_uf", "ano"], how="left"
    )

    uf_agg["pct_municipios_meta_atingida"] = (
        uf_agg["municipios_meta_atingida"] / uf_agg["total_municipios"] * 100
    ).round(2)

    # Variação ano a ano
    uf_agg = uf_agg.sort_values(["id_uf", "ano"])
    uf_agg["variacao_yoy"] = uf_agg.groupby("id_uf")["indicador_medio"].diff().round(2)

    uf_agg["_gold_timestamp"] = datetime.utcnow().isoformat()
    logger.info(f"Gold evolucao_uf: {len(uf_agg)} registros")
    return uf_agg


# ---------------------------------------------------------------------------
# Dataset 3: Painel Nacional Consolidado
# ---------------------------------------------------------------------------

def build_painel_nacional(
    indicador: pd.DataFrame,
    meta_brasil: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("Construindo Gold: painel_nacional...")

    nacional = (
        indicador.groupby("ano")
        .agg(
            indicador_medio_nacional=("indicador_alfabetizacao", "mean"),
            total_municipios=("id_municipio", "nunique"),
            municipios_meta_atingida=("meta_atingida", "sum"),
            total_matriculas=("quantidade_matriculas", "sum"),
        )
        .reset_index()
    )

    nacional = nacional.merge(
        meta_brasil[["ano", "meta"]].rename(columns={"meta": "meta_nacional"}),
        on="ano", how="left"
    )

    nacional["pct_municipios_alfabetizados"] = (
        nacional["municipios_meta_atingida"] / nacional["total_municipios"] * 100
    ).round(2)

    nacional["gap_meta"] = (
        nacional["indicador_medio_nacional"] - nacional["meta_nacional"]
    ).round(2)

    nacional["_gold_timestamp"] = datetime.utcnow().isoformat()
    logger.info(f"Gold painel_nacional: {len(nacional)} registros")
    return nacional


# ---------------------------------------------------------------------------
# Dataset 4: Features para ML (Flat Table)
# ---------------------------------------------------------------------------

def build_ml_features(
    indicador_municipio_gold: pd.DataFrame,
) -> pd.DataFrame:
    """
    Cria feature store para modelos de ML.
    Cada linha = município + ano com features para predição.
    """
    logger.info("Construindo Gold: ml_features...")

    df = indicador_municipio_gold.copy()

    # Lag features (último ano disponível como referência)
    df = df.sort_values(["id_municipio", "ano"])
    df["indicador_lag1"] = df.groupby("id_municipio")["indicador_alfabetizacao"].shift(1)
    df["indicador_lag2"] = df.groupby("id_municipio")["indicador_alfabetizacao"].shift(2)
    df["tendencia"] = df["indicador_alfabetizacao"] - df["indicador_lag1"]

    # Seleciona features relevantes
    feature_cols = [
        "id_municipio", "nome", "sigla_uf", "ano",
        "indicador_alfabetizacao", "indicador_lag1", "indicador_lag2",
        "tendencia", "meta_municipio", "meta_nacional",
        "gap_vs_meta_municipio", "gap_vs_meta_nacional",
        "quantidade_matriculas", "meta_atingida",
    ]
    available = [c for c in feature_cols if c in df.columns]
    df = df[available].dropna(subset=["indicador_alfabetizacao"])

    df["_gold_timestamp"] = datetime.utcnow().isoformat()
    logger.info(f"Gold ml_features: {len(df)} registros")
    return df


# ---------------------------------------------------------------------------
# Salvamento: GCS + BigQuery
# ---------------------------------------------------------------------------

def save_gold_gcs(
    df: pd.DataFrame,
    gcs_client: storage.Client,
    table_name: str,
):
    bucket = gcs_client.bucket(BUCKET_NAME)
    blob_path = f"{GOLD_PREFIX}/{table_name}/gold_date={RUN_DATE}/{table_name}.parquet"
    local_path = Path(f"/tmp/gold_{table_name}.parquet")
    df.to_parquet(local_path, index=False, engine="pyarrow")
    bucket.blob(blob_path).upload_from_filename(str(local_path))
    gcs_uri = f"gs://{BUCKET_NAME}/{blob_path}"
    logger.info(f"Gold GCS: {gcs_uri}")
    return gcs_uri


def save_gold_bigquery(
    df: pd.DataFrame,
    bq_client: bigquery.Client,
    table_name: str,
):
    """Carrega Gold no BigQuery para consumo por BI / ML."""
    table_ref = f"{PROJECT_ID}.{BQ_DATASET_GOLD}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    local_path = Path(f"/tmp/gold_{table_name}.parquet")
    df.to_parquet(local_path, index=False, engine="pyarrow")
    with open(local_path, "rb") as f:
        job = bq_client.load_table_from_file(f, table_ref, job_config=job_config)
    job.result()
    logger.info(f"Gold BigQuery: {table_ref} ({len(df)} registros)")


# ---------------------------------------------------------------------------
# Orquestração Gold
# ---------------------------------------------------------------------------

def run_gold_pipeline():
    logger.info("=== Iniciando Gold Pipeline ===")
    gcs_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)
    errors = []

    # Carrega Silver
    try:
        indicador = read_silver(gcs_client, "indicador_alfabetizacao")
        municipios = read_silver(gcs_client, "municipios")
        ufs = read_silver(gcs_client, "ufs")
        meta_brasil = read_silver(gcs_client, "meta_brasil")
        meta_uf = read_silver(gcs_client, "meta_uf")
        meta_municipio = read_silver(gcs_client, "meta_municipio")
    except Exception as e:
        logger.error(f"Falha ao carregar Silver: {e}")
        return

    # Constrói e salva cada dataset Gold
    gold_tables = [
        ("indicador_municipio", lambda: build_indicador_municipio(
            indicador, municipios, ufs, meta_municipio, meta_brasil
        )),
        ("evolucao_uf", lambda: build_evolucao_uf(
            indicador, municipios, ufs, meta_uf
        )),
        ("painel_nacional", lambda: build_painel_nacional(
            indicador, meta_brasil
        )),
    ]

    gold_dfs = {}
    for table_name, builder in gold_tables:
        try:
            df = builder()
            save_gold_gcs(df, gcs_client, table_name)
            save_gold_bigquery(df, bq_client, table_name)
            gold_dfs[table_name] = df
        except Exception as e:
            logger.error(f"Gold '{table_name}': {e}")
            errors.append(table_name)

    # ML features depende do indicador_municipio
    if "indicador_municipio" in gold_dfs:
        try:
            ml_df = build_ml_features(gold_dfs["indicador_municipio"])
            save_gold_gcs(ml_df, gcs_client, "ml_features")
            save_gold_bigquery(ml_df, bq_client, "ml_features")
        except Exception as e:
            logger.error(f"Gold 'ml_features': {e}")
            errors.append("ml_features")

    logger.info(f"=== Gold concluído. Erros: {errors} ===")
    return errors


if __name__ == "__main__":
    run_gold_pipeline()
