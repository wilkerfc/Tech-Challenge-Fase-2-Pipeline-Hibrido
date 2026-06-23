"""
Silver Layer - Tratamento, Limpeza e Integração de Dados
Transforma dados brutos (Bronze) em dados limpos e integrados.
"""

import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from google.cloud import storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("silver.transform")

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "tc-alfabetizacao")
BUCKET_NAME = os.getenv("GCS_BUCKET", "tc-alfabetizacao-datalake")
RUN_DATE = datetime.utcnow().strftime("%Y-%m-%d")

BRONZE_PREFIX = "bronze"
SILVER_PREFIX = "silver"


# ---------------------------------------------------------------------------
# Leitura da camada Bronze
# ---------------------------------------------------------------------------

def read_bronze(gcs_client: storage.Client, entity: str) -> pd.DataFrame:
    """Lê o arquivo Parquet mais recente da camada Bronze."""
    bucket = gcs_client.bucket(BUCKET_NAME)
    prefix = f"{BRONZE_PREFIX}/{entity}/ingestion_date={RUN_DATE}/"
    blobs = list(bucket.list_blobs(prefix=prefix))
    if not blobs:
        raise FileNotFoundError(f"Nenhum arquivo encontrado em: {prefix}")
    blob = blobs[0]
    local_path = Path(f"/tmp/bronze_{entity}.parquet")
    blob.download_to_filename(str(local_path))
    df = pd.read_parquet(local_path)
    logger.info(f"[{entity}] Bronze carregado: {len(df)} registros")
    return df


# ---------------------------------------------------------------------------
# Transformações comuns
# ---------------------------------------------------------------------------

def remove_duplicates(df: pd.DataFrame, subset: list) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates(subset=subset)
    removed = before - len(df)
    if removed:
        logger.warning(f"Duplicatas removidas: {removed}")
    return df


def fill_missing(df: pd.DataFrame, rules: dict) -> pd.DataFrame:
    """rules: {coluna: valor_padrão}"""
    for col, default in rules.items():
        if col in df.columns:
            nulls = df[col].isna().sum()
            if nulls:
                logger.warning(f"Coluna '{col}': {nulls} valores nulos → preenchido com '{default}'")
            df[col] = df[col].fillna(default)
    return df


def normalize_text_columns(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.upper()
                .str.normalize("NFKD")
                .str.encode("ascii", errors="ignore")
                .str.decode("ascii")
            )
    return df


def cast_types(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """schema: {coluna: dtype}"""
    for col, dtype in schema.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except Exception as e:
                logger.warning(f"Cast falhou para '{col}' → {dtype}: {e}")
    return df


def validate_referential_integrity(
    df: pd.DataFrame,
    ref_df: pd.DataFrame,
    key: str,
    ref_key: str,
    entity_name: str,
):
    """Verifica se todas as chaves existem na tabela de referência."""
    invalid = ~df[key].isin(ref_df[ref_key])
    count = invalid.sum()
    if count:
        logger.warning(
            f"[{entity_name}] {count} registros com '{key}' não encontrado na referência"
        )
    return df[~invalid]


# ---------------------------------------------------------------------------
# Transformações específicas por entidade
# ---------------------------------------------------------------------------

def transform_ufs(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transformando UFs...")
    df = remove_duplicates(df, subset=["id_uf"])
    df = normalize_text_columns(df, ["sigla", "nome"])
    df = cast_types(df, {"id_uf": str})
    df["_silver_timestamp"] = datetime.utcnow().isoformat()
    return df[["id_uf", "sigla", "nome", "_silver_timestamp"]]


def transform_municipios(df: pd.DataFrame, ufs_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transformando Municípios...")
    df = remove_duplicates(df, subset=["id_municipio"])
    df = normalize_text_columns(df, ["nome"])
    df = cast_types(df, {"id_municipio": str, "id_uf": str})
    df = fill_missing(df, {"nome": "NAO INFORMADO"})
    # Validação de integridade referencial
    df = validate_referential_integrity(df, ufs_df, "sigla_uf", "sigla", "municipios")
    df["_silver_timestamp"] = datetime.utcnow().isoformat()
    return df[["id_municipio", "nome", "sigla_uf", "id_uf", "_silver_timestamp"]]


def transform_indicador(df: pd.DataFrame, municipios_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transformando Indicador de Alfabetização...")
    df = remove_duplicates(df, subset=["id_municipio", "ano"])
    df = cast_types(df, {
        "id_municipio": str,
        "ano": int,
        "indicador_alfabetizacao": float,
        "quantidade_matriculas": "Int64",
    })
    df = fill_missing(df, {"indicador_alfabetizacao": None, "quantidade_matriculas": None})
    df = validate_referential_integrity(df, municipios_df, "id_municipio", "id_municipio", "indicador")
    # Criar flag de meta atingida (ponto de corte 743 → indicador >= 50% por convenção do dataset)
    if "indicador_alfabetizacao" in df.columns:
        df["meta_atingida"] = df["indicador_alfabetizacao"] >= 50.0
    df["_silver_timestamp"] = datetime.utcnow().isoformat()
    return df


def transform_meta_brasil(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transformando Meta Brasil...")
    df = remove_duplicates(df, subset=["ano"])
    df = cast_types(df, {"ano": int, "meta": float})
    df["_silver_timestamp"] = datetime.utcnow().isoformat()
    return df


def transform_meta_uf(df: pd.DataFrame, ufs_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transformando Meta por UF...")
    df = remove_duplicates(df, subset=["id_uf", "ano"])
    df = cast_types(df, {"id_uf": str, "ano": int, "meta": float})
    df = validate_referential_integrity(df, ufs_df, "id_uf", "id_uf", "meta_uf")
    df["_silver_timestamp"] = datetime.utcnow().isoformat()
    return df


def transform_meta_municipio(df: pd.DataFrame, municipios_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transformando Meta por Município...")
    df = remove_duplicates(df, subset=["id_municipio", "ano"])
    df = cast_types(df, {"id_municipio": str, "ano": int, "meta": float})
    df = validate_referential_integrity(df, municipios_df, "id_municipio", "id_municipio", "meta_municipio")
    df["_silver_timestamp"] = datetime.utcnow().isoformat()
    return df


# ---------------------------------------------------------------------------
# Salvamento na camada Silver
# ---------------------------------------------------------------------------

def save_silver(
    df: pd.DataFrame,
    gcs_client: storage.Client,
    entity: str,
):
    bucket = gcs_client.bucket(BUCKET_NAME)
    blob_path = f"{SILVER_PREFIX}/{entity}/processed_date={RUN_DATE}/{entity}.parquet"
    local_path = Path(f"/tmp/silver_{entity}.parquet")
    df.to_parquet(local_path, index=False, engine="pyarrow")
    bucket.blob(blob_path).upload_from_filename(str(local_path))
    gcs_uri = f"gs://{BUCKET_NAME}/{blob_path}"
    logger.info(f"Silver salvo: {gcs_uri} ({len(df)} registros)")
    return gcs_uri


# ---------------------------------------------------------------------------
# Orquestração Silver
# ---------------------------------------------------------------------------

def run_silver_pipeline():
    logger.info("=== Iniciando Silver Pipeline ===")
    gcs_client = storage.Client(project=PROJECT_ID)
    errors = []

    try:
        ufs_raw = read_bronze(gcs_client, "ufs")
        ufs = transform_ufs(ufs_raw)
        save_silver(ufs, gcs_client, "ufs")
    except Exception as e:
        logger.error(f"UFs: {e}")
        errors.append("ufs")

    try:
        mun_raw = read_bronze(gcs_client, "municipios")
        municipios = transform_municipios(mun_raw, ufs)
        save_silver(municipios, gcs_client, "municipios")
    except Exception as e:
        logger.error(f"Municipios: {e}")
        errors.append("municipios")

    try:
        ind_raw = read_bronze(gcs_client, "indicador_alfabetizacao")
        indicador = transform_indicador(ind_raw, municipios)
        save_silver(indicador, gcs_client, "indicador_alfabetizacao")
    except Exception as e:
        logger.error(f"Indicador: {e}")
        errors.append("indicador_alfabetizacao")

    try:
        mb_raw = read_bronze(gcs_client, "meta_brasil")
        meta_brasil = transform_meta_brasil(mb_raw)
        save_silver(meta_brasil, gcs_client, "meta_brasil")
    except Exception as e:
        logger.error(f"Meta Brasil: {e}")
        errors.append("meta_brasil")

    try:
        muf_raw = read_bronze(gcs_client, "meta_uf")
        meta_uf = transform_meta_uf(muf_raw, ufs)
        save_silver(meta_uf, gcs_client, "meta_uf")
    except Exception as e:
        logger.error(f"Meta UF: {e}")
        errors.append("meta_uf")

    try:
        mmun_raw = read_bronze(gcs_client, "meta_municipio")
        meta_municipio = transform_meta_municipio(mmun_raw, municipios)
        save_silver(meta_municipio, gcs_client, "meta_municipio")
    except Exception as e:
        logger.error(f"Meta Municipio: {e}")
        errors.append("meta_municipio")

    logger.info(f"=== Silver concluído. Erros: {errors} ===")
    return errors


if __name__ == "__main__":
    run_silver_pipeline()
