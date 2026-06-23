"""
Scripts de Validação e Qualidade de Dados
Executa checks nas camadas Bronze, Silver e Gold.
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

import pandas as pd
from google.cloud import storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("data_quality")

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "tc-alfabetizacao")
BUCKET_NAME = os.getenv("GCS_BUCKET", "tc-alfabetizacao-datalake")
RUN_DATE = datetime.utcnow().strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Framework de checks
# ---------------------------------------------------------------------------

@dataclass
class CheckResult:
    check_name: str
    entity: str
    layer: str
    passed: bool
    details: str
    row_count: int = 0
    failed_count: int = 0
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())


def check(name: str):
    """Decorator para registrar checks de qualidade."""
    def decorator(fn: Callable) -> Callable:
        fn._check_name = name
        return fn
    return decorator


class DataQualityRunner:
    def __init__(self):
        self.results: list[CheckResult] = []

    def run(
        self,
        df: pd.DataFrame,
        entity: str,
        layer: str,
        checks: list[Callable],
    ):
        for chk in checks:
            try:
                result = chk(df, entity, layer)
                self.results.append(result)
                status = "✅ PASS" if result.passed else "❌ FAIL"
                logger.info(f"[{layer}/{entity}] {status} {result.check_name}: {result.details}")
            except Exception as e:
                self.results.append(CheckResult(
                    check_name=getattr(chk, "_check_name", chk.__name__),
                    entity=entity,
                    layer=layer,
                    passed=False,
                    details=f"EXCEPTION: {e}",
                ))
                logger.error(f"[{layer}/{entity}] EXCEPTION em {chk.__name__}: {e}")

    def summary(self) -> dict:
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed = total - passed
        return {
            "total": total,
            "passed": passed,
            "failed": failed,
            "pass_rate": round(passed / total * 100, 1) if total else 0,
            "failed_checks": [r.check_name for r in self.results if not r.passed],
        }

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame([vars(r) for r in self.results])

    def save_report(self, gcs_client: storage.Client):
        df = self.to_dataframe()
        bucket = gcs_client.bucket(BUCKET_NAME)
        blob_path = f"monitoring/quality_reports/date={RUN_DATE}/quality_report.parquet"
        local = "/tmp/quality_report.parquet"
        df.to_parquet(local, index=False)
        bucket.blob(blob_path).upload_from_filename(local)
        logger.info(f"Relatório de qualidade salvo: gs://{BUCKET_NAME}/{blob_path}")


# ---------------------------------------------------------------------------
# Checks genéricos reutilizáveis
# ---------------------------------------------------------------------------

@check("sem_duplicatas")
def check_no_duplicates(df: pd.DataFrame, entity: str, layer: str, key_cols: list = None):
    subset = key_cols or df.columns.tolist()
    subset = [c for c in subset if c in df.columns]
    n_dup = df.duplicated(subset=subset).sum()
    return CheckResult(
        check_name="sem_duplicatas",
        entity=entity,
        layer=layer,
        passed=n_dup == 0,
        details=f"{n_dup} duplicatas encontradas (key={subset})",
        row_count=len(df),
        failed_count=int(n_dup),
    )


@check("sem_nulos_criticos")
def check_no_nulls_critical(df: pd.DataFrame, entity: str, layer: str, critical_cols: list = None):
    cols = critical_cols or df.columns.tolist()
    cols = [c for c in cols if c in df.columns]
    null_counts = {c: int(df[c].isna().sum()) for c in cols if df[c].isna().any()}
    passed = len(null_counts) == 0
    return CheckResult(
        check_name="sem_nulos_criticos",
        entity=entity,
        layer=layer,
        passed=passed,
        details=f"Nulos críticos: {null_counts}" if null_counts else "Nenhum nulo crítico",
        row_count=len(df),
        failed_count=sum(null_counts.values()),
    )


@check("volume_minimo")
def check_minimum_volume(
    df: pd.DataFrame, entity: str, layer: str, min_rows: int = 100
):
    passed = len(df) >= min_rows
    return CheckResult(
        check_name="volume_minimo",
        entity=entity,
        layer=layer,
        passed=passed,
        details=f"{len(df)} registros (mínimo: {min_rows})",
        row_count=len(df),
    )


@check("range_indicador")
def check_indicador_range(df: pd.DataFrame, entity: str, layer: str):
    col = "indicador_alfabetizacao"
    if col not in df.columns:
        return CheckResult(
            check_name="range_indicador",
            entity=entity,
            layer=layer,
            passed=True,
            details="Coluna não presente - skip",
        )
    out_of_range = df[col].dropna()
    out_of_range = out_of_range[(out_of_range < 0) | (out_of_range > 100)]
    passed = len(out_of_range) == 0
    return CheckResult(
        check_name="range_indicador",
        entity=entity,
        layer=layer,
        passed=passed,
        details=f"{len(out_of_range)} valores fora de [0, 100]",
        row_count=len(df),
        failed_count=len(out_of_range),
    )


@check("anos_validos")
def check_valid_years(df: pd.DataFrame, entity: str, layer: str):
    col = "ano"
    if col not in df.columns:
        return CheckResult(
            check_name="anos_validos",
            entity=entity,
            layer=layer,
            passed=True,
            details="Coluna 'ano' não presente - skip",
        )
    invalid = df[col].dropna()
    invalid = invalid[(invalid < 2000) | (invalid > 2030)]
    passed = len(invalid) == 0
    return CheckResult(
        check_name="anos_validos",
        entity=entity,
        layer=layer,
        passed=passed,
        details=f"{len(invalid)} anos fora do intervalo [2000, 2030]",
        row_count=len(df),
        failed_count=len(invalid),
    )


@check("consistencia_gold_vs_silver")
def check_gold_silver_consistency(
    gold_df: pd.DataFrame,
    silver_df: pd.DataFrame,
    entity: str,
    tolerance: float = 0.05,
) -> CheckResult:
    """Verifica se contagem Gold está próxima da Silver (max 5% de perda)."""
    silver_n = len(silver_df)
    gold_n = len(gold_df)
    loss = (silver_n - gold_n) / silver_n if silver_n else 0
    passed = loss <= tolerance
    return CheckResult(
        check_name="consistencia_gold_vs_silver",
        entity=entity,
        layer="gold",
        passed=passed,
        details=f"Silver={silver_n} | Gold={gold_n} | Perda={loss:.1%} (tolerância={tolerance:.0%})",
        row_count=gold_n,
        failed_count=max(0, silver_n - gold_n),
    )


# ---------------------------------------------------------------------------
# Runner principal de qualidade
# ---------------------------------------------------------------------------

def load_parquet_from_gcs(
    gcs_client: storage.Client, prefix: str, entity: str
) -> pd.DataFrame:
    bucket = gcs_client.bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs(prefix=prefix))
    if not blobs:
        raise FileNotFoundError(f"Nenhum arquivo em: {prefix}")
    local = f"/tmp/qc_{entity}.parquet"
    blobs[0].download_to_filename(local)
    return pd.read_parquet(local)


def run_quality_pipeline():
    logger.info("=== Iniciando Validação de Qualidade ===")
    gcs_client = storage.Client(project=PROJECT_ID)
    runner = DataQualityRunner()

    entities = {
        "silver": {
            "ufs": {
                "key_cols": ["id_uf"],
                "critical_cols": ["id_uf", "sigla", "nome"],
                "min_rows": 27,
            },
            "municipios": {
                "key_cols": ["id_municipio"],
                "critical_cols": ["id_municipio", "nome", "sigla_uf"],
                "min_rows": 5000,
            },
            "indicador_alfabetizacao": {
                "key_cols": ["id_municipio", "ano"],
                "critical_cols": ["id_municipio", "ano"],
                "min_rows": 1000,
            },
        },
        "gold": {
            "indicador_municipio": {
                "key_cols": ["id_municipio", "ano"],
                "critical_cols": ["id_municipio", "ano", "indicador_alfabetizacao"],
                "min_rows": 1000,
            },
            "painel_nacional": {
                "key_cols": ["ano"],
                "critical_cols": ["ano", "indicador_medio_nacional"],
                "min_rows": 3,
            },
        },
    }

    for layer, layer_entities in entities.items():
        for entity, config in layer_entities.items():
            prefix_map = {
                "silver": f"silver/{entity}/processed_date={RUN_DATE}/",
                "gold": f"gold/{entity}/gold_date={RUN_DATE}/",
            }
            try:
                df = load_parquet_from_gcs(gcs_client, prefix_map[layer], entity)
            except FileNotFoundError as e:
                logger.error(e)
                continue

            # Checks comuns
            runner.run(
                df, entity, layer,
                [
                    lambda df, e, l: check_no_duplicates(df, e, l, config["key_cols"]),
                    lambda df, e, l: check_no_nulls_critical(df, e, l, config["critical_cols"]),
                    lambda df, e, l: check_minimum_volume(df, e, l, config["min_rows"]),
                    check_indicador_range,
                    check_valid_years,
                ],
            )

    summary = runner.summary()
    logger.info(f"=== Resumo de Qualidade: {summary} ===")

    runner.save_report(gcs_client)
    return summary


if __name__ == "__main__":
    result = run_quality_pipeline()
    if result["failed"] > 0:
        exit(1)
