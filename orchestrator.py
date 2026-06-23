"""
Orquestrador Principal da Pipeline
Executa as camadas em ordem: Bronze → Silver → Gold → Quality Check
"""

import logging
import os
import sys
from datetime import datetime

from monitoring.monitoring import PipelineHealthCheck, RunLogger, pipeline_span
from pipeline.bronze.ingest_batch import run_batch_ingestion
from pipeline.gold.build_analytics import run_gold_pipeline
from pipeline.silver.transform import run_silver_pipeline
from scripts.data_quality import run_quality_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("orchestrator")


def run_full_pipeline(skip_streaming: bool = True):
    """
    Executa o pipeline completo na ordem correta.

    Ordem de execução:
    1. Health Check pré-execução
    2. Bronze Batch Ingestion
    3. Silver Transformation
    4. Gold Analytics Build
    5. Data Quality Validation
    6. Health Check pós-execução
    """
    run_logger = RunLogger()
    overall_status = "SUCCESS"
    start_ts = datetime.utcnow()

    logger.info("=" * 60)
    logger.info("  PIPELINE ALFABETIZAÇÃO BRASIL - INÍCIO")
    logger.info(f"  Run ID: {run_logger.run_id}")
    logger.info(f"  Timestamp: {start_ts.isoformat()}")
    logger.info("=" * 60)

    # -----------------------------------------------------------------------
    # 0. Health Check Pré-execução
    # -----------------------------------------------------------------------
    with pipeline_span("health_check_pre"):
        hc_pre = PipelineHealthCheck()
        hc_result = hc_pre.check_gcs_connectivity()
        if not hc_result:
            logger.error("GCS indisponível. Abortando pipeline.")
            run_logger.log_event("health_check", "gcs", "FAILED")
            run_logger.finalize("ABORTED")
            sys.exit(1)
        run_logger.log_event("health_check_pre", "gcs", "OK")

    # -----------------------------------------------------------------------
    # 1. Bronze – Ingestão Batch
    # -----------------------------------------------------------------------
    with pipeline_span("bronze_ingestion"):
        try:
            results, errors = run_batch_ingestion()
            status = "SUCCESS" if not errors else "PARTIAL"
            run_logger.log_event(
                "bronze", "batch",
                status,
                {"ingested": len(results), "errors": errors},
            )
            if errors:
                logger.warning(f"Bronze parcial: entidades com erro: {errors}")
        except Exception as exc:
            logger.error(f"Bronze falhou criticamente: {exc}")
            run_logger.log_event("bronze", "batch", "FAILED", {"error": str(exc)})
            overall_status = "FAILED"
            run_logger.finalize(overall_status)
            sys.exit(1)

    # -----------------------------------------------------------------------
    # 2. Silver – Transformação
    # -----------------------------------------------------------------------
    with pipeline_span("silver_transformation"):
        try:
            silver_errors = run_silver_pipeline()
            status = "SUCCESS" if not silver_errors else "PARTIAL"
            run_logger.log_event(
                "silver", "transform",
                status,
                {"errors": silver_errors},
            )
            if silver_errors:
                overall_status = "PARTIAL"
        except Exception as exc:
            logger.error(f"Silver falhou: {exc}")
            run_logger.log_event("silver", "transform", "FAILED", {"error": str(exc)})
            overall_status = "FAILED"

    # -----------------------------------------------------------------------
    # 3. Gold – Analytics
    # -----------------------------------------------------------------------
    if overall_status != "FAILED":
        with pipeline_span("gold_analytics"):
            try:
                gold_errors = run_gold_pipeline()
                status = "SUCCESS" if not gold_errors else "PARTIAL"
                run_logger.log_event(
                    "gold", "analytics",
                    status,
                    {"errors": gold_errors},
                )
                if gold_errors:
                    overall_status = "PARTIAL"
            except Exception as exc:
                logger.error(f"Gold falhou: {exc}")
                run_logger.log_event("gold", "analytics", "FAILED", {"error": str(exc)})
                overall_status = "PARTIAL"

    # -----------------------------------------------------------------------
    # 4. Qualidade de Dados
    # -----------------------------------------------------------------------
    with pipeline_span("data_quality"):
        try:
            quality_summary = run_quality_pipeline()
            qstatus = "SUCCESS" if quality_summary["failed"] == 0 else "WARNINGS"
            run_logger.log_event(
                "quality", "validation",
                qstatus,
                quality_summary,
            )
            logger.info(
                f"Quality: {quality_summary['passed']}/{quality_summary['total']} "
                f"checks passaram ({quality_summary['pass_rate']}%)"
            )
        except Exception as exc:
            logger.warning(f"Quality check falhou: {exc}")
            run_logger.log_event("quality", "validation", "FAILED", {"error": str(exc)})

    # -----------------------------------------------------------------------
    # 5. Health Check Pós-execução
    # -----------------------------------------------------------------------
    with pipeline_span("health_check_post"):
        hc_post = PipelineHealthCheck()
        hc_post_result = hc_post.run_all()
        run_logger.log_event(
            "health_check_post", "all",
            hc_post_result["overall"],
            hc_post_result["checks"],
        )

    # -----------------------------------------------------------------------
    # Finalização
    # -----------------------------------------------------------------------
    end_ts = datetime.utcnow()
    duration = (end_ts - start_ts).seconds

    logger.info("=" * 60)
    logger.info(f"  PIPELINE CONCLUÍDO: {overall_status}")
    logger.info(f"  Duração total: {duration}s")
    logger.info("=" * 60)

    report = run_logger.finalize(overall_status)
    return report, overall_status


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Orquestrador da Pipeline de Alfabetização")
    parser.add_argument(
        "--stage",
        choices=["full", "bronze", "silver", "gold", "quality"],
        default="full",
        help="Estágio a executar (default: full)",
    )
    args = parser.parse_args()

    if args.stage == "full":
        report, status = run_full_pipeline()
        sys.exit(0 if status in ("SUCCESS", "PARTIAL") else 1)
    elif args.stage == "bronze":
        run_batch_ingestion()
    elif args.stage == "silver":
        run_silver_pipeline()
    elif args.stage == "gold":
        run_gold_pipeline()
    elif args.stage == "quality":
        run_quality_pipeline()
