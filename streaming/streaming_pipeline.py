"""
Streaming Pipeline - Simulação de Ingestão em Tempo Quase Real
Usa Google Cloud Pub/Sub + Cloud Functions (ou Dataflow) para processar
eventos de atualização de indicadores educacionais.

Este módulo contém:
1. Producer  - simula publicação de eventos no Pub/Sub
2. Consumer  - processa mensagens e grava na camada Bronze/Silver em tempo real
"""

import json
import logging
import os
import random
import time
from datetime import datetime

from google.cloud import pubsub_v1, storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("streaming")

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "tc-alfabetizacao")
TOPIC_ID = os.getenv("PUBSUB_TOPIC", "alfabetizacao-events")
SUBSCRIPTION_ID = os.getenv("PUBSUB_SUBSCRIPTION", "alfabetizacao-events-sub")
BUCKET_NAME = os.getenv("GCS_BUCKET", "tc-alfabetizacao-datalake")

EVENT_TYPES = [
    "INDICADOR_ATUALIZADO",
    "META_REVISADA",
    "NOVA_MEDICAO",
]

# IDs de municípios simulados (em produção viria do Silver)
SAMPLE_MUNICIPIOS = [
    "3550308",  # São Paulo
    "3304557",  # Rio de Janeiro
    "3106200",  # Belo Horizonte
    "4106902",  # Curitiba
    "2927408",  # Salvador
]


# ---------------------------------------------------------------------------
# PRODUCER – Publica eventos no Pub/Sub
# ---------------------------------------------------------------------------

class AlfabetizacaoEventProducer:
    """Simula a publicação de eventos de atualização de indicadores."""

    def __init__(self):
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(PROJECT_ID, TOPIC_ID)

    def _build_event(self, event_type: str) -> dict:
        municipio_id = random.choice(SAMPLE_MUNICIPIOS)
        return {
            "event_id": f"evt-{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}",
            "event_type": event_type,
            "timestamp": datetime.utcnow().isoformat(),
            "payload": {
                "id_municipio": municipio_id,
                "ano": 2024,
                "indicador_alfabetizacao": round(random.uniform(30.0, 95.0), 2),
                "quantidade_matriculas": random.randint(100, 5000),
                "fonte": "SAEB_STREAMING",
            },
        }

    def publish(self, event_type: str) -> str:
        event = self._build_event(event_type)
        data = json.dumps(event, ensure_ascii=False).encode("utf-8")
        future = self.publisher.publish(
            self.topic_path,
            data=data,
            event_type=event_type,
            source="streaming_producer",
        )
        message_id = future.result()
        logger.info(f"Publicado [{event_type}] → message_id={message_id}")
        return message_id

    def simulate_stream(self, n_events: int = 20, interval_sec: float = 1.0):
        """Simula um fluxo contínuo de eventos para teste."""
        logger.info(f"Iniciando simulação: {n_events} eventos com intervalo {interval_sec}s")
        for i in range(n_events):
            event_type = random.choice(EVENT_TYPES)
            self.publish(event_type)
            time.sleep(interval_sec)
        logger.info("Simulação concluída.")


# ---------------------------------------------------------------------------
# CONSUMER – Processa mensagens do Pub/Sub
# ---------------------------------------------------------------------------

class AlfabetizacaoEventConsumer:
    """
    Consome eventos do Pub/Sub e persiste no data lake.
    Em produção, este lógica seria empacotada em uma Cloud Function
    ou job Dataflow com trigger em Pub/Sub.
    """

    def __init__(self):
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(
            PROJECT_ID, SUBSCRIPTION_ID
        )
        self.gcs = storage.Client(project=PROJECT_ID)
        self.bucket = self.gcs.bucket(BUCKET_NAME)
        self._buffer: list[dict] = []
        self._buffer_size = 50  # flush a cada N mensagens
        self._flush_interval = 60  # ou a cada 60s
        self._last_flush = time.time()

    def _parse_message(self, message: pubsub_v1.types.ReceivedMessage) -> dict:
        data = json.loads(message.message.data.decode("utf-8"))
        data["_pubsub_message_id"] = message.message.message_id
        data["_received_at"] = datetime.utcnow().isoformat()
        return data

    def _validate_event(self, event: dict) -> bool:
        """Validações básicas de qualidade no stream."""
        payload = event.get("payload", {})
        if not payload.get("id_municipio"):
            logger.warning("Evento sem id_municipio descartado.")
            return False
        ind = payload.get("indicador_alfabetizacao")
        if ind is not None and not (0 <= ind <= 100):
            logger.warning(f"Indicador fora do range [0,100]: {ind}")
            return False
        return True

    def _enrich_event(self, event: dict) -> dict:
        """Enriquece evento com metadados de processamento."""
        event["_layer"] = "bronze_streaming"
        event["_processing_date"] = datetime.utcnow().strftime("%Y-%m-%d")
        event["_pipeline_version"] = "1.0.0"
        return event

    def _flush_buffer(self):
        if not self._buffer:
            return
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        blob_path = (
            f"bronze/streaming/date={datetime.utcnow().strftime('%Y-%m-%d')}/"
            f"events_{timestamp}.json"
        )
        content = "\n".join(json.dumps(e, ensure_ascii=False) for e in self._buffer)
        self.bucket.blob(blob_path).upload_from_string(
            content, content_type="application/json"
        )
        logger.info(f"Flush: {len(self._buffer)} eventos → gs://{BUCKET_NAME}/{blob_path}")
        self._buffer.clear()
        self._last_flush = time.time()

    def _process_message(self, message: pubsub_v1.types.ReceivedMessage):
        try:
            event = self._parse_message(message)
            if self._validate_event(event):
                event = self._enrich_event(event)
                self._buffer.append(event)
            message.ack()

            # Flush por tamanho ou tempo
            if (
                len(self._buffer) >= self._buffer_size
                or time.time() - self._last_flush > self._flush_interval
            ):
                self._flush_buffer()

        except Exception as e:
            logger.error(f"Erro ao processar mensagem: {e}")
            message.nack()

    def start(self, timeout_sec: float = 300.0):
        """Inicia o consumo de mensagens por timeout_sec segundos."""
        logger.info(f"Consumidor iniciado. Aguardando mensagens por {timeout_sec}s...")
        streaming_pull_future = self.subscriber.subscribe(
            self.subscription_path,
            callback=self._process_message,
        )
        try:
            streaming_pull_future.result(timeout=timeout_sec)
        except Exception:
            streaming_pull_future.cancel()
            self._flush_buffer()  # flush final
        logger.info("Consumidor encerrado.")


# ---------------------------------------------------------------------------
# Cloud Function entrypoint (HTTP ou Pub/Sub trigger)
# ---------------------------------------------------------------------------

def pubsub_trigger(event: dict, context):
    """
    Entrypoint para Google Cloud Function com trigger Pub/Sub.
    Processa um único evento por invocação.
    """
    import base64

    raw_data = base64.b64decode(event["data"]).decode("utf-8")
    payload = json.loads(raw_data)

    logger.info(f"Cloud Function processando: {payload.get('event_type')}")

    # Validação mínima
    if not payload.get("payload", {}).get("id_municipio"):
        logger.warning("Evento inválido recebido na Cloud Function.")
        return

    # Persiste evento individual no GCS (micro-batch real-time)
    gcs = storage.Client()
    bucket = gcs.bucket(BUCKET_NAME)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    blob_path = f"bronze/streaming/date={datetime.utcnow().strftime('%Y-%m-%d')}/event_{ts}.json"
    bucket.blob(blob_path).upload_from_string(
        json.dumps(payload, ensure_ascii=False),
        content_type="application/json",
    )
    logger.info(f"Evento persistido: gs://{BUCKET_NAME}/{blob_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    mode = sys.argv[1] if len(sys.argv) > 1 else "simulate"

    if mode == "simulate":
        producer = AlfabetizacaoEventProducer()
        producer.simulate_stream(n_events=30, interval_sec=0.5)
    elif mode == "consume":
        consumer = AlfabetizacaoEventConsumer()
        consumer.start(timeout_sec=300)
    else:
        print(f"Modo desconhecido: {mode}. Use 'simulate' ou 'consume'.")
