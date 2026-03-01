"""
olist_streaming_pipeline.py

Pipeline Apache Beam para procesar eventos de Olist en tiempo real
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows, SlidingWindows
import json
import logging
from datetime import datetime

logging.getLogger().setLevel(logging.INFO)

# ==========================================
# CONFIGURACIÓN
# ==========================================

PROJECT_ID = "ecommerce-olist-ben-260301"
TOPIC = f"projects/{PROJECT_ID}/topics/olist-order-events"
SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/olist-order-events-sub"
DATASET = "olist_analytics"

# BigQuery schemas (required for streaming inserts)
REALTIME_ORDERS_SCHEMA = {
    "fields": [
        {"name": "event_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "event_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "customer_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "order_status", "type": "STRING", "mode": "NULLABLE"},
        {"name": "payment_value", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "event_timestamp", "type": "STRING", "mode": "NULLABLE"},
        {"name": "processing_timestamp", "type": "STRING", "mode": "NULLABLE"},
        {"name": "value_category", "type": "STRING", "mode": "NULLABLE"},
        {"name": "time_of_day", "type": "STRING", "mode": "NULLABLE"},
        {"name": "metadata_source", "type": "STRING", "mode": "NULLABLE"},
        {"name": "metadata_version", "type": "STRING", "mode": "NULLABLE"},
    ]
}

REALTIME_METRICS_SCHEMA = {
    "fields": [
        {"name": "event_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "window_start", "type": "STRING", "mode": "NULLABLE"},
        {"name": "window_end", "type": "STRING", "mode": "NULLABLE"},
        {"name": "processing_timestamp", "type": "STRING", "mode": "NULLABLE"},
        {"name": "event_count", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "total_revenue", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "avg_order_value", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "unique_customers", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "unique_orders", "type": "INTEGER", "mode": "NULLABLE"},
    ]
}

# ==========================================
# TRANSFORMS
# ==========================================


class ParsePubSubMessage(beam.DoFn):
    """Parse JSON message from Pub/Sub"""

    def process(self, element):
        try:
            # Decodificar mensaje
            message = element.decode("utf-8") if isinstance(element, bytes) else element

            # Parse JSON
            data = json.loads(message)

            # Agregar processing timestamp
            data["processing_timestamp"] = datetime.utcnow().isoformat() + "Z"

            yield data

        except Exception as e:
            logging.error(f"Error parsing message: {e}")
            # No yield = mensaje descartado


class EnrichEvent(beam.DoFn):
    """Enriquece evento con información adicional"""

    def process(self, element):
        try:
            # Agregar campos calculados
            enriched = element.copy()

            # Flatten metadata dict to avoid nested structure errors in BQ
            metadata = enriched.pop("metadata", {})
            if isinstance(metadata, dict):
                enriched["metadata_source"] = metadata.get("source", "")
                enriched["metadata_version"] = metadata.get("version", "")

            # Categorizar por monto
            value = enriched.get("payment_value", 0)
            if value >= 200:
                enriched["value_category"] = "high"
            elif value >= 100:
                enriched["value_category"] = "medium"
            else:
                enriched["value_category"] = "low"

            # Hora del día
            timestamp = datetime.fromisoformat(
                enriched["event_timestamp"].replace("Z", "+00:00")
            )
            hour = timestamp.hour
            if 6 <= hour < 12:
                enriched["time_of_day"] = "morning"
            elif 12 <= hour < 18:
                enriched["time_of_day"] = "afternoon"
            elif 18 <= hour < 24:
                enriched["time_of_day"] = "evening"
            else:
                enriched["time_of_day"] = "night"

            yield enriched

        except Exception as e:
            logging.error(f"Error enriching event: {e}")


class ComputeMetrics(beam.CombineFn):
    """Agrega métricas de eventos en ventana"""

    def create_accumulator(self):
        return {
            "event_count": 0,
            "total_revenue": 0.0,
            "customer_ids": set(),
            "order_ids": set(),
        }

    def add_input(self, accumulator, element):
        accumulator["event_count"] += 1
        accumulator["total_revenue"] += element.get("payment_value", 0.0)
        accumulator["customer_ids"].add(element.get("customer_id"))
        accumulator["order_ids"].add(element.get("order_id"))
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            merged["event_count"] += acc["event_count"]
            merged["total_revenue"] += acc["total_revenue"]
            merged["customer_ids"].update(acc["customer_ids"])
            merged["order_ids"].update(acc["order_ids"])
        return merged

    def extract_output(self, accumulator):
        count = accumulator["event_count"]
        revenue = accumulator["total_revenue"]

        return {
            "event_count": count,
            "total_revenue": round(revenue, 2),
            "avg_order_value": round(revenue / count, 2) if count > 0 else 0,
            "unique_customers": len(accumulator["customer_ids"]),
            "unique_orders": len(accumulator["order_ids"]),
        }


class AddWindowTimestamp(beam.DoFn):
    """Agrega timestamps de ventana al resultado"""

    def process(self, element, window=beam.DoFn.WindowParam):
        yield {
            "window_start": window.start.to_utc_datetime().isoformat() + "Z",
            "window_end": window.end.to_utc_datetime().isoformat() + "Z",
            "processing_timestamp": datetime.utcnow().isoformat() + "Z",
            **element[1],  # Métricas
        }


# ==========================================
# PIPELINE
# ==========================================


def run_streaming_pipeline(options):
    """
    Pipeline principal de streaming
    """

    print("=" * 80)
    print("STREAMING PIPELINE")
    print("=" * 80)
    print(f"Project: {PROJECT_ID}")
    print(f"Topic: {TOPIC}")
    print(f"Dataset: {DATASET}")
    print("=" * 80)

    with beam.Pipeline(options=options) as pipeline:

        # ==========================================
        # STEP 1: Read from Pub/Sub
        # ==========================================

        raw_events = (
            pipeline
            | "Read Pub/Sub" >> beam.io.ReadFromPubSub(topic=TOPIC)
            | "Parse JSON" >> beam.ParDo(ParsePubSubMessage())
        )

        # ==========================================
        # STEP 2: Enrich Events
        # ==========================================

        enriched_events = raw_events | "Enrich Events" >> beam.ParDo(EnrichEvent())

        # ==========================================
        # STEP 3: Write Individual Events
        # ==========================================

        enriched_events | "Write Events to BQ" >> beam.io.WriteToBigQuery(
            table=f"{PROJECT_ID}:{DATASET}.realtime_orders",
            schema=REALTIME_ORDERS_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

        # ==========================================
        # STEP 4: Windowing - 1 minute windows
        # ==========================================

        windowed_events = enriched_events | "Window 1min" >> beam.WindowInto(
            FixedWindows(60)
        )

        # ==========================================
        # STEP 5: Aggregate by Event Type
        # ==========================================

        metrics_by_type = (
            windowed_events
            | "Key by Type" >> beam.Map(lambda e: (e["event_type"], e))
            | "Compute Metrics" >> beam.CombinePerKey(ComputeMetrics())
            | "Add Window Info" >> beam.ParDo(AddWindowTimestamp())
            | "Add Event Type"
            >> beam.Map(
                lambda x: {
                    "event_type": x[0] if isinstance(x, tuple) else "unknown",
                    **(x[1] if isinstance(x, tuple) else x),
                }
            )
        )

        # ==========================================
        # STEP 6: Write Aggregated Metrics
        # ==========================================

        metrics_by_type | "Write Metrics to BQ" >> beam.io.WriteToBigQuery(
            table=f"{PROJECT_ID}:{DATASET}.realtime_metrics",
            schema=REALTIME_METRICS_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

        # ==========================================
        # STEP 7: Sliding Window (últimos 5 minutos)
        # ==========================================

        sliding_metrics = (
            enriched_events
            | "Sliding Window 5min"
            >> beam.WindowInto(SlidingWindows(300, 60))  # 5min window, 1min slide
            | "Key All" >> beam.Map(lambda e: ("all", e))
            | "Compute Rolling Metrics" >> beam.CombinePerKey(ComputeMetrics())
            | "Add Sliding Window Info" >> beam.ParDo(AddWindowTimestamp())
        )

        # Log para debugging (solo en DirectRunner)
        if options.view_as(StandardOptions).runner == "DirectRunner":
            (
                sliding_metrics
                | "Log Rolling Metrics"
                >> beam.Map(lambda x: logging.info(f"Rolling metrics: {x}"))
            )


# ==========================================
# MAIN
# ==========================================


def main():
    """Función principal"""

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project", type=str, default=PROJECT_ID, help="GCP Project ID"
    )
    parser.add_argument(
        "--runner",
        type=str,
        default="DirectRunner",
        choices=["DirectRunner", "DataflowRunner"],
        help="Pipeline runner",
    )
    parser.add_argument(
        "--region", type=str, default="us-central1", help="GCP region for Dataflow"
    )

    args, beam_args = parser.parse_known_args()

    # Configurar opciones
    options = PipelineOptions(
        beam_args,
        streaming=True,
        project=args.project,
        runner=args.runner,
        region=args.region,
        temp_location=f"gs://{args.project}-dataflow-temp/temp",
        staging_location=f"gs://{args.project}-dataflow-temp/staging",
        save_main_session=True,
    )

    # Ejecutar pipeline
    run_streaming_pipeline(options)


if __name__ == "__main__":
    main()
