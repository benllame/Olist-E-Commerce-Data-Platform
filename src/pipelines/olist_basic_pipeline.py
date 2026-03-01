"""
Pipeline básico Apache Beam para datos de Olist
Lee desde BigQuery, aplica transformaciones, escribe resultados

Uso:
    # DirectRunner (local) - requiere --temp_location para ReadFromBigQuery
    python olist_basic_pipeline.py \
        --project $GCP_PROJECT_ID \
        --runner DirectRunner \
        --temp_location gs://$GCP_PROJECT_ID-dataflow-temp/temp/

    # DataflowRunner (cloud)
    python olist_basic_pipeline.py \
        --project $GCP_PROJECT_ID \
        --runner DataflowRunner \
        --temp_location gs://$GCP_PROJECT_ID-dataflow-temp/temp/ \
        --staging_location gs://$GCP_PROJECT_ID-dataflow-temp/staging/ \
        --region southamerica-west1

Prerequisitos:
    - Bucket GCS: gs://$GCP_PROJECT_ID-dataflow-temp/ (para archivos temporales)
    - Dataset BQ: olist_raw (con tablas orders, order_payments)
    - Dataset BQ: olist_analytics (destino, debe existir previamente)
    - Permisos: bigquery.tables.create en olist_analytics
      NOTA: Si el dataset fue creado con Terraform y tiene bloques access {}
      explícitos, projectOwners debe estar incluido o se bloquea el acceso.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import logging

# Configurar logging
logging.getLogger().setLevel(logging.INFO)


class ParseOrder(beam.DoFn):
    """
    DoFn = "Do Function" - procesa cada elemento

    En Beam, los DoFn son la forma de aplicar transformaciones custom
    """

    def process(self, element):
        """
        Process se llama para cada elemento del PCollection

        Args:
            element: Un diccionario con los datos de la orden (row de BigQuery)

        Yields:
            Diccionario transformado
        """
        try:
            # element es un dict con las columnas de BigQuery
            order = {
                "order_id": element["order_id"],
                "customer_id": element["customer_id"],
                "order_status": element["order_status"],
                "order_date": element["order_purchase_timestamp"],
            }

            # Agregar campo calculado
            order["year"] = element["order_purchase_timestamp"].year
            order["month"] = element["order_purchase_timestamp"].month

            yield order

        except Exception as e:
            logging.error(f"Error parsing order: {e}")


class EnrichWithPayment(beam.DoFn):
    """
    Enriquece órdenes con información de pago

    Usa Side Input: un patrón en Beam para joins con datos pequeños
    """

    def process(self, element, payments_dict):
        """
        Args:
            element: orden
            payments_dict: diccionario de pagos (side input)
        """
        order_id = element["order_id"]

        # Lookup en diccionario de pagos
        if order_id in payments_dict:
            payment = payments_dict[order_id]
            element["payment_type"] = payment.get("payment_type", "unknown")
            element["payment_value"] = payment.get("payment_value", 0.0)
        else:
            element["payment_type"] = "no_payment"
            element["payment_value"] = 0.0

        yield element


class ComputeMonthlyMetrics(beam.CombineFn):
    """
    CombineFn = función de agregación personalizada

    Similar a SQL GROUP BY + aggregate functions
    Útil para cálculos complejos que no son simplemente SUM/AVG
    """

    def create_accumulator(self):
        """Inicializa acumulador vacío"""
        return {
            "total_orders": 0,
            "total_revenue": 0.0,
            "payment_types": {},
            "order_statuses": {},
        }

    def add_input(self, accumulator, element):
        """Agrega un elemento al acumulador"""
        accumulator["total_orders"] += 1
        accumulator["total_revenue"] += element.get("payment_value", 0.0)

        # Contar payment types
        payment_type = element.get("payment_type", "unknown")
        accumulator["payment_types"][payment_type] = (
            accumulator["payment_types"].get(payment_type, 0) + 1
        )

        # Contar order statuses
        status = element.get("order_status", "unknown")
        accumulator["order_statuses"][status] = (
            accumulator["order_statuses"].get(status, 0) + 1
        )

        return accumulator

    def merge_accumulators(self, accumulators):
        """
        Combina múltiples acumuladores
        (necesario para procesamiento distribuido)
        """
        merged = self.create_accumulator()

        for acc in accumulators:
            merged["total_orders"] += acc["total_orders"]
            merged["total_revenue"] += acc["total_revenue"]

            # Merge payment types
            for ptype, count in acc["payment_types"].items():
                merged["payment_types"][ptype] = (
                    merged["payment_types"].get(ptype, 0) + count
                )

            # Merge order statuses
            for status, count in acc["order_statuses"].items():
                merged["order_statuses"][status] = (
                    merged["order_statuses"].get(status, 0) + count
                )

        return merged

    def extract_output(self, accumulator):
        """Convierte acumulador a output final"""
        return {
            "total_orders": accumulator["total_orders"],
            "total_revenue": accumulator["total_revenue"],
            "avg_order_value": (
                accumulator["total_revenue"] / accumulator["total_orders"]
                if accumulator["total_orders"] > 0
                else 0
            ),
            "payment_types": accumulator["payment_types"],
            "order_statuses": accumulator["order_statuses"],
        }


def run_pipeline(project_id, runner="DirectRunner", pipeline_args=None):
    """
    Función principal del pipeline

    Args:
        project_id: GCP project ID
        runner: 'DirectRunner' (local) o 'DataflowRunner' (cloud)
        pipeline_args: Additional pipeline arguments
    """

    # Configurar opciones del pipeline
    if pipeline_args:
        options = PipelineOptions(pipeline_args)
    else:
        options = PipelineOptions(
            runner=runner,
            project=project_id,
            temp_location=f"gs://{project_id}-dataflow-temp/temp",
            staging_location=f"gs://{project_id}-dataflow-temp/staging",
            region="southamerica-west1",
        )

    # FIX: temp_location es OBLIGATORIO para ReadFromBigQuery, incluso con DirectRunner.
    # ReadFromBigQuery exporta datos a GCS temporalmente antes de procesarlos.
    # Sin esto se obtiene:
    #   ValueError: ReadFromBigQuery requires a GCS location to be provided.
    # Aquí se configura un fallback si no se pasó --temp_location en CLI.
    gcp_options = options.view_as(beam.options.pipeline_options.GoogleCloudOptions)
    if not gcp_options.temp_location:
        gcp_options.temp_location = f"gs://{project_id}-dataflow-temp/temp"
    if not gcp_options.region:
        gcp_options.region = "southamerica-west1"

    print("=" * 80)
    print("🚀 OLIST BASIC PIPELINE")
    print("=" * 80)
    print(f"Project: {project_id}")
    print(f"Runner: {runner}")
    print("=" * 80)
    print()

    with beam.Pipeline(options=options) as pipeline:

        # ==========================================
        # STEP 1: Leer órdenes desde BigQuery
        # ==========================================
        orders_query = f"""
        SELECT
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp
        FROM `{project_id}.olist_raw.orders`
        WHERE order_status = 'delivered'
        LIMIT 10000
        """

        orders = (
            pipeline
            | "Read Orders from BigQuery"
            >> beam.io.ReadFromBigQuery(query=orders_query, use_standard_sql=True)
            | "Parse Orders" >> beam.ParDo(ParseOrder())
        )

        # ==========================================
        # STEP 2: Leer pagos desde BigQuery
        # ==========================================
        payments_query = f"""
        SELECT
            order_id,
            payment_type,
            payment_value
        FROM `{project_id}.olist_raw.order_payments`
        """

        payments = (
            pipeline
            | "Read Payments from BigQuery"
            >> beam.io.ReadFromBigQuery(query=payments_query, use_standard_sql=True)
            | "Key Payments by Order" >> beam.Map(lambda p: (p["order_id"], p))
        )

        # Convertir a diccionario (Side Input)
        payments_dict = beam.pvalue.AsDict(payments)

        # ==========================================
        # STEP 3: Enriquecer órdenes con pagos
        # ==========================================
        enriched_orders = orders | "Enrich with Payment" >> beam.ParDo(
            EnrichWithPayment(), payments_dict=payments_dict
        )

        # ==========================================
        # STEP 4: Agregaciones por mes
        # ==========================================
        monthly_metrics = (
            enriched_orders
            | "Key by Year-Month"
            >> beam.Map(lambda order: (f"{order['year']}-{order['month']:02d}", order))
            | "Compute Metrics" >> beam.CombinePerKey(ComputeMonthlyMetrics())
            | "Format Output" >> beam.Map(lambda kv: {"year_month": kv[0], **kv[1]})
        )

        # ==========================================
        # STEP 5: Escribir resultados
        # ==========================================

        # Output 1: BigQuery
        # FIX: Filtrar solo campos planos antes de escribir a BigQuery.
        # ComputeMonthlyMetrics produce campos anidados (payment_types, order_statuses)
        # que son dicts Python. BigQuery rechaza campos que no están en el schema:
        #   "JSON table encountered too many errors, giving up."
        # Solución: extraer solo los 4 campos definidos en el schema.
        bq_output = monthly_metrics | "Filter BQ Fields" >> beam.Map(
            lambda row: {
                "year_month": row["year_month"],
                "total_orders": row["total_orders"],
                "total_revenue": row["total_revenue"],
                "avg_order_value": row["avg_order_value"],
            }
        )

        bq_output | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            table=f"{project_id}:olist_analytics.monthly_metrics",
            schema="year_month:STRING, total_orders:INTEGER, "
            "total_revenue:FLOAT, avg_order_value:FLOAT",
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        )

        # Output 2: JSON completo (incluye payment_types y order_statuses para debugging)
        monthly_metrics | "Write to JSON" >> beam.io.WriteToText(
            "output/monthly_metrics",
            file_name_suffix=".json",
            shard_name_template="",  # Single file
        )

    print()
    print("=" * 80)
    print("✅ PIPELINE COMPLETADO")
    print("=" * 80)
    print()
    print("Verificar resultados:")
    print(f"  BigQuery: {project_id}.olist_analytics.monthly_metrics")
    print("  Local: output/monthly_metrics.json")
    print()


def main():
    """Función principal"""

    parser = argparse.ArgumentParser(description="Olist Basic Pipeline")
    parser.add_argument("--project", type=str, required=True, help="GCP Project ID")
    parser.add_argument(
        "--runner",
        type=str,
        default="DirectRunner",
        choices=["DirectRunner", "DataflowRunner"],
        help="Pipeline runner",
    )

    # parse_known_args separa argumentos conocidos de los desconocidos
    # Los desconocidos se pasan a PipelineOptions
    args, pipeline_args = parser.parse_known_args()

    # Agregar runner y project a pipeline_args
    pipeline_args.extend([f"--runner={args.runner}", f"--project={args.project}"])

    run_pipeline(args.project, args.runner, pipeline_args)


if __name__ == "__main__":
    main()
