import time
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
)
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import pymongo

# ── Config ───────────────────────────────────────────────────────────────────
PROJECT_ID     = "mongodb-gke-project"
DATASET        = "healthcare_claims"
TABLE          = "finalized_claims"
MONGO_URI      = "mongodb+srv://akhilaakhikatepalli_db_user:u6kUhOFq5CGYpW7J@cluster0.vkglb7m.mongodb.net/"
MONGO_DB       = "healthcare_claims"
MONGO_COLL     = "finalized_claims"
STAGING_BUCKET = "gs://mongodb-gke-dataflow-staging"
REGION         = "us-central1"

# Only read records not yet loaded — checkpoint via separate log table
BQ_QUERY = f"""
SELECT f.*
FROM `{PROJECT_ID}.{DATASET}.{TABLE}` f
LEFT JOIN `{PROJECT_ID}.{DATASET}.mongodb_load_log` l
  ON f.claim_id = l.claim_id
WHERE l.claim_id IS NULL
"""

# ── Transform ─────────────────────────────────────────────────────────────────
class TransformClaim(beam.DoFn):
    def process(self, row):
        doc = {
            "claim_id":           row.get("claim_id"),
            "member_id":          row.get("member_id"),
            "member_name":        row.get("member_name"),
            "member_dob":         str(row.get("member_dob", "")),
            "member_state":       row.get("member_state"),
            "provider_npi":       row.get("provider_npi"),
            "provider_name":      row.get("provider_name"),
            "service_date":       str(row.get("service_date", "")),
            "diagnosis_codes":    row.get("diagnosis_codes", "").split(","),
            "procedure_codes":    row.get("procedure_codes", "").split(","),
            "billed_amount":      float(row.get("billed_amount", 0)),
            "adjudicated_amount": float(row.get("adjudicated_amount", 0)),
            "status":             row.get("status"),
            "denial_reason":      row.get("denial_reason", ""),
            "payer_type":         row.get("payer_type"),
            "payer_name":         row.get("payer_name"),
            "created_at":         str(row.get("created_at", "")),
            "processed_at":       str(row.get("processed_at", "")),
        }
        yield doc


# ── MongoDB Writer with checkpoint + retry ────────────────────────────────────
class WriteToMongoDB(beam.DoFn):
    def __init__(self, uri, db_name, collection_name, project_id, dataset, table):
        self.uri             = uri
        self.db_name         = db_name
        self.collection_name = collection_name
        self.project_id      = project_id
        self.dataset         = dataset
        self.table           = table
        self.client          = None

    def start_bundle(self):
        self.client = pymongo.MongoClient(self.uri)
        self.buffer = []
        col = self.client[self.db_name][self.collection_name]
        col.create_index("claim_id", unique=True)

    def process(self, doc):
        self.buffer.append(doc)
        if len(self.buffer) >= 500:
            self._flush()

    def finish_bundle(self):
        if self.buffer:
            self._flush()
        if self.client:
            self.client.close()

    def _flush(self):
        from pymongo import UpdateOne
        from google.cloud import bigquery

        col = self.client[self.db_name][self.collection_name]
        operations = [
            UpdateOne({"claim_id": doc["claim_id"]}, {"$set": doc}, upsert=True)
            for doc in self.buffer
        ]

        # Retry up to 3 times on MongoDB failure
        for attempt in range(3):
            try:
                result = col.bulk_write(operations, ordered=False)
                logging.info(
                    f"Upserted {result.upserted_count} new, "
                    f"updated {result.modified_count} existing documents"
                )
                break
            except Exception as e:
                logging.warning(f"MongoDB write attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
                    self.client = pymongo.MongoClient(self.uri)
                    col = self.client[self.db_name][self.collection_name]
                else:
                    raise

        # Checkpoint: insert loaded claim_ids into log table (INSERT avoids streaming buffer lock)
        loaded_ids = [doc["claim_id"] for doc in self.buffer]
        bq_client = bigquery.Client(project=self.project_id)
        log_table = f"{self.project_id}.{self.dataset}.mongodb_load_log"
        rows = [{"claim_id": cid, "loaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())} for cid in loaded_ids]
        bq_client.insert_rows_json(log_table, rows)
        logging.info(f"Checkpointed {len(loaded_ids)} records in BigQuery log table")
        self.buffer = []


# ── Pipeline ──────────────────────────────────────────────────────────────────
def check_pending_records():
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    result = client.query(f"""
        SELECT COUNT(*) as pending
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}` f
        LEFT JOIN `{PROJECT_ID}.{DATASET}.mongodb_load_log` l
          ON f.claim_id = l.claim_id
        WHERE l.claim_id IS NULL
    """).result()
    return list(result)[0]["pending"]


def run():
    pending = check_pending_records()
    if pending == 0:
        logging.info("=" * 60)
        logging.info("NO NEW RECORDS TO LOAD — all claims already in MongoDB.")
        logging.info("=" * 60)
        return
    logging.info(f"Found {pending} records pending load to MongoDB. Starting pipeline...")

    options = PipelineOptions()

    gcp_opts = options.view_as(GoogleCloudOptions)
    gcp_opts.project          = PROJECT_ID
    gcp_opts.region           = REGION
    gcp_opts.staging_location = f"{STAGING_BUCKET}/staging"
    gcp_opts.temp_location    = f"{STAGING_BUCKET}/temp"

    options.view_as(StandardOptions).runner = "DataflowRunner"
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromBigQuery" >> beam.io.ReadFromBigQuery(
                query=BQ_QUERY,
                use_standard_sql=True,
                project=PROJECT_ID,
            )
            | "TransformClaims" >> beam.ParDo(TransformClaim())
            | "WriteToMongoDB"  >> beam.ParDo(
                WriteToMongoDB(
                    MONGO_URI, MONGO_DB, MONGO_COLL,
                    PROJECT_ID, DATASET, TABLE
                )
            )
        )

    logging.info("Pipeline completed successfully.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
