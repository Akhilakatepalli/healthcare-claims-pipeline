import random
import uuid
from datetime import date, datetime, timedelta
from faker import Faker
from google.cloud import bigquery

fake = Faker()

PROJECT_ID = "mongodb-gke-project"
DATASET = "healthcare_claims"
TABLE = "finalized_claims"

STATES = [
    "CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI",
    "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI"
]

PAYER_TYPES = ["MEDICARE", "MEDICAID", "COMMERCIAL"]

PAYERS = {
    "MEDICARE": ["Medicare Federal", "Medicare Advantage"],
    "MEDICAID": ["Medicaid CA", "Medicaid TX", "Medicaid FL", "Medicaid NY"],
    "COMMERCIAL": ["BCBS", "Aetna", "UnitedHealth", "Cigna", "Humana"]
}

DIAGNOSIS_CODES = [
    "E11.9", "I10", "J44.1", "N18.3", "F32.1",
    "M54.5", "K21.0", "Z79.899", "E78.5", "I25.10"
]

PROCEDURE_CODES = [
    "99213", "99214", "99232", "71046", "93000",
    "80053", "85025", "36415", "99285", "27447"
]

STATUSES = ["APPROVED", "DENIED", "FRAUD_BLOCKED"]
STATUS_WEIGHTS = [0.75, 0.18, 0.07]

DENIAL_REASONS = {
    "DENIED": ["Not medically necessary", "Prior authorization required",
               "Coverage terminated", "Duplicate claim", "Out of network"],
    "FRAUD_BLOCKED": ["Impossible billing pattern", "Duplicate claim detected",
                      "Provider NPI on exclusion list", "Upcoding detected"],
    "APPROVED": [""]
}


def random_date(start_year=2024, end_year=2025):
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))


def generate_claim():
    payer_type = random.choice(PAYER_TYPES)
    payer_name = random.choice(PAYERS[payer_type])
    status = random.choices(STATUSES, weights=STATUS_WEIGHTS)[0]
    denial_reason = random.choice(DENIAL_REASONS[status])
    billed = round(random.uniform(50, 15000), 2)
    adjudicated = round(billed * random.uniform(0.6, 0.95), 2) if status == "APPROVED" else 0.0
    service_date = random_date()
    created_at = datetime.combine(service_date, datetime.min.time()) + timedelta(hours=random.randint(1, 48))
    processed_at = created_at + timedelta(hours=random.randint(1, 72))

    return {
        "claim_id": str(uuid.uuid4()),
        "member_id": f"MBR{random.randint(100000, 999999)}",
        "member_name": fake.name(),
        "member_dob": str(fake.date_of_birth(minimum_age=18, maximum_age=90)),
        "member_state": random.choice(STATES),
        "provider_npi": f"{random.randint(1000000000, 9999999999)}",
        "provider_name": f"Dr. {fake.last_name()} {random.choice(['MD', 'DO', 'NP'])}",
        "service_date": str(service_date),
        "diagnosis_codes": ",".join(random.sample(DIAGNOSIS_CODES, k=random.randint(1, 3))),
        "procedure_codes": ",".join(random.sample(PROCEDURE_CODES, k=random.randint(1, 2))),
        "billed_amount": billed,
        "adjudicated_amount": adjudicated,
        "status": status,
        "denial_reason": denial_reason,
        "payer_type": payer_type,
        "payer_name": payer_name,
        "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "processed_at": processed_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
    }


def main():
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    print("Generating 10,000 claims records...")
    records = [generate_claim() for _ in range(10000)]

    print(f"Loading into BigQuery table: {table_ref}")
    errors = client.insert_rows_json(table_ref, records)

    if errors:
        print(f"Errors: {errors}")
    else:
        print("Done! 10,000 records loaded successfully.")


if __name__ == "__main__":
    main()
