from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
import os

app = FastAPI(title="Claim Status API")

MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://akhilaakhikatepalli_db_user:u6kUhOFq5CGYpW7J@cluster0.vkglb7m.mongodb.net/")
client = MongoClient(MONGO_URI)
col = client["healthcare_claims"]["finalized_claims"]


@app.get("/health")
def health():
    return {"status": "ok", "service": "claims"}


@app.get("/claims/{claim_id}/status")
def get_claim_status(claim_id: str):
    claim = col.find_one({"claim_id": claim_id}, {"_id": 0})

    if not claim:
        raise HTTPException(status_code=404, detail=f"Claim {claim_id} not found")

    return {
        "claim_id":           claim.get("claim_id"),
        "member_id":          claim.get("member_id"),
        "provider_name":      claim.get("provider_name"),
        "service_date":       claim.get("service_date"),
        "diagnosis_codes":    claim.get("diagnosis_codes"),
        "procedure_codes":    claim.get("procedure_codes"),
        "billed_amount":      claim.get("billed_amount"),
        "adjudicated_amount": claim.get("adjudicated_amount"),
        "status":             claim.get("status"),
        "denial_reason":      claim.get("denial_reason"),
        "payer_name":         claim.get("payer_name"),
        "processed_at":       claim.get("processed_at"),
    }
