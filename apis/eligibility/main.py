from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
import os

app = FastAPI(title="Member Eligibility API")

MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://akhilaakhikatepalli_db_user:u6kUhOFq5CGYpW7J@cluster0.vkglb7m.mongodb.net/")
client = MongoClient(MONGO_URI)
col = client["healthcare_claims"]["finalized_claims"]


@app.get("/health")
def health():
    return {"status": "ok", "service": "eligibility"}


@app.get("/eligibility/{member_id}")
def get_eligibility(member_id: str):
    claims = list(col.find(
        {"member_id": member_id},
        {"_id": 0, "claim_id": 1, "member_name": 1, "member_state": 1,
         "payer_type": 1, "payer_name": 1, "status": 1, "service_date": 1}
    ).sort("service_date", -1).limit(10))

    if not claims:
        raise HTTPException(status_code=404, detail=f"Member {member_id} not found")

    latest = claims[0]
    return {
        "member_id":    member_id,
        "member_name":  latest.get("member_name"),
        "member_state": latest.get("member_state"),
        "payer_type":   latest.get("payer_type"),
        "payer_name":   latest.get("payer_name"),
        "eligible":     latest.get("status") == "APPROVED",
        "recent_claims": claims,
    }
