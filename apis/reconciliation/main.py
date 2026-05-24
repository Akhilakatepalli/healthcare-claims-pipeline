from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
import os

app = FastAPI(title="Provider Reconciliation API")

MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://akhilaakhikatepalli_db_user:u6kUhOFq5CGYpW7J@cluster0.vkglb7m.mongodb.net/")
client = MongoClient(MONGO_URI)
col = client["healthcare_claims"]["finalized_claims"]


@app.get("/health")
def health():
    return {"status": "ok", "service": "reconciliation"}


@app.get("/providers/{npi}/reconciliation")
def get_reconciliation(npi: str):
    pipeline = [
        {"$match": {"provider_npi": npi}},
        {"$group": {
            "_id": "$status",
            "count":              {"$sum": 1},
            "total_billed":       {"$sum": "$billed_amount"},
            "total_adjudicated":  {"$sum": "$adjudicated_amount"},
        }},
    ]
    results = list(col.aggregate(pipeline))

    if not results:
        raise HTTPException(status_code=404, detail=f"Provider NPI {npi} not found")

    provider = col.find_one({"provider_npi": npi}, {"_id": 0, "provider_name": 1})

    summary = {r["_id"]: {
        "count":             r["count"],
        "total_billed":      round(r["total_billed"], 2),
        "total_adjudicated": round(r["total_adjudicated"], 2),
    } for r in results}

    total_billed      = sum(r["total_billed"] for r in results)
    total_adjudicated = sum(r["total_adjudicated"] for r in results)

    return {
        "provider_npi":       npi,
        "provider_name":      provider.get("provider_name") if provider else None,
        "summary_by_status":  summary,
        "total_billed":       round(total_billed, 2),
        "total_adjudicated":  round(total_adjudicated, 2),
        "total_claims":       sum(r["count"] for r in results),
    }
