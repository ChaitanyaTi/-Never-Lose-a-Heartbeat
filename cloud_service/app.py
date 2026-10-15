"""
Cloud Service Web Application
FastAPI application for receiving streaming biosignal samples, triggering real-time window processing,
and servicing remote queries and cloud exports.
"""

import os
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from cloud_service.cloud_store import CloudStore

app = FastAPI(title="MayaCardio Cloud Ingestion & Analytics Service", version="1.0.0")

cloud_store = CloudStore()


class SampleItem(BaseModel):
    sensor: int
    index: int
    values: List[int]


class LossItem(BaseModel):
    sensor: int
    from_index: int
    to_index: Optional[int] = None
    reason: str = "unknown"


class IngestPayload(BaseModel):
    session_id: str
    samples: List[SampleItem] = []
    losses: List[LossItem] = []


@app.get("/health")
def health_check():
    return {"status": "healthy"}


@app.post("/api/v1/ingest")
def ingest_data(payload: IngestPayload):
    samples_data = [s.model_dump() for s in payload.samples]
    losses_data = [l.model_dump() for l in payload.losses]
    result = cloud_store.ingest_batch(payload.session_id, samples_data, losses_data)
    return {"status": "ok", **result}


@app.get("/api/v1/status")
def get_session_status(session_id: str = Query(...)):
    cnt = cloud_store.get_sample_count(session_id)
    return {"session_id": session_id, "samples_in_cloud": cnt}


@app.get("/api/v1/export")
def export_cloud_data(session_id: str = Query(...)):
    def line_generator():
        for line in cloud_store.export_samples_stream(session_id):
            yield line + "\n"
        for line in cloud_store.export_losses_stream(session_id):
            yield line + "\n"

    return StreamingResponse(line_generator(), media_type="application/x-ndjson")


@app.get("/api/v1/results")
def export_cloud_results(session_id: str = Query(...)):
    def results_generator():
        for line in cloud_store.get_results_stream(session_id):
            yield line + "\n"

    return StreamingResponse(results_generator(), media_type="application/x-ndjson")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("cloud_service.app:app", host="0.0.0.0", port=port, log_level="info")
