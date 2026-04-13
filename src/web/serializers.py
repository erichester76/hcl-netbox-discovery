"""Shared JSON serializers for job API responses."""

from __future__ import annotations

from typing import Any


def jobs_payload(jobs: list[dict[str, Any]]) -> dict[str, Any]:
    return {"jobs": jobs, "count": len(jobs)}


def job_artifact_payload(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "job_id": job["id"],
        "status": job["status"],
        "artifact": job.get("artifact"),
    }


def job_logs_payload(job: dict[str, Any], logs: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "status": job["status"],
        "logs": logs,
    }
