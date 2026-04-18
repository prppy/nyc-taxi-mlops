"""
Smoke tests for the Flask backend API.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "source", "devops", "backend"))

from app import app


def test_health_endpoint():
    """GET / should return 200 with status message."""
    client = app.test_client()
    response = client.get("/")
    assert response.status_code == 200
    assert response.get_json() == {"message": "Backend is running"}


def test_health_endpoint_returns_json():
    """GET / should return JSON content type."""
    client = app.test_client()
    response = client.get("/")
    assert response.content_type == "application/json"
