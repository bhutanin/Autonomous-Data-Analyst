"""Application configuration settings."""
import os
from dataclasses import dataclass
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    """Application settings loaded from environment variables."""

    # Google Cloud
    project_id: str
    default_dataset: str | None

    # Query limits
    max_bytes_billed: int

    # Gemini configuration
    gemini_model: str
    vertex_ai_location: str

    # Profiling defaults
    sample_size_for_large_tables: int = 100000
    large_table_threshold: int = 1000000

    # Relationship detection
    relationship_confidence_threshold: float = 0.5


@lru_cache
def get_settings() -> Settings:
    """Get cached application settings."""
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        # Try to get from gcloud config
        try:
            import subprocess
            result = subprocess.run(
                ["gcloud", "config", "get-value", "project"],
                capture_output=True,
                text=True,
                timeout=5
            )
            project_id = result.stdout.strip()
        except Exception:
            pass

    if not project_id:
        raise ValueError(
            "GOOGLE_CLOUD_PROJECT environment variable is not set. "
            "Please set it in .env or run 'gcloud config set project PROJECT_ID'"
        )

    return Settings(
        project_id=project_id,
        default_dataset=os.getenv("DEFAULT_DATASET"),
        max_bytes_billed=int(os.getenv("MAX_BYTES_BILLED", "1000000000")),
        gemini_model=os.getenv("GEMINI_MODEL", "gemini-2.0-flash"),
        vertex_ai_location=os.getenv("VERTEX_AI_LOCATION", "us-central1"),
    )
