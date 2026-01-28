"""Relationship detection engine."""
from dataclasses import dataclass
from typing import Any

from core.bigquery_client import BigQueryClient, ColumnInfo
from core.exceptions import RelationshipDetectionError
from config.settings import get_settings
from .column_matcher import ColumnMatcher, ColumnMatch


@dataclass
class Relationship:
    """A detected relationship between tables."""

    source_table: str
    source_column: str
    target_table: str
    target_column: str
    relationship_type: str  # 'explicit_fk', 'inferred', 'name_match'
    confidence: float
    evidence: list[str]


class RelationshipDetector:
    """Detects relationships between tables in a dataset."""

    def __init__(self, client: BigQueryClient | None = None):
        """Initialize the detector."""
        self.client = client or BigQueryClient()
        self.settings = get_settings()
        self.matcher = ColumnMatcher(
            confidence_threshold=self.settings.relationship_confidence_threshold
        )

    def detect_relationships(
        self,
        dataset_id: str,
        table_ids: list[str] | None = None,
    ) -> list[Relationship]:
        """
        Detect relationships between tables in a dataset.

        Args:
            dataset_id: The dataset to analyze
            table_ids: Optional list of specific tables (defaults to all)

        Returns:
            List of detected relationships
        """
        try:
            # Get tables to analyze
            if table_ids is None:
                tables = self.client.list_tables(dataset_id)
                table_ids = [t.table for t in tables]

            if not table_ids:
                return []

            # Get schemas for all tables
            schemas = {}
            for table_id in table_ids:
                schemas[table_id] = self.client.get_table_schema(dataset_id, table_id)

            relationships = []

            # Check for explicit foreign keys
            explicit_fks = self._get_explicit_foreign_keys(dataset_id)
            relationships.extend(explicit_fks)

            # Check for inferred relationships
            inferred = self._infer_relationships(dataset_id, table_ids, schemas)
            relationships.extend(inferred)

            # Deduplicate and merge
            relationships = self._merge_relationships(relationships)

            return relationships

        except Exception as e:
            raise RelationshipDetectionError(f"Failed to detect relationships: {str(e)}")

    def _get_explicit_foreign_keys(self, dataset_id: str) -> list[Relationship]:
        """Query INFORMATION_SCHEMA for explicit FK constraints."""
        # Note: BigQuery doesn't have traditional FK constraints, but we check anyway
        # for potential future support or external table links
        try:
            query = f"""
            SELECT
                tc.table_name AS source_table,
                kcu.column_name AS source_column,
                ccu.table_name AS target_table,
                ccu.column_name AS target_column
            FROM `{self.client.project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS` tc
            JOIN `{self.client.project_id}.{dataset_id}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN `{self.client.project_id}.{dataset_id}.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE` ccu
                ON tc.constraint_name = ccu.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            """
            df = self.client.execute_query(query, validate=False)

            relationships = []
            for _, row in df.iterrows():
                relationships.append(
                    Relationship(
                        source_table=row["source_table"],
                        source_column=row["source_column"],
                        target_table=row["target_table"],
                        target_column=row["target_column"],
                        relationship_type="explicit_fk",
                        confidence=1.0,
                        evidence=["INFORMATION_SCHEMA foreign key constraint"],
                    )
                )
            return relationships

        except Exception:
            # BigQuery doesn't support FK constraints, so this will usually fail
            return []

    def _infer_relationships(
        self,
        dataset_id: str,
        table_ids: list[str],
        schemas: dict[str, list[ColumnInfo]],
    ) -> list[Relationship]:
        """Infer relationships based on column names and types."""
        relationships = []

        # Compare each pair of tables
        for i, source_table in enumerate(table_ids):
            source_columns = schemas[source_table]

            for target_table in table_ids[i + 1:]:
                target_columns = schemas[target_table]

                # Find matches in both directions
                matches = self.matcher.find_matches(
                    source_table, source_columns,
                    target_table, target_columns,
                )
                matches.extend(
                    self.matcher.find_matches(
                        target_table, target_columns,
                        source_table, source_columns,
                    )
                )

                for match in matches:
                    relationships.append(
                        Relationship(
                            source_table=match.source_table,
                            source_column=match.source_column,
                            target_table=match.target_table,
                            target_column=match.target_column,
                            relationship_type="inferred",
                            confidence=match.confidence,
                            evidence=[f"Column pattern match: {match.match_type}"],
                        )
                    )

        return relationships

    def _merge_relationships(self, relationships: list[Relationship]) -> list[Relationship]:
        """Merge duplicate relationships and update confidence."""
        # Key by source and target
        merged: dict[tuple, Relationship] = {}

        for rel in relationships:
            key = (rel.source_table, rel.source_column, rel.target_table, rel.target_column)
            reverse_key = (rel.target_table, rel.target_column, rel.source_table, rel.source_column)

            if key in merged:
                # Update existing - keep highest confidence
                existing = merged[key]
                if rel.confidence > existing.confidence:
                    existing.confidence = rel.confidence
                    existing.relationship_type = rel.relationship_type
                existing.evidence.extend(rel.evidence)

            elif reverse_key in merged:
                # Reverse relationship exists
                existing = merged[reverse_key]
                existing.evidence.extend(rel.evidence)

            else:
                merged[key] = Relationship(
                    source_table=rel.source_table,
                    source_column=rel.source_column,
                    target_table=rel.target_table,
                    target_column=rel.target_column,
                    relationship_type=rel.relationship_type,
                    confidence=rel.confidence,
                    evidence=list(rel.evidence),
                )

        return list(merged.values())

    def validate_relationship(
        self,
        dataset_id: str,
        relationship: Relationship,
        sample_size: int = 1000,
    ) -> tuple[bool, float]:
        """
        Validate a relationship by checking referential integrity.

        Args:
            dataset_id: The dataset containing the tables
            relationship: The relationship to validate
            sample_size: Number of rows to sample for validation

        Returns:
            Tuple of (is_valid, match_rate)
        """
        try:
            query = f"""
            WITH source_sample AS (
                SELECT DISTINCT `{relationship.source_column}` AS source_value
                FROM `{self.client.project_id}.{dataset_id}.{relationship.source_table}`
                WHERE `{relationship.source_column}` IS NOT NULL
                LIMIT {sample_size}
            ),
            matches AS (
                SELECT s.source_value
                FROM source_sample s
                INNER JOIN `{self.client.project_id}.{dataset_id}.{relationship.target_table}` t
                    ON s.source_value = t.`{relationship.target_column}`
            )
            SELECT
                (SELECT COUNT(*) FROM source_sample) AS total,
                (SELECT COUNT(*) FROM matches) AS matched
            """

            df = self.client.execute_query(query, validate=False)

            if df.empty:
                return False, 0.0

            total = df.iloc[0]["total"]
            matched = df.iloc[0]["matched"]

            if total == 0:
                return False, 0.0

            match_rate = matched / total
            is_valid = match_rate >= 0.8  # At least 80% match rate

            return is_valid, match_rate

        except Exception:
            return False, 0.0
