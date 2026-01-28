"""Column name and type matching for relationship detection."""
import re
from dataclasses import dataclass
from typing import Any

from core.bigquery_client import ColumnInfo


@dataclass
class ColumnMatch:
    """A potential column match between tables."""

    source_table: str
    source_column: str
    target_table: str
    target_column: str
    match_type: str  # 'exact', 'pattern', 'type_compatible'
    confidence: float  # 0.0 to 1.0


class ColumnMatcher:
    """Matches columns between tables based on naming patterns and types."""

    # Common FK naming patterns
    FK_PATTERNS = [
        # table_id -> table.id
        (r"^(\w+)_id$", r"^id$", "fk_to_id"),
        # table_name_id -> table_name.id
        (r"^(\w+)_id$", r"^id$", "fk_to_id"),
        # fk_table_id -> table.id
        (r"^fk_(\w+)_id$", r"^id$", "explicit_fk"),
        # ref_table -> table.id
        (r"^ref_(\w+)$", r"^id$", "ref_pattern"),
    ]

    # Type compatibility mapping
    TYPE_COMPATIBILITY = {
        "INT64": {"INT64", "INTEGER", "NUMERIC"},
        "INTEGER": {"INT64", "INTEGER", "NUMERIC"},
        "STRING": {"STRING"},
        "FLOAT64": {"FLOAT64", "FLOAT", "NUMERIC"},
        "NUMERIC": {"INT64", "INTEGER", "FLOAT64", "FLOAT", "NUMERIC", "BIGNUMERIC"},
        "BOOL": {"BOOL", "BOOLEAN"},
        "BOOLEAN": {"BOOL", "BOOLEAN"},
        "DATE": {"DATE", "DATETIME", "TIMESTAMP"},
        "DATETIME": {"DATE", "DATETIME", "TIMESTAMP"},
        "TIMESTAMP": {"DATE", "DATETIME", "TIMESTAMP"},
    }

    def __init__(self, confidence_threshold: float = 0.5):
        """Initialize the matcher with a confidence threshold."""
        self.confidence_threshold = confidence_threshold

    def find_matches(
        self,
        source_table: str,
        source_columns: list[ColumnInfo],
        target_table: str,
        target_columns: list[ColumnInfo],
    ) -> list[ColumnMatch]:
        """
        Find potential column matches between two tables.

        Args:
            source_table: Name of the source table
            source_columns: Columns in the source table
            target_table: Name of the target table
            target_columns: Columns in the target table

        Returns:
            List of potential column matches above confidence threshold
        """
        matches = []

        for source_col in source_columns:
            for target_col in target_columns:
                match = self._check_match(
                    source_table, source_col,
                    target_table, target_col,
                )
                if match and match.confidence >= self.confidence_threshold:
                    matches.append(match)

        return matches

    def _check_match(
        self,
        source_table: str,
        source_col: ColumnInfo,
        target_table: str,
        target_col: ColumnInfo,
    ) -> ColumnMatch | None:
        """Check if two columns match."""
        # Check type compatibility first
        if not self._types_compatible(source_col.data_type, target_col.data_type):
            return None

        # Check for exact name match
        if source_col.name.lower() == target_col.name.lower():
            return ColumnMatch(
                source_table=source_table,
                source_column=source_col.name,
                target_table=target_table,
                target_column=target_col.name,
                match_type="exact",
                confidence=0.9,
            )

        # Check for FK pattern matches
        pattern_match = self._check_fk_pattern(
            source_table, source_col.name,
            target_table, target_col.name,
        )
        if pattern_match:
            return ColumnMatch(
                source_table=source_table,
                source_column=source_col.name,
                target_table=target_table,
                target_column=target_col.name,
                match_type=f"pattern:{pattern_match}",
                confidence=0.8,
            )

        # Check for column name containing table name
        table_ref_match = self._check_table_reference(
            source_col.name, target_table, target_col.name
        )
        if table_ref_match:
            return ColumnMatch(
                source_table=source_table,
                source_column=source_col.name,
                target_table=target_table,
                target_column=target_col.name,
                match_type="table_reference",
                confidence=0.7,
            )

        return None

    def _types_compatible(self, type1: str, type2: str) -> bool:
        """Check if two BigQuery types are compatible."""
        type1_upper = type1.upper()
        type2_upper = type2.upper()

        if type1_upper == type2_upper:
            return True

        compatible_types = self.TYPE_COMPATIBILITY.get(type1_upper, set())
        return type2_upper in compatible_types

    def _check_fk_pattern(
        self,
        source_table: str,
        source_col: str,
        target_table: str,
        target_col: str,
    ) -> str | None:
        """Check if columns match FK naming patterns."""
        source_col_lower = source_col.lower()
        target_col_lower = target_col.lower()
        target_table_lower = target_table.lower()

        # Check common patterns
        for source_pattern, target_pattern, pattern_name in self.FK_PATTERNS:
            source_match = re.match(source_pattern, source_col_lower)
            target_match = re.match(target_pattern, target_col_lower)

            if source_match and target_match:
                # Extract the table reference from source
                if source_match.groups():
                    ref_table = source_match.group(1)
                    # Check if it matches target table (singular/plural)
                    if self._table_names_match(ref_table, target_table_lower):
                        return pattern_name

        return None

    def _check_table_reference(
        self,
        source_col: str,
        target_table: str,
        target_col: str,
    ) -> bool:
        """Check if source column name references target table."""
        source_col_lower = source_col.lower()
        target_table_lower = target_table.lower()
        target_col_lower = target_col.lower()

        # Pattern: {table}_id matches {table}.id
        if source_col_lower.endswith("_id"):
            col_prefix = source_col_lower[:-3]
            if self._table_names_match(col_prefix, target_table_lower):
                return target_col_lower == "id"

        return False

    def _table_names_match(self, name1: str, name2: str) -> bool:
        """Check if two table names match (handling singular/plural)."""
        name1 = name1.lower()
        name2 = name2.lower()

        if name1 == name2:
            return True

        # Simple plural handling
        if name1 + "s" == name2 or name2 + "s" == name1:
            return True

        if name1 + "es" == name2 or name2 + "es" == name1:
            return True

        # Handle 'ies' plural (category -> categories)
        if name1.endswith("y") and name1[:-1] + "ies" == name2:
            return True
        if name2.endswith("y") and name2[:-1] + "ies" == name1:
            return True

        return False
