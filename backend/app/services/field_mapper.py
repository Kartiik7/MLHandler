"""Field mapping utilities for MLHandler.

This module provides the FieldMapper class which maps incoming DataFrame columns
to standardized schema field names using exact matches, case-insensitive matching,
and alias resolution.
"""
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class FieldMapper:
    """Maps DataFrame columns to schema field names.
    
    Supports:
    - Exact matches
    - Case-insensitive matches
    - Alias matching
    - Logging of unmapped columns
    
    Example schema:
        {
            "fields": {
                "Feature_A": {
                    "aliases": ["feat_a", "FA"],
                    "type": "numeric"
                },
                "Feature_B": {
                    "aliases": ["feat_b", "FB"],
                    "type": "numeric"
                }
            }
        }
    """
    
    def __init__(self, schema: Optional[Dict[str, Any]] = None):
        """Initialize FieldMapper with a schema.
        
        Args:
            schema: Dictionary with 'fields' key containing field definitions.
                   Each field can have 'aliases' list for alternative names.
        """
        self.schema = schema or {}
        self.fields = self.schema.get("fields", {})
        
        # Build mapping lookup tables
        self._build_lookup_tables()
    
    def _build_lookup_tables(self) -> None:
        """Build internal lookup tables for efficient mapping."""
        # Exact match: incoming_name -> canonical_name
        self.exact_map: Dict[str, str] = {}
        
        # Case-insensitive match: lowercase_name -> canonical_name
        self.case_insensitive_map: Dict[str, str] = {}
        
        # Alias match: alias -> canonical_name
        self.alias_map: Dict[str, str] = {}
        
        for canonical_name, field_def in self.fields.items():
            # Add canonical name to exact and case-insensitive maps
            self.exact_map[canonical_name] = canonical_name
            self.case_insensitive_map[canonical_name.lower()] = canonical_name
            
            # Add aliases
            aliases = field_def.get("aliases", [])
            for alias in aliases:
                # Exact alias match
                self.alias_map[alias] = canonical_name
                # Case-insensitive alias match
                self.case_insensitive_map[alias.lower()] = canonical_name
    
    def map_column_name(self, column_name: str) -> Tuple[str, str]:
        """Map a single column name to its canonical form.
        
        Args:
            column_name: Original column name from DataFrame
            
        Returns:
            Tuple of (mapped_name, match_type) where match_type is one of:
            'exact', 'case_insensitive', 'alias', 'unmapped'
        """
        # 1. Try exact match
        if column_name in self.exact_map:
            return self.exact_map[column_name], "exact"
        
        # 2. Try alias match (exact)
        if column_name in self.alias_map:
            return self.alias_map[column_name], "alias"
        
        # 3. Try case-insensitive match
        lower_name = column_name.lower()
        if lower_name in self.case_insensitive_map:
            return self.case_insensitive_map[lower_name], "case_insensitive"
        
        # 4. No match found
        return column_name, "unmapped"
    
    def map_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map DataFrame columns to schema field names.
        
        Args:
            df: Input DataFrame with potentially inconsistent column names
            
        Returns:
            DataFrame with standardized column names
        """
        if df.empty:
            logger.warning("Received empty DataFrame for field mapping")
            return df
        
        working = df.copy()
        rename_map: Dict[str, str] = {}
        mapping_stats: Dict[str, List[str]] = {
            "exact": [],
            "case_insensitive": [],
            "alias": [],
            "unmapped": []
        }
        
        # Map each column
        for original_col in working.columns:
            mapped_col, match_type = self.map_column_name(str(original_col))
            
            # Track mapping statistics
            mapping_stats[match_type].append(str(original_col))
            
            # Only rename if mapping was found and name changed
            if match_type != "unmapped" and original_col != mapped_col:
                rename_map[original_col] = mapped_col
        
        # Apply renaming
        if rename_map:
            working = working.rename(columns=rename_map)
            logger.info(f"Mapped {len(rename_map)} columns: {rename_map}")
        
        # Log unmapped columns
        if mapping_stats["unmapped"]:
            logger.warning(f"Unmapped columns (no schema match): {mapping_stats['unmapped']}")
        
        # Log mapping summary
        logger.info(
            f"Field mapping summary - "
            f"Exact: {len(mapping_stats['exact'])}, "
            f"Case-insensitive: {len(mapping_stats['case_insensitive'])}, "
            f"Alias: {len(mapping_stats['alias'])}, "
            f"Unmapped: {len(mapping_stats['unmapped'])}"
        )
        
        return working
    
    def get_mapping_report(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate a detailed mapping report without modifying the DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary with mapping details for each column
        """
        report = {
            "total_columns": len(df.columns),
            "mappings": {},
            "unmapped": []
        }
        
        for col in df.columns:
            mapped_name, match_type = self.map_column_name(str(col))
            
            if match_type == "unmapped":
                report["unmapped"].append(str(col))
            else:
                report["mappings"][str(col)] = {
                    "mapped_to": mapped_name,
                    "match_type": match_type
                }
        
        return report
