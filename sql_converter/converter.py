"""
SQL Converter module.

This module handles the conversion of SQL queries between different dialects.
"""

import logging
import re
import sqlparse
from typing import Dict, Any, List, Optional, Union, Callable, Match

from .parser import parse_sql
from .dialects import get_dialect_handler, get_supported_dialects

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQLConverter:
    """
    SQLConverter class to convert SQL queries between different dialects.
    
    This class handles the conversion of SQL queries between different database dialects
    by parsing SQL statements and applying dialect-specific transformations.
    """
    
    # Date format mappings between dialects
    DATE_FORMAT_MAPPINGS = {
        'mysql_to_postgresql': {
            '%Y': 'YYYY',
            '%m': 'MM',
            '%d': 'DD',
            '%H': 'HH24',
            '%i': 'MI',
            '%s': 'SS',
        },
        'postgresql_to_mysql': {
            'YYYY': '%Y',
            'MM': '%m',
            'DD': '%d',
            'HH24': '%H',
            'MI': '%i',
            'SS': '%s',
        },
        'oracle_to_postgresql': {
            'YYYY': 'YYYY',
            'MM': 'MM',
            'DD': 'DD',
            'HH24': 'HH24',
            'MI': 'MI',
            'SS': 'SS',
        },
        'oracle_to_pyspark': {
            'YYYY': 'yyyy',
            'MM': 'MM',
            'DD': 'dd',
            'HH24': 'HH',
            'MI': 'mm',
            'SS': 'ss',
        },
    }
    
    def __init__(self):
        """Initialize the SQL Converter."""
        # Initialize function mappings with helper methods
        self._init_function_mappings()
    
    def _init_function_mappings(self):
        """Initialize function name mappings between dialects."""
        self.FUNCTION_MAPPINGS = {
            # Date functions
            ('mysql', 'postgresql'): {
                r'DATE_FORMAT\s*\(\s*([^,]+)\s*,\s*[\'"](.*?)[\'"]\s*\)': 
                    lambda m: f"TO_CHAR({m.group(1)}, '{self._format_date_pattern(m.group(2), 'mysql', 'postgresql')}')",
                r'NOW\(\s*\)': 'CURRENT_TIMESTAMP',
                r'CURDATE\(\s*\)': 'CURRENT_DATE',
                r'INTERVAL\s+(\d+)\s+DAY': lambda m: f"INTERVAL '{m.group(1)} DAY'",
            },
            ('postgresql', 'mysql'): {
                r'TO_CHAR\s*\(\s*([^,]+)\s*,\s*[\'"](.*?)[\'"]\s*\)': 
                    lambda m: f"DATE_FORMAT({m.group(1)}, '{self._format_date_pattern(m.group(2), 'postgresql', 'mysql')}')",
                r'CURRENT_TIMESTAMP': 'NOW()',
                r'CURRENT_DATE': 'CURDATE()',
                r'INTERVAL\s+[\'"](.*?)[\'"]\s+DAY': lambda m: f"INTERVAL {m.group(1)} DAY",
            },
            ('oracle', 'postgresql'): {
                r'TO_CHAR\s*\(\s*([^,]+)\s*,\s*[\'"](.*?)[\'"]\s*\)': 
                    lambda m: f"TO_CHAR({m.group(1)}, '{self._format_date_pattern(m.group(2), 'oracle', 'postgresql')}')",
                r'SYSDATE': 'CURRENT_DATE',
                r'SYSTIMESTAMP': 'CURRENT_TIMESTAMP',
                r'ADD_MONTHS\s*\(\s*([^,]+)\s*,\s*(-?\d+)\s*\)': lambda m: f"({m.group(1)} + INTERVAL '{m.group(2)} MONTH')",
            },
            ('oracle', 'pyspark'): {
                r'TO_CHAR\s*\(\s*([^,]+)\s*,\s*[\'"](.*?)[\'"]\s*\)': 
                    lambda m: f"date_format({m.group(1)}, '{self._format_date_pattern(m.group(2), 'oracle', 'pyspark')}')",
                r'SYSDATE': 'current_date()',
                r'SYSTIMESTAMP': 'current_timestamp()',
                r'ADD_MONTHS\s*\(\s*([^,]+)\s*,\s*(-?\d+)\s*\)': lambda m: f"add_months({m.group(1)}, {m.group(2)})",
            },
            # String functions
            ('mysql', 'postgresql'): {
                r'CONCAT\s*\(([^)]+)\)': self._concat_to_pipe,
                r'IFNULL\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)': lambda m: f"COALESCE({m.group(1)}, {m.group(2)})",
            },
            ('postgresql', 'mysql'): {
                r'([\w\.]+)\s*\|\|\s*([\w\.\'\"]+)': lambda m: f"CONCAT({m.group(1)}, {m.group(2)})",
                r'COALESCE\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)': lambda m: f"IFNULL({m.group(1)}, {m.group(2)})",
            },
            ('oracle', 'postgresql'): {
                r'([\w\.]+)\s*\|\|\s*([\w\.\'\"]+)': lambda m: f"{m.group(1)} || {m.group(2)}",
                r'NVL\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)': lambda m: f"COALESCE({m.group(1)}, {m.group(2)})",
            },
            ('oracle', 'pyspark'): {
                r'([\w\.]+)\s*\|\|\s*([\w\.\'\"]+)': lambda m: f"concat({m.group(1)}, {m.group(2)})",
                r'NVL\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)': lambda m: f"coalesce({m.group(1)}, {m.group(2)})",
            },
        }
    
    def _concat_to_pipe(self, match: Match) -> str:
        """Convert MySQL CONCAT to PostgreSQL string concatenation with pipes."""
        args = match.group(1).split(',')
        return ' || '.join(arg.strip() for arg in args)
    
    def _format_date_pattern(self, pattern: str, source_dialect: str, target_dialect: str) -> str:
        """
        Convert date format patterns between different SQL dialects.
        
        Args:
            pattern (str): The date format pattern to convert
            source_dialect (str): The source SQL dialect
            target_dialect (str): The target SQL dialect
            
        Returns:
            str: The converted date format pattern
        """
        mapping_key = f"{source_dialect}_to_{target_dialect}"
        
        if mapping_key in self.DATE_FORMAT_MAPPINGS:
            mapping = self.DATE_FORMAT_MAPPINGS[mapping_key]
            result = pattern
            for src, tgt in mapping.items():
                result = result.replace(src, tgt)
            return result
        
        return pattern
    
    def convert(self, sql: str, source_dialect: str, target_dialect: str) -> str:
        """
        Convert SQL from one dialect to another.
        
        Args:
            sql (str): The SQL query to convert
            source_dialect (str): The source SQL dialect
            target_dialect (str): The target SQL dialect
            
        Returns:
            str: The converted SQL query
        """
        try:
            # Check that source and target dialects are different
            if source_dialect.lower() == target_dialect.lower():
                return sql
            
            # Parse the SQL
            parsed_sql = parse_sql(sql, source_dialect)
            
            # Get the dialect handler for the target dialect
            dialect_handler = get_dialect_handler(target_dialect)
            
            # Convert the parsed SQL to the target dialect
            converted_sql = dialect_handler.convert(parsed_sql)
            
            # Apply function name and syntax transformations
            converted_sql = self._apply_function_transformations(
                converted_sql, source_dialect, target_dialect
            )
            
            # Format the SQL to make it more readable
            formatted_sql = sqlparse.format(
                converted_sql,
                keyword_case='upper',
                identifier_case='lower',
                reindent=True,
                reindent_aligned=True
            )
            
            return formatted_sql
        
        except Exception as e:
            logger.error(f"Error converting SQL: {e}")
            raise
    
    def _apply_function_transformations(self, sql: str, source_dialect: str, target_dialect: str) -> str:
        """
        Apply SQL function name and syntax transformations based on the source and target dialects.
        
        Args:
            sql (str): The SQL query to transform
            source_dialect (str): The source SQL dialect
            target_dialect (str): The target SQL dialect
            
        Returns:
            str: The transformed SQL query
        """
        # Get the appropriate function mappings for this conversion
        mapping_key = (source_dialect, target_dialect)
        if mapping_key in self.FUNCTION_MAPPINGS:
            mappings = self.FUNCTION_MAPPINGS[mapping_key]
            
            # Apply each transformation in sequence
            result = sql
            for pattern, replacement in mappings.items():
                if callable(replacement):
                    # For more complex replacements that need a function
                    result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
                else:
                    # For simple string replacements
                    result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
            
            return result
        
        return sql
    
    def batch_convert(self, sql_queries: List[str], source_dialect: str, target_dialect: str) -> List[str]:
        """
        Convert multiple SQL queries from one dialect to another.
        
        Args:
            sql_queries (List[str]): List of SQL queries to convert
            source_dialect (str): The source SQL dialect
            target_dialect (str): The target SQL dialect
            
        Returns:
            List[str]: List of converted SQL queries
        """
        converted_queries = []
        errors = []
        
        for i, sql in enumerate(sql_queries):
            try:
                converted_sql = self.convert(sql, source_dialect, target_dialect)
                converted_queries.append(converted_sql)
            except Exception as e:
                logger.error(f"Error converting query {i}: {e}")
                errors.append({"index": i, "query": sql, "error": str(e)})
                converted_queries.append(None)  # Placeholder for failed conversion
        
        if errors:
            logger.warning(f"Completed with {len(errors)} errors")
            
        return converted_queries


def convert_sql(sql: str, source_dialect: str, target_dialect: str) -> str:
    """
    Convert SQL from one dialect to another.
    
    Args:
        sql (str): The SQL query to convert
        source_dialect (str): The source SQL dialect
        target_dialect (str): The target SQL dialect
        
    Returns:
        str: The converted SQL query
    """
    converter = SQLConverter()
    return converter.convert(sql, source_dialect, target_dialect)
