"""
PySpark SQL dialect handler.

This module contains the PySpark dialect implementation for SQL conversion.
"""

import re
from typing import Dict, Any

class PySparkDialect:
    """PySpark SQL dialect handler."""
    
    def __init__(self):
        """Initialize the PySpark dialect handler."""
        pass
    
    def convert(self, parsed_sql: Dict[str, Any]) -> str:
        """
        Convert parsed SQL to PySpark SQL.
        
        Args:
            parsed_sql (Dict[str, Any]): The parsed SQL structure
            
        Returns:
            str: PySpark SQL query
        """
        sql_type = parsed_sql['type']
        
        if sql_type == 'SELECT':
            return self._convert_select(parsed_sql)
        elif sql_type == 'INSERT':
            return self._convert_insert(parsed_sql)
        elif sql_type == 'UPDATE':
            return self._convert_update(parsed_sql)
        elif sql_type == 'DELETE':
            return self._convert_delete(parsed_sql)
        else:
            # For other statement types, try a generic conversion
            return self._convert_other(parsed_sql)
    
    def _convert_select(self, parsed_sql: Dict[str, Any]) -> str:
        """Convert a SELECT statement to PySpark dialect."""
        # Start with the basic structure
        sql_parts = ["SELECT"]
        
        # Add select items
        if 'select' in parsed_sql:
            sql_parts.append(parsed_sql['select'])
        else:
            sql_parts.append("*")
        
        # Add FROM clause
        if 'from' in parsed_sql:
            sql_parts.append("FROM")
            sql_parts.append(parsed_sql['from'])
        
        # Add WHERE clause
        if 'where' in parsed_sql and parsed_sql['where']:
            sql_parts.append("WHERE")
            sql_parts.append(parsed_sql['where'])
        
        # Add GROUP BY clause
        if 'group_by' in parsed_sql and parsed_sql['group_by']:
            sql_parts.append("GROUP BY")
            sql_parts.append(parsed_sql['group_by'])
        
        # Add HAVING clause
        if 'having' in parsed_sql and parsed_sql['having']:
            sql_parts.append("HAVING")
            sql_parts.append(parsed_sql['having'])
        
        # Add ORDER BY clause
        if 'order_by' in parsed_sql and parsed_sql['order_by']:
            sql_parts.append("ORDER BY")
            sql_parts.append(parsed_sql['order_by'])
        
        # Add LIMIT clause
        if 'limit' in parsed_sql and parsed_sql['limit']:
            sql_parts.append("LIMIT")
            sql_parts.append(parsed_sql['limit'])
        
        # Process for PySpark-specific syntax
        result = " ".join(sql_parts)
        result = self._replace_functions(result)
        
        return result
    
    def _convert_insert(self, parsed_sql: Dict[str, Any]) -> str:
        """Convert an INSERT statement to PySpark dialect."""
        # PySpark INSERT syntax is similar to standard SQL, but with some differences
        # In PySpark, we often use df.write methods instead of SQL INSERT
        
        # For SQL-based inserts, PySpark supports INSERT INTO and INSERT OVERWRITE
        
        return parsed_sql['original_query']  # For now, return the original query
    
    def _convert_update(self, parsed_sql: Dict[str, Any]) -> str:
        """Convert an UPDATE statement to PySpark dialect."""
        # PySpark doesn't support UPDATE directly in SQL in older versions
        # In newer versions, this depends on the Delta Lake setup
        
        # For now, return a comment noting that this might need manual conversion
        return (
            "-- Note: PySpark SQL may not support UPDATE statements directly.\n"
            "-- Consider using DataFrame operations instead.\n" +
            parsed_sql['original_query']
        )
    
    def _convert_delete(self, parsed_sql: Dict[str, Any]) -> str:
        """Convert a DELETE statement to PySpark dialect."""
        # PySpark doesn't support DELETE directly in SQL in older versions
        # In newer versions, this depends on the Delta Lake setup
        
        # For now, return a comment noting that this might need manual conversion
        return (
            "-- Note: PySpark SQL may not support DELETE statements directly.\n"
            "-- Consider using DataFrame operations instead.\n" +
            parsed_sql['original_query']
        )
    
    def _convert_other(self, parsed_sql: Dict[str, Any]) -> str:
        """Convert other types of SQL statements to PySpark dialect."""
        # For other statement types, just return the original query with a note
        return (
            "-- Note: This statement may need manual review for PySpark compatibility.\n" +
            parsed_sql['original_query']
        )
    
    def _replace_functions(self, sql: str) -> str:
        """
        Replace functions with their PySpark equivalents.
        
        Args:
            sql (str): SQL string to process
            
        Returns:
            str: SQL with PySpark function syntax
        """
        # Examples of function replacements for PySpark
        replacements = {
            # Date functions
            r'SYSDATE': 'current_date()',
            r'GETDATE\(\)': 'current_timestamp()',
            
            # String functions
            r'SUBSTR\(([^,]+), ([^,]+), ([^)]+)\)': r'substring(\1, \2, \3)',
            
            # Aggregation
            r'TOP\s+(\d+)': r'LIMIT \1',
        }
        
        result = sql
        for pattern, replacement in replacements.items():
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        
        return result
