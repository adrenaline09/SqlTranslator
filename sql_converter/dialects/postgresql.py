"""
PostgreSQL dialect handler.

This module contains the PostgreSQL dialect implementation for SQL conversion.
"""

import re
from typing import Dict, Any

class PostgreSQLDialect:
    """PostgreSQL SQL dialect handler."""
    
    def __init__(self):
        """Initialize the PostgreSQL dialect handler."""
        pass
    
    def convert(self, parsed_sql: Dict[str, Any]) -> str:
        """
        Convert parsed SQL to PostgreSQL SQL.
        
        Args:
            parsed_sql (Dict[str, Any]): The parsed SQL structure
            
        Returns:
            str: PostgreSQL SQL query
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
        """Convert a SELECT statement to PostgreSQL dialect."""
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
        
        # Add LIMIT and OFFSET clauses
        if 'limit' in parsed_sql and parsed_sql['limit']:
            sql_parts.append("LIMIT")
            sql_parts.append(parsed_sql['limit'])
            
            if 'offset' in parsed_sql and parsed_sql['offset']:
                sql_parts.append("OFFSET")
                sql_parts.append(parsed_sql['offset'])
        
        # Process for PostgreSQL-specific syntax
        result = " ".join(sql_parts)
        result = self._replace_functions(result)
        
        return result
    
    def _convert_insert(self, parsed_sql: Dict[str, Any]) -> str:
        """Convert an INSERT statement to PostgreSQL dialect."""
        # PostgreSQL INSERT syntax has some extensions like RETURNING
        
        # Start with the original query
        original = parsed_sql['original_query']
        
        # Replace any dialect-specific constructs
        result = self._replace_functions(original)
        
        # Check for specific syntax conversions
        # - Oracle's RETURNING INTO -> PostgreSQL's RETURNING
        result = re.sub(
            r'RETURNING\s+(.+?)\s+INTO\s+(.+?)(?:\s+|;|$)', 
            r'RETURNING \1',
            result, 
            flags=re.IGNORECASE
        )
        
        return result
    
    def _convert_update(self, parsed_sql: Dict[str, Any]) -> str:
        """Convert an UPDATE statement to PostgreSQL dialect."""
        # PostgreSQL UPDATE syntax is standard with extensions like RETURNING
        
        # Start with the original query
        original = parsed_sql['original_query']
        
        # Replace any dialect-specific constructs
        result = self._replace_functions(original)
        
        # Check for Oracle's RETURNING INTO -> PostgreSQL's RETURNING
        result = re.sub(
            r'RETURNING\s+(.+?)\s+INTO\s+(.+?)(?:\s+|;|$)', 
            r'RETURNING \1',
            result, 
            flags=re.IGNORECASE
        )
        
        return result
    
    def _convert_delete(self, parsed_sql: Dict[str, Any]) -> str:
        """Convert a DELETE statement to PostgreSQL dialect."""
        # PostgreSQL DELETE syntax is standard with extensions like RETURNING
        
        # Start with the original query
        original = parsed_sql['original_query']
        
        # Replace any dialect-specific constructs
        result = self._replace_functions(original)
        
        # Check for Oracle's RETURNING INTO -> PostgreSQL's RETURNING
        result = re.sub(
            r'RETURNING\s+(.+?)\s+INTO\s+(.+?)(?:\s+|;|$)', 
            r'RETURNING \1',
            result, 
            flags=re.IGNORECASE
        )
        
        return result
    
    def _convert_other(self, parsed_sql: Dict[str, Any]) -> str:
        """Convert other types of SQL statements to PostgreSQL dialect."""
        # For other statement types, just process function replacements
        original = parsed_sql['original_query']
        result = self._replace_functions(original)
        
        return result
    
    def _replace_functions(self, sql: str) -> str:
        """
        Replace functions with their PostgreSQL equivalents.
        
        Args:
            sql (str): SQL string to process
            
        Returns:
            str: SQL with PostgreSQL function syntax
        """
        # Examples of function replacements for PostgreSQL
        replacements = {
            # Date functions
            r'SYSDATE': 'CURRENT_DATE',
            r'SYSTIMESTAMP': 'CURRENT_TIMESTAMP',
            r'TO_DATE\(([^,]+),\s*([^)]+)\)': r'TO_DATE(\1, \2)',
            
            # String functions
            r'NVL\(([^,]+),\s*([^)]+)\)': r'COALESCE(\1, \2)',
            r'SUBSTR\(': 'SUBSTRING(',
            
            # Number functions
            r'DECODE\(([^,]+),\s*([^,]+),\s*([^,]+),\s*([^)]+)\)': r'CASE WHEN \1 = \2 THEN \3 ELSE \4 END',
        }
        
        result = sql
        for pattern, replacement in replacements.items():
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        
        return result
