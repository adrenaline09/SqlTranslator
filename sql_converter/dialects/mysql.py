"""
MySQL SQL dialect handler.

This module contains the MySQL dialect implementation for SQL conversion.
"""

import re
from typing import Dict, Any

class MySQLDialect:
    """MySQL SQL dialect handler."""
    
    def __init__(self):
        """Initialize the MySQL dialect handler."""
        pass
    
    def convert(self, parsed_sql: Dict[str, Any]) -> str:
        """
        Convert parsed SQL to MySQL SQL.
        
        Args:
            parsed_sql (Dict[str, Any]): The parsed SQL structure
            
        Returns:
            str: MySQL SQL query
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
        """Convert a SELECT statement to MySQL dialect."""
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
        
        # Process for MySQL-specific syntax
        result = " ".join(sql_parts)
        result = self._replace_functions(result)
        
        return result
    
    def _convert_insert(self, parsed_sql: Dict[str, Any]) -> str:
        """Convert an INSERT statement to MySQL dialect."""
        # MySQL INSERT syntax is standard with some extensions
        
        # Start with the original query
        original = parsed_sql['original_query']
        
        # Replace any dialect-specific constructs
        result = self._replace_functions(original)
        
        # MySQL-specific INSERT modifications
        # If the query has ON DUPLICATE KEY UPDATE, etc., handle it here
        
        return result
    
    def _convert_update(self, parsed_sql: Dict[str, Any]) -> str:
        """Convert an UPDATE statement to MySQL dialect."""
        # MySQL UPDATE syntax is standard with some extensions
        
        # Start with the original query
        original = parsed_sql['original_query']
        
        # Replace any dialect-specific constructs
        result = self._replace_functions(original)
        
        return result
    
    def _convert_delete(self, parsed_sql: Dict[str, Any]) -> str:
        """Convert a DELETE statement to MySQL dialect."""
        # MySQL DELETE syntax is standard with some extensions
        
        # Start with the original query
        original = parsed_sql['original_query']
        
        # Replace any dialect-specific constructs
        result = self._replace_functions(original)
        
        return result
    
    def _convert_other(self, parsed_sql: Dict[str, Any]) -> str:
        """Convert other types of SQL statements to MySQL dialect."""
        # For other statement types, just process function replacements
        original = parsed_sql['original_query']
        result = self._replace_functions(original)
        
        return result
    
    def _replace_functions(self, sql: str) -> str:
        """
        Replace functions with their MySQL equivalents.
        
        Args:
            sql (str): SQL string to process
            
        Returns:
            str: SQL with MySQL function syntax
        """
        # Examples of function replacements for MySQL
        replacements = {
            # Date functions
            r'SYSDATE': 'NOW()',
            r'SYSTIMESTAMP': 'NOW(3)',
            r'TO_DATE\(([^,]+),\s*([^)]+)\)': r'STR_TO_DATE(\1, \2)',
            
            # String functions
            r'NVL\(([^,]+),\s*([^)]+)\)': r'IFNULL(\1, \2)',
            r'SUBSTR\(': 'SUBSTRING(',
            
            # Concatenation  
            r'(\w+)\s*\|\|\s*(\w+)': r'CONCAT(\1, \2)',
        }
        
        result = sql
        for pattern, replacement in replacements.items():
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        
        return result
