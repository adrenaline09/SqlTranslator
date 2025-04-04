"""
Oracle SQL dialect handler.

This module contains the Oracle dialect implementation for SQL conversion.
"""

import re
from typing import Dict, Any

class OracleDialect:
    """Oracle SQL dialect handler."""
    
    def __init__(self):
        """Initialize the Oracle dialect handler."""
        pass
    
    def convert(self, parsed_sql: Dict[str, Any]) -> str:
        """
        Convert parsed SQL to Oracle SQL.
        
        Args:
            parsed_sql (Dict[str, Any]): The parsed SQL structure
            
        Returns:
            str: Oracle SQL query
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
        """Convert a SELECT statement to Oracle dialect."""
        # Start with the basic structure
        sql_parts = ["SELECT"]
        
        # Add select items
        if 'select' in parsed_sql:
            # Oracle doesn't support LIMIT directly, we'll need to handle pagination differently
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
        
        # Handle LIMIT and OFFSET with ROWNUM for Oracle
        if ('limit' in parsed_sql and parsed_sql['limit']) or \
           ('offset' in parsed_sql and parsed_sql['offset']):
            # Oracle uses nested queries with ROWNUM for pagination
            limit_val = parsed_sql.get('limit', None)
            offset_val = parsed_sql.get('offset', None)
            
            # Wrap the query
            original_query = " ".join(sql_parts)
            
            if offset_val:
                # For queries with OFFSET, we need a double-wrapped query in Oracle
                sql_parts = [
                    f"SELECT * FROM (",
                    f"  SELECT a.*, ROWNUM rnum FROM (",
                    f"    {original_query}",
                    f"  ) a",
                    f"  WHERE ROWNUM <= {int(offset_val) + (int(limit_val) if limit_val else 0)}",
                    f") WHERE rnum > {offset_val}"
                ]
            elif limit_val:
                # For simple LIMIT queries
                sql_parts = [
                    f"SELECT * FROM (",
                    f"  {original_query}",
                    f") WHERE ROWNUM <= {limit_val}"
                ]
        
        return " ".join(sql_parts)
    
    def _convert_insert(self, parsed_sql: Dict[str, Any]) -> str:
        """Convert an INSERT statement to Oracle dialect."""
        # Oracle INSERT syntax is largely standard
        # Handle the specific Oracle requirements
        
        return parsed_sql['original_query']  # For now, just return the original
    
    def _convert_update(self, parsed_sql: Dict[str, Any]) -> str:
        """Convert an UPDATE statement to Oracle dialect."""
        # Oracle UPDATE syntax is largely standard
        # Handle the specific Oracle requirements
        
        return parsed_sql['original_query']  # For now, just return the original
    
    def _convert_delete(self, parsed_sql: Dict[str, Any]) -> str:
        """Convert a DELETE statement to Oracle dialect."""
        # Oracle DELETE syntax is largely standard
        # Handle the specific Oracle requirements
        
        return parsed_sql['original_query']  # For now, just return the original
    
    def _convert_other(self, parsed_sql: Dict[str, Any]) -> str:
        """Convert other types of SQL statements to Oracle dialect."""
        # For other statement types, just return the original query
        return parsed_sql['original_query']
    
    def _replace_functions(self, sql: str) -> str:
        """
        Replace functions with their Oracle equivalents.
        
        Args:
            sql (str): SQL string to process
            
        Returns:
            str: SQL with Oracle function syntax
        """
        # Examples of function replacements
        replacements = {
            # Date functions
            r'NOW\(\)': 'SYSDATE',
            r'CURRENT_TIMESTAMP\(\)': 'SYSTIMESTAMP',
            
            # String functions
            r'CONCAT\(([^,]+), ([^)]+)\)': r'\1 || \2',
            r'SUBSTRING\(([^,]+), ([^,]+), ([^)]+)\)': r'SUBSTR(\1, \2, \3)',
            
            # Misc functions
            r'IFNULL\(([^,]+), ([^)]+)\)': r'NVL(\1, \2)',
        }
        
        result = sql
        for pattern, replacement in replacements.items():
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        
        return result
