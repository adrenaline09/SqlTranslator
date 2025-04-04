"""
SQL query parser module.

This module handles the parsing of SQL queries into a structured format
that can be used for conversion between different SQL dialects.
"""

import sqlparse
import logging
from typing import Dict, Any, List, Optional, Tuple, Union

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQLParser:
    """SQL Parser class to parse SQL queries."""
    
    def __init__(self):
        pass
    
    def parse(self, sql: str, dialect: str) -> Dict[str, Any]:
        """
        Parse SQL query into a structured format.
        
        Args:
            sql (str): The SQL query to parse
            dialect (str): The source dialect of the SQL query
            
        Returns:
            Dict[str, Any]: Structured representation of the SQL query
        """
        try:
            # Format the query
            formatted_sql = sqlparse.format(
                sql, 
                keyword_case='upper',
                identifier_case='lower',
                strip_comments=True,
                reindent=True
            )
            
            # Parse the SQL
            parsed = sqlparse.parse(formatted_sql)
            
            if not parsed:
                raise ValueError("Failed to parse SQL query")
            
            statement = parsed[0]
            
            # Extract the statement type
            stmt_type = self._get_statement_type(statement)
            
            # Parse based on statement type
            if stmt_type == 'SELECT':
                result = self._parse_select(statement, dialect)
            elif stmt_type == 'INSERT':
                result = self._parse_insert(statement, dialect)
            elif stmt_type == 'UPDATE':
                result = self._parse_update(statement, dialect)
            elif stmt_type == 'DELETE':
                result = self._parse_delete(statement, dialect)
            else:
                result = self._parse_other(statement, dialect)
            
            result['type'] = stmt_type
            result['source_dialect'] = dialect
            result['original_query'] = sql
            
            return result
        
        except Exception as e:
            logger.error(f"Error parsing SQL: {e}")
            raise
    
    def _get_statement_type(self, statement) -> str:
        """Extract the statement type from the parsed SQL."""
        for token in statement.tokens:
            if token.ttype is sqlparse.tokens.DML:
                return token.value.upper()
        return "UNKNOWN"
    
    def _parse_select(self, statement, dialect: str) -> Dict[str, Any]:
        """Parse a SELECT statement."""
        result = {
            'select_items': [],
            'from': [],
            'joins': [],
            'where': None,
            'group_by': [],
            'having': None,
            'order_by': [],
            'limit': None,
            'offset': None
        }
        
        # Extract the main components from the SQL statement
        current_section = None
        section_tokens = {}
        
        for token in statement.tokens:
            token_upper = token.value.upper() if hasattr(token, 'value') else ''
            
            if token_upper == 'SELECT':
                current_section = 'select'
                section_tokens[current_section] = []
            elif token_upper == 'FROM':
                current_section = 'from'
                section_tokens[current_section] = []
            elif token_upper == 'WHERE':
                current_section = 'where'
                section_tokens[current_section] = []
            elif token_upper == 'GROUP BY':
                current_section = 'group_by'
                section_tokens[current_section] = []
            elif token_upper == 'HAVING':
                current_section = 'having'
                section_tokens[current_section] = []
            elif token_upper == 'ORDER BY':
                current_section = 'order_by'
                section_tokens[current_section] = []
            elif token_upper == 'LIMIT':
                current_section = 'limit'
                section_tokens[current_section] = []
            elif token_upper == 'OFFSET':
                current_section = 'offset'
                section_tokens[current_section] = []
            elif current_section and hasattr(token, 'value') and token.value.strip():
                if token.ttype is not sqlparse.tokens.Keyword:
                    section_tokens[current_section].append(token.value.strip())
        
        # Combine tokens for each section into a string
        for section, tokens in section_tokens.items():
            if tokens:
                result[section] = ' '.join(tokens)
        
        return result
    
    def _parse_insert(self, statement, dialect: str) -> Dict[str, Any]:
        """Parse an INSERT statement."""
        result = {
            'table': None,
            'columns': [],
            'values': [],
            'select': None
        }
        
        # Extract table name and other components
        table_found = False
        values_section = False
        current_tokens = []
        
        for token in statement.tokens:
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'INTO':
                for t in statement.tokens[statement.tokens.index(token)+1:]:
                    if t.ttype is not sqlparse.tokens.Whitespace and not table_found:
                        result['table'] = t.value.strip()
                        table_found = True
                        break
            
            # Extract VALUES section
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'VALUES':
                values_section = True
                current_tokens = []
            elif values_section and token.ttype is not sqlparse.tokens.Keyword and hasattr(token, 'value') and token.value.strip():
                current_tokens.append(token.value.strip())
        
        if current_tokens:
            result['values'] = ' '.join(current_tokens)
        
        return result
    
    def _parse_update(self, statement, dialect: str) -> Dict[str, Any]:
        """Parse an UPDATE statement."""
        result = {
            'table': None,
            'set_clauses': [],
            'where': None
        }
        
        # Extract table name and other components
        current_section = None
        section_tokens = {'table': [], 'set': [], 'where': []}
        
        for i, token in enumerate(statement.tokens):
            token_upper = token.value.upper() if hasattr(token, 'value') else ''
            
            if token.ttype is sqlparse.tokens.Keyword and token_upper == 'UPDATE':
                current_section = 'table'
            elif token.ttype is sqlparse.tokens.Keyword and token_upper == 'SET':
                current_section = 'set'
            elif token.ttype is sqlparse.tokens.Keyword and token_upper == 'WHERE':
                current_section = 'where'
            elif current_section and hasattr(token, 'value') and token.value.strip():
                if token.ttype is not sqlparse.tokens.Keyword:
                    section_tokens[current_section].append(token.value.strip())
        
        # Combine tokens for each section
        if section_tokens['table']:
            result['table'] = section_tokens['table'][0]  # Usually just the first non-whitespace token
        if section_tokens['set']:
            result['set_clauses'] = ' '.join(section_tokens['set'])
        if section_tokens['where']:
            result['where'] = ' '.join(section_tokens['where'])
        
        return result
    
    def _parse_delete(self, statement, dialect: str) -> Dict[str, Any]:
        """Parse a DELETE statement."""
        result = {
            'table': None,
            'where': None
        }
        
        # Extract table name and WHERE clause
        current_section = None
        section_tokens = {'table': [], 'where': []}
        
        for i, token in enumerate(statement.tokens):
            token_upper = token.value.upper() if hasattr(token, 'value') else ''
            
            if token.ttype is sqlparse.tokens.Keyword and token_upper == 'FROM':
                current_section = 'table'
            elif token.ttype is sqlparse.tokens.Keyword and token_upper == 'WHERE':
                current_section = 'where'
            elif current_section and hasattr(token, 'value') and token.value.strip():
                if token.ttype is not sqlparse.tokens.Keyword:
                    section_tokens[current_section].append(token.value.strip())
        
        # Combine tokens for each section
        if section_tokens['table']:
            result['table'] = section_tokens['table'][0]  # Usually just the first non-whitespace token
        if section_tokens['where']:
            result['where'] = ' '.join(section_tokens['where'])
        
        return result
    
    def _parse_other(self, statement, dialect: str) -> Dict[str, Any]:
        """Parse other types of SQL statements."""
        return {
            'statement': str(statement)
        }


def parse_sql(sql: str, dialect: str) -> Dict[str, Any]:
    """
    Parse a SQL query into a structured format.
    
    Args:
        sql (str): The SQL query to parse
        dialect (str): The source dialect of the SQL query
        
    Returns:
        Dict[str, Any]: Structured representation of the SQL query
    """
    parser = SQLParser()
    return parser.parse(sql, dialect)
