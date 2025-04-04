"""
Tests for the SQL parser.
"""

import pytest
from sql_converter.parser import parse_sql

def test_parse_select():
    """Test parsing a simple SELECT statement."""
    sql = "SELECT id, name FROM users WHERE age > 18"
    result = parse_sql(sql, "mysql")
    
    assert result['type'] == 'SELECT'
    assert result['source_dialect'] == 'mysql'
    assert result['original_query'] == sql
    
def test_parse_insert():
    """Test parsing a simple INSERT statement."""
    sql = "INSERT INTO users (id, name) VALUES (1, 'John')"
    result = parse_sql(sql, "mysql")
    
    assert result['type'] == 'INSERT'
    assert result['source_dialect'] == 'mysql'
    assert result['original_query'] == sql

def test_parse_update():
    """Test parsing a simple UPDATE statement."""
    sql = "UPDATE users SET name = 'John' WHERE id = 1"
    result = parse_sql(sql, "mysql")
    
    assert result['type'] == 'UPDATE'
    assert result['source_dialect'] == 'mysql'
    assert result['original_query'] == sql

def test_parse_delete():
    """Test parsing a simple DELETE statement."""
    sql = "DELETE FROM users WHERE id = 1"
    result = parse_sql(sql, "mysql")
    
    assert result['type'] == 'DELETE'
    assert result['source_dialect'] == 'mysql'
    assert result['original_query'] == sql

def test_parse_invalid_sql():
    """Test parsing invalid SQL."""
    sql = "NOT A VALID SQL QUERY"
    
    # Should not raise exception but include the type as "UNKNOWN"
    result = parse_sql(sql, "mysql")
    assert result['type'] == 'UNKNOWN'

def test_parse_complex_select():
    """Test parsing a more complex SELECT statement."""
    sql = """
    SELECT 
        u.id, 
        u.name, 
        COUNT(o.id) as order_count
    FROM 
        users u
    LEFT JOIN 
        orders o ON u.id = o.user_id
    WHERE 
        u.created_at > '2020-01-01'
    GROUP BY 
        u.id, u.name
    HAVING 
        COUNT(o.id) > 0
    ORDER BY 
        u.name
    LIMIT 10
    OFFSET 20
    """
    
    result = parse_sql(sql, "mysql")
    assert result['type'] == 'SELECT'
    assert result['source_dialect'] == 'mysql'
    assert result['original_query'] == sql
