"""
Tests for the SQL converter.
"""

import pytest
from sql_converter.converter import convert_sql

def test_convert_identical_dialects():
    """Test that conversion between identical dialects returns the original."""
    sql = "SELECT id, name FROM users WHERE age > 18"
    
    # Converting from MySQL to MySQL should return the original
    result = convert_sql(sql, "mysql", "mysql")
    assert result == sql

def test_convert_simple_select():
    """Test converting a simple SELECT statement."""
    sql = "SELECT id, name FROM users WHERE age > 18"
    
    # Convert from MySQL to PostgreSQL
    result = convert_sql(sql, "mysql", "postgresql")
    
    # Since this is a simple query with standard SQL, it should be very similar
    assert "SELECT" in result
    assert "FROM users" in result
    assert "WHERE" in result

def test_convert_with_limit():
    """Test converting a SELECT with LIMIT."""
    sql = "SELECT id, name FROM users LIMIT 10"
    
    # Convert from MySQL to Oracle (which uses ROWNUM instead of LIMIT)
    result = convert_sql(sql, "mysql", "oracle")
    
    # Oracle uses a subquery with ROWNUM
    assert "ROWNUM" in result or "rownum" in result

def test_convert_with_functions():
    """Test converting SQL with database-specific functions."""
    # MySQL's NOW() function should be converted to Oracle's SYSDATE
    sql = "SELECT * FROM users WHERE created_at > NOW()"
    
    result = convert_sql(sql, "mysql", "oracle")
    
    # Oracle uses SYSDATE instead of NOW()
    assert "SYSDATE" in result

def test_convert_unsupported_dialect():
    """Test that trying to convert to/from an unsupported dialect raises an error."""
    sql = "SELECT id, name FROM users"
    
    with pytest.raises(ValueError):
        convert_sql(sql, "unsupported", "mysql")
    
    with pytest.raises(ValueError):
        convert_sql(sql, "mysql", "unsupported")
