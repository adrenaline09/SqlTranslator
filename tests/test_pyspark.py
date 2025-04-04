"""
Tests for the PySpark dialect handler.
"""

import pytest
from sql_converter.dialects.pyspark import PySparkDialect
from sql_converter.parser import parse_sql

def test_pyspark_select():
    """Test converting a SELECT statement to PySpark syntax."""
    sql = "SELECT id, name FROM users WHERE age > 18"
    parsed = parse_sql(sql, "mysql")
    
    pyspark = PySparkDialect()
    result = pyspark.convert(parsed)
    
    # PySpark supports standard SELECT syntax
    assert "SELECT" in result
    assert "FROM users" in result
    assert "WHERE" in result

def test_pyspark_date_functions():
    """Test converting date functions to PySpark syntax."""
    # Test converting Oracle's SYSDATE to PySpark's current_date()
    sql = "SELECT * FROM users WHERE created_at > SYSDATE"
    parsed = parse_sql(sql, "oracle")
    
    pyspark = PySparkDialect()
    result = pyspark.convert(parsed)
    
    # PySpark uses current_date() instead of SYSDATE
    assert "current_date()" in result

def test_pyspark_update():
    """Test handling an UPDATE statement in PySpark."""
    sql = "UPDATE users SET name = 'John' WHERE id = 1"
    parsed = parse_sql(sql, "mysql")
    
    pyspark = PySparkDialect()
    result = pyspark.convert(parsed)
    
    # PySpark doesn't support UPDATE directly in older versions
    # Should include a warning comment
    assert "--" in result  # Should have a comment
    assert "UPDATE" in result  # Should still include the original statement

def test_pyspark_limit():
    """Test converting a query with LIMIT to PySpark syntax."""
    sql = "SELECT * FROM users LIMIT 10"
    parsed = parse_sql(sql, "mysql")
    
    pyspark = PySparkDialect()
    result = pyspark.convert(parsed)
    
    # PySpark supports LIMIT
    assert "LIMIT" in result
    assert "10" in result  # The limit value should still be there
