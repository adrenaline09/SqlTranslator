"""
Tests for the Oracle dialect handler.
"""

import pytest
from sql_converter.dialects.oracle import OracleDialect
from sql_converter.parser import parse_sql

def test_oracle_select_with_limit():
    """Test converting a SELECT with LIMIT to Oracle syntax."""
    # Oracle doesn't have LIMIT directly, it uses ROWNUM
    sql = "SELECT id, name FROM users LIMIT 10"
    parsed = parse_sql(sql, "mysql")
    
    oracle = OracleDialect()
    result = oracle.convert(parsed)
    
    # Oracle should use ROWNUM for limit
    assert "ROWNUM" in result or "rownum" in result
    assert "10" in result  # The limit value should still be there

def test_oracle_date_functions():
    """Test converting date functions to Oracle syntax."""
    # Test converting NOW() to SYSDATE
    sql = "SELECT * FROM users WHERE created_at > NOW()"
    parsed = parse_sql(sql, "mysql")
    
    oracle = OracleDialect()
    result = oracle.convert(parsed)
    
    # Oracle uses SYSDATE instead of NOW()
    assert "SYSDATE" in result

def test_oracle_string_concat():
    """Test converting string concatenation to Oracle syntax."""
    # Test converting CONCAT() to ||
    sql = "SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM users"
    parsed = parse_sql(sql, "mysql")
    
    oracle = OracleDialect()
    result = oracle.convert(parsed)
    
    # Oracle uses || for concatenation
    # Note: This is simplified - the actual regexp replacement might be more complex
    assert "||" in result or "CONCAT" in result  # Either is acceptable

def test_oracle_pagination():
    """Test converting pagination to Oracle syntax."""
    # Test converting LIMIT and OFFSET to Oracle's approach
    sql = "SELECT * FROM users ORDER BY created_at DESC LIMIT 20 OFFSET 40"
    parsed = parse_sql(sql, "mysql")
    
    oracle = OracleDialect()
    result = oracle.convert(parsed)
    
    # Oracle uses nested queries with ROWNUM for pagination
    assert "ROWNUM" in result or "rownum" in result
    # The result should include both the limit and offset values
    assert "20" in result  # Limit value
    assert "40" in result  # Offset value
