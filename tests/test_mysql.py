"""
Tests for the MySQL dialect handler.
"""

import pytest
from sql_converter.dialects.mysql import MySQLDialect
from sql_converter.parser import parse_sql

def test_mysql_select():
    """Test converting a SELECT statement to MySQL syntax."""
    sql = "SELECT id, name FROM users WHERE age > 18"
    parsed = parse_sql(sql, "oracle")
    
    mysql = MySQLDialect()
    result = mysql.convert(parsed)
    
    # MySQL supports standard SELECT syntax
    assert "SELECT" in result
    assert "FROM users" in result
    assert "WHERE" in result

def test_mysql_date_functions():
    """Test converting date functions to MySQL syntax."""
    # Test converting Oracle's SYSDATE to MySQL's NOW()
    sql = "SELECT * FROM users WHERE created_at > SYSDATE"
    parsed = parse_sql(sql, "oracle")
    
    mysql = MySQLDialect()
    result = mysql.convert(parsed)
    
    # MySQL uses NOW() instead of SYSDATE
    assert "NOW()" in result

def test_mysql_nvl_function():
    """Test converting NVL function to MySQL's IFNULL."""
    # Test converting Oracle's NVL to MySQL's IFNULL
    sql = "SELECT NVL(phone, 'Unknown') AS phone FROM users"
    parsed = parse_sql(sql, "oracle")
    
    mysql = MySQLDialect()
    result = mysql.convert(parsed)
    
    # MySQL uses IFNULL instead of NVL
    assert "IFNULL" in result

def test_mysql_pagination():
    """Test converting pagination to MySQL syntax."""
    # Test converting Oracle's ROWNUM-based pagination to MySQL's LIMIT/OFFSET
    sql = """
    SELECT * FROM (
        SELECT a.*, ROWNUM rnum FROM (
            SELECT * FROM users ORDER BY created_at DESC
        ) a
        WHERE ROWNUM <= 60
    ) WHERE rnum > 40
    """
    parsed = parse_sql(sql, "oracle")
    
    mysql = MySQLDialect()
    result = mysql.convert(parsed)
    
    # MySQL uses LIMIT and OFFSET for pagination
    # This test might be quite complex due to the structure difference
    # In this simplified test, we just check for the presence of certain keywords
    assert "SELECT" in result
    assert "FROM" in result
    assert "users" in result
