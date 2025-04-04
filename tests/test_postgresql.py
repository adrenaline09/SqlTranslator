"""
Tests for the PostgreSQL dialect handler.
"""

import pytest
from sql_converter.dialects.postgresql import PostgreSQLDialect
from sql_converter.parser import parse_sql

def test_postgresql_select():
    """Test converting a SELECT statement to PostgreSQL syntax."""
    sql = "SELECT id, name FROM users WHERE age > 18"
    parsed = parse_sql(sql, "mysql")
    
    postgres = PostgreSQLDialect()
    result = postgres.convert(parsed)
    
    # PostgreSQL supports standard SELECT syntax
    assert "SELECT" in result
    assert "FROM users" in result
    assert "WHERE" in result

def test_postgresql_date_functions():
    """Test converting date functions to PostgreSQL syntax."""
    # Test converting Oracle's SYSDATE to PostgreSQL's CURRENT_DATE
    sql = "SELECT * FROM users WHERE created_at > SYSDATE"
    parsed = parse_sql(sql, "oracle")
    
    postgres = PostgreSQLDialect()
    result = postgres.convert(parsed)
    
    # PostgreSQL uses CURRENT_DATE instead of SYSDATE
    assert "CURRENT_DATE" in result

def test_postgresql_nvl_function():
    """Test converting NVL function to PostgreSQL's COALESCE."""
    # Test converting Oracle's NVL to PostgreSQL's COALESCE
    sql = "SELECT NVL(phone, 'Unknown') AS phone FROM users"
    parsed = parse_sql(sql, "oracle")
    
    postgres = PostgreSQLDialect()
    result = postgres.convert(parsed)
    
    # PostgreSQL uses COALESCE instead of NVL
    assert "COALESCE" in result

def test_postgresql_returning():
    """Test converting Oracle's RETURNING INTO to PostgreSQL's RETURNING."""
    # Test converting Oracle's RETURNING INTO to PostgreSQL's RETURNING
    sql = "INSERT INTO users (id, name) VALUES (1, 'John') RETURNING id INTO v_id"
    parsed = parse_sql(sql, "oracle")
    
    postgres = PostgreSQLDialect()
    result = postgres.convert(parsed)
    
    # PostgreSQL uses RETURNING without INTO
    assert "RETURNING" in result
    assert "INTO" not in result or "INTO v_id" not in result  # The INTO clause should be removed
