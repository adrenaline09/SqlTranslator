"""
Tests for the Python API.
"""

import pytest
from sql_converter.api import convert_sql, batch_convert_sql, get_supported_dialects

def test_api_convert_sql():
    """Test the convert_sql API function."""
    # Test converting a simple query from MySQL to PostgreSQL
    sql = "SELECT * FROM users LIMIT 10"
    result = convert_sql(sql, "mysql", "postgresql")
    
    # Should return a non-empty string
    assert isinstance(result, str)
    assert result != ""
    
    # Basic check of the result
    assert "SELECT" in result
    assert "FROM users" in result
    assert "LIMIT" in result

def test_api_batch_convert_sql():
    """Test the batch_convert_sql API function."""
    # Test batch converting multiple queries
    queries = [
        "SELECT * FROM users LIMIT 10",
        "SELECT id, name FROM products WHERE price > 100"
    ]
    
    results = batch_convert_sql(queries, "mysql", "postgresql")
    
    # Should return a list of the same length
    assert isinstance(results, list)
    assert len(results) == len(queries)
    
    # Basic check of each result
    for result in results:
        assert isinstance(result, str)
        assert result != ""
        assert "SELECT" in result

def test_api_get_supported_dialects():
    """Test the get_supported_dialects API function."""
    dialects = get_supported_dialects()
    
    # Should return a non-empty list
    assert isinstance(dialects, list)
    assert len(dialects) > 0
    
    # Should include our main dialects
    expected_dialects = ["oracle", "pyspark", "mysql", "postgresql"]
    for dialect in expected_dialects:
        assert dialect in dialects

def test_api_invalid_dialect():
    """Test the API with an invalid dialect."""
    sql = "SELECT * FROM users"
    
    with pytest.raises(ValueError):
        convert_sql(sql, "invalid", "mysql")
    
    with pytest.raises(ValueError):
        convert_sql(sql, "mysql", "invalid")

def test_api_batch_convert_with_errors():
    """Test batch conversion with some queries causing errors."""
    queries = [
        "SELECT * FROM users LIMIT 10",  # Valid query
        "NOT A VALID SQL QUERY"  # Invalid query
    ]
    
    # Should still return results for valid queries
    results = batch_convert_sql(queries, "mysql", "postgresql")
    
    assert len(results) == len(queries)
    assert results[0] is not None  # First query should convert successfully
