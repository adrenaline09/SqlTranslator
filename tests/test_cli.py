"""
Tests for the command-line interface.
"""

import pytest
import tempfile
import os
from sql_converter.cli import main

def test_cli_query_argument():
    """Test the CLI with a query argument."""
    # Test converting a simple query from MySQL to PostgreSQL
    args = [
        "-s", "mysql",
        "-t", "postgresql",
        "-q", "SELECT * FROM users LIMIT 10"
    ]
    
    # Shouldn't raise an exception
    exit_code = main(args)
    assert exit_code == 0

def test_cli_file_argument():
    """Test the CLI with a file argument."""
    # Create a temporary file with a SQL query
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as f:
        f.write("SELECT * FROM users LIMIT 10")
        temp_filename = f.name
    
    try:
        # Test converting the file from MySQL to PostgreSQL
        args = [
            "-s", "mysql",
            "-t", "postgresql",
            "-f", temp_filename
        ]
        
        # Shouldn't raise an exception
        exit_code = main(args)
        assert exit_code == 0
    finally:
        # Clean up
        os.unlink(temp_filename)

def test_cli_output_file():
    """Test the CLI with an output file."""
    # Create a temporary output file
    fd, temp_output = tempfile.mkstemp()
    os.close(fd)
    
    try:
        # Test converting a query and saving to the output file
        args = [
            "-s", "mysql",
            "-t", "postgresql",
            "-q", "SELECT * FROM users LIMIT 10",
            "-o", temp_output
        ]
        
        # Shouldn't raise an exception
        exit_code = main(args)
        assert exit_code == 0
        
        # Check that the output file was created and has content
        with open(temp_output, 'r') as f:
            content = f.read()
            assert content.strip() != ""
    finally:
        # Clean up
        os.unlink(temp_output)

def test_cli_invalid_dialect():
    """Test the CLI with an invalid dialect."""
    args = [
        "-s", "invalid",
        "-t", "mysql",
        "-q", "SELECT * FROM users"
    ]
    
    # Should fail with a non-zero exit code
    exit_code = main(args)
    assert exit_code != 0

def test_cli_missing_required_args():
    """Test the CLI with missing required arguments."""
    # Missing source dialect
    args = [
        "-t", "mysql",
        "-q", "SELECT * FROM users"
    ]
    
    # Should fail with a non-zero exit code or raise SystemExit
    with pytest.raises(SystemExit):
        main(args)
