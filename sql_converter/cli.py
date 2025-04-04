"""
Command-line interface for SQL Converter.

This module provides a CLI for converting SQL between different dialects.
"""

import argparse
import sys
import logging
from typing import List, Optional

from .api import convert_sql, get_supported_dialects

def setup_parser() -> argparse.ArgumentParser:
    """
    Set up the argument parser for the CLI.
    
    Returns:
        argparse.ArgumentParser: Configured argument parser
    """
    parser = argparse.ArgumentParser(
        description='Convert SQL queries between different database dialects.',
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        '-s', '--source',
        type=str,
        required=True,
        choices=get_supported_dialects(),
        help='Source SQL dialect'
    )
    
    parser.add_argument(
        '-t', '--target',
        type=str,
        required=True,
        choices=get_supported_dialects(),
        help='Target SQL dialect'
    )
    
    input_group = parser.add_mutually_exclusive_group(required=True)
    
    input_group.add_argument(
        '-q', '--query',
        type=str,
        help='SQL query to convert'
    )
    
    input_group.add_argument(
        '-f', '--file',
        type=str,
        help='File containing SQL query to convert'
    )
    
    parser.add_argument(
        '-o', '--output',
        type=str,
        help='Output file to save the converted SQL query (default: stdout)'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    return parser

def main(args: Optional[List[str]] = None) -> int:
    """
    Main entry point for the CLI.
    
    Args:
        args (List[str], optional): Command-line arguments. Defaults to None.
        
    Returns:
        int: Exit code
    """
    parser = setup_parser()
    parsed_args = parser.parse_args(args)
    
    # Set up logging
    log_level = logging.DEBUG if parsed_args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format='%(message)s')
    logger = logging.getLogger(__name__)
    
    try:
        # Get the SQL query
        if parsed_args.query:
            sql = parsed_args.query
        else:
            try:
                with open(parsed_args.file, 'r') as f:
                    sql = f.read()
            except Exception as e:
                logger.error(f"Failed to read SQL from file: {e}")
                return 1
        
        # Convert the SQL
        converted_sql = convert_sql(sql, parsed_args.source, parsed_args.target)
        
        # Output the result
        if parsed_args.output:
            try:
                with open(parsed_args.output, 'w') as f:
                    f.write(converted_sql)
                logger.info(f"Converted SQL written to {parsed_args.output}")
            except Exception as e:
                logger.error(f"Failed to write to output file: {e}")
                return 1
        else:
            print(converted_sql)
        
        return 0
    
    except Exception as e:
        logger.error(f"Error: {e}")
        if parsed_args.verbose:
            logger.exception("Detailed error information:")
        return 1

if __name__ == "__main__":
    sys.exit(main())
