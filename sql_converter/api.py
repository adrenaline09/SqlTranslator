"""
Python API for SQL Converter.

This module provides a Python API for converting SQL between different dialects
and optional AI-powered optimization suggestions.
"""

import logging
from typing import List, Dict, Any, Optional

from .converter import SQLConverter
from .dialects import get_supported_dialects as get_dialects

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def convert_sql(sql: str, source_dialect: str, target_dialect: str, custom_removals: Optional[List[str]] = None) -> str:
    """
    Convert SQL from one dialect to another.
    
    Args:
        sql (str): The SQL query to convert
        source_dialect (str): The source SQL dialect
        target_dialect (str): The target SQL dialect
        custom_removals (List[str], optional): List of characters or words to be removed from the SQL query.
            Can include both exact strings or regex patterns. Defaults to None.
        
    Returns:
        str: The converted SQL query
        
    Raises:
        ValueError: If an unsupported dialect is specified
    """
    try:
        # Use the enhanced converter from simple_converter module which provides better cleanup
        # and uses sqlglot for conversion with fallback to regex-based conversion
        from .simple_converter import convert_sql as enhanced_convert
        
        try:
            return enhanced_convert(sql, source_dialect, target_dialect, custom_removals)
        except Exception as e:
            logger.warning(f"Enhanced conversion failed: {e}. Falling back to standard converter.")
            
        # Fall back to standard converter if enhanced converter fails
        converter = SQLConverter()
        return converter.convert(sql, source_dialect, target_dialect)
    except Exception as e:
        logger.error(f"Error converting SQL: {e}")
        raise

def batch_convert_sql(queries: List[str], source_dialect: str, target_dialect: str, custom_removals: Optional[List[str]] = None) -> List[str]:
    """
    Convert multiple SQL queries from one dialect to another.
    
    Args:
        queries (List[str]): The SQL queries to convert
        source_dialect (str): The source SQL dialect
        target_dialect (str): The target SQL dialect
        custom_removals (List[str], optional): List of characters or words to be removed from the SQL queries.
            Can include both exact strings or regex patterns. Defaults to None.
        
    Returns:
        List[str]: The converted SQL queries
        
    Raises:
        ValueError: If an unsupported dialect is specified
    """
    try:
        # Use the enhanced batch converter from simple_converter module
        from .simple_converter import batch_convert as enhanced_batch_convert
        
        try:
            return enhanced_batch_convert(queries, source_dialect, target_dialect, custom_removals)
        except Exception as e:
            logger.warning(f"Enhanced batch conversion failed: {e}. Falling back to standard converter.")
        
        # Fall back to standard converter if enhanced converter fails
        converter = SQLConverter()
        return converter.batch_convert(queries, source_dialect, target_dialect)
    except Exception as e:
        logger.error(f"Error in batch conversion: {e}")
        raise

def get_supported_dialects() -> List[str]:
    """
    Get a list of supported SQL dialects.
    
    Returns:
        List[str]: List of supported dialect names
    """
    return get_dialects()

def get_optimization_status() -> Dict[str, Any]:
    """
    Get the status of the AI-powered SQL optimization feature.
    
    Returns:
        Dict[str, Any]: A dictionary containing the feature availability status
    """
    try:
        from .ai_optimizer import get_optimization_status as get_status
        return get_status()
    except ImportError:
        return {
            "available": False,
            "message": "AI optimization module not available"
        }
    except Exception as e:
        logger.error(f"Error checking optimization status: {e}")
        return {
            "available": False,
            "message": f"Error checking optimization status: {str(e)}"
        }

def get_optimization_suggestions(sql: str, dialect: str) -> Dict[str, Any]:
    """
    Get AI-powered optimization suggestions for the given SQL query.
    This feature is optional and requires an OpenAI API key.
    
    Args:
        sql (str): The SQL query to analyze and optimize
        dialect (str): The SQL dialect of the query
        
    Returns:
        Dict[str, Any]: A dictionary containing optimization results
    """
    try:
        from .ai_optimizer import optimize_sql_query
        return optimize_sql_query(sql, dialect)
    except ImportError:
        return {
            "available": False,
            "message": "AI optimization module not available",
            "suggestions": []
        }
    except Exception as e:
        logger.error(f"Error getting optimization suggestions: {e}")
        return {
            "available": False,
            "message": f"Error getting optimization suggestions: {str(e)}",
            "suggestions": []
        }
