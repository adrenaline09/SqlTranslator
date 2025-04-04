"""
AI-Powered SQL Query Optimization module.

This module provides AI-based SQL query optimization suggestions
using language models. The feature is optional and only activated
when the user provides their own API key.
"""

import os
import logging
from typing import Dict, List, Union

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AIOptimizer:
    """
    AI-based SQL query optimizer that provides suggestions for improving query performance.
    This is an optional feature that requires an API key to be provided by the user.
    """
    
    def __init__(self, api_key: str = None):
        """
        Initialize the AI optimizer.
        
        Args:
            api_key (str, optional): API key for OpenAI. If provided, it overrides the environment variable.
        """
        self.openai_api_key = api_key or os.environ.get('OPENAI_API_KEY')
        
    def _check_availability(self) -> bool:
        """
        Check if the AI optimization feature is available based on API key presence.
        
        Returns:
            bool: True if the feature is available, False otherwise
        """
        return self.openai_api_key is not None and len(self.openai_api_key) > 0
        
    def get_optimization_suggestions(self, sql: str, dialect: str, api_key: str = None) -> Dict[str, Union[bool, str, List[Dict[str, str]]]]:
        """
        Get optimization suggestions for the given SQL query.
        
        Args:
            sql (str): The SQL query to analyze and optimize
            dialect (str): The SQL dialect of the query (e.g., 'mysql', 'postgresql', 'oracle')
            
        Returns:
            Dict: A dictionary containing optimization results with fields:
                - available (bool): Whether the AI optimization feature is available
                - message (str): A message about the availability status
                - suggestions (List[Dict]): A list of optimization suggestions if available
        """
        # First check if the feature is available
        if not self._check_availability():
            return {
                "available": False,
                "message": "AI optimization feature requires an OpenAI API key. "
                          "Please set the OPENAI_API_KEY environment variable.",
                "suggestions": []
            }
            
        try:
            # Import OpenAI here to avoid errors if it's not installed
            import openai
            client = openai.OpenAI(api_key=self.openai_api_key)
            
            # Prepare the prompt for SQL optimization
            prompt = f"""
            You are an expert SQL tuning consultant specializing in {dialect} SQL dialect.
            Analyze the following SQL query and suggest optimizations:

            ```sql
            {sql}
            ```

            Provide a detailed analysis focusing on:
            1. Indexing opportunities
            2. Query structure improvements
            3. Performance bottlenecks
            4. Rewrite suggestions if applicable
            
            Format your response as a JSON array of objects, each with these fields:
            - title: A short title for the suggestion
            - description: A detailed explanation of the optimization
            - impact: "High", "Medium", or "Low" based on expected improvement
            - example: An example of the optimized code (if applicable)
            """
            
            # Call the OpenAI API to get optimization suggestions
            response = client.chat.completions.create(
                model="gpt-4o",  # Using the latest model
                messages=[
                    {"role": "system", "content": "You are an expert SQL optimization assistant."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                max_tokens=2000
            )
            
            # Extract suggestions from the response
            suggestions_text = response.choices[0].message.content.strip()
            import json
            suggestions = json.loads(suggestions_text)
            
            if not isinstance(suggestions, list):
                if "suggestions" in suggestions:
                    # Handle if the model returned {"suggestions": [...]}
                    suggestions = suggestions.get("suggestions", [])
                else:
                    # If it's not a list and doesn't have a suggestions field, return empty list
                    suggestions = []
            
            return {
                "available": True,
                "message": "AI optimization suggestions are available.",
                "suggestions": suggestions
            }
            
        except Exception as e:
            logger.error(f"Error getting AI optimization suggestions: {e}")
            return {
                "available": True,  # The feature is available but there was an error
                "message": f"Error getting optimization suggestions: {str(e)}",
                "suggestions": []
            }

def get_optimization_status(api_key: str = None) -> Dict[str, Union[bool, str]]:
    """
    Get the status of the AI optimization feature.
    
    Args:
        api_key (str, optional): API key for OpenAI. If provided, it overrides the environment variable.
    
    Returns:
        Dict: A dictionary containing the status information
    """
    try:
        # Import OpenAI here to avoid errors if it's not installed
        import openai
        # Use provided API key or fall back to environment variable
        key = api_key or os.environ.get('OPENAI_API_KEY')
        
        if key is None or len(key) == 0:
            return {
                "available": False,
                "message": "AI optimization feature requires an OpenAI API key. "
                          "Please provide an API key or set the OPENAI_API_KEY environment variable."
            }
        
        return {
            "available": True,
            "message": "AI optimization feature is available."
        }
    except ImportError:
        return {
            "available": False,
            "message": "OpenAI package is not installed. "
                     "Please install it using 'pip install openai'."
        }
    except Exception as e:
        logger.error(f"Error checking AI optimization availability: {e}")
        return {
            "available": False,
            "message": f"Error checking availability: {str(e)}"
        }

def optimize_sql_query(sql: str, dialect: str, api_key: str = None) -> Dict[str, Union[bool, str, List[Dict[str, str]]]]:
    """
    Get AI-powered optimization suggestions for a SQL query.
    
    Args:
        sql (str): The SQL query to optimize
        dialect (str): The SQL dialect of the query
        api_key (str, optional): API key for OpenAI. If provided, it overrides the environment variable.
        
    Returns:
        Dict: A dictionary containing optimization results
    """
    optimizer = AIOptimizer(api_key=api_key)
    return optimizer.get_optimization_suggestions(sql, dialect)