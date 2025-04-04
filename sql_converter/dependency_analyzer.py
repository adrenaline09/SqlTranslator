"""
SQL Dependency Analyzer module.

This module analyzes SQL statements to extract table dependencies and determine
the correct order for table creation or importation, using the sqlglot library
for SQL parsing with optimizations for handling large volumes of queries.
"""

import logging
import re
import time
from typing import Dict, List, Set, Tuple, Optional, Iterator
from collections import defaultdict, deque
import sqlglot
from sqlglot.optimizer import optimize, qualify
from sqlglot.expressions import Table, Create, Select, Insert, Update, Delete, CTE, With, Join, Expression

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQLDependencyAnalyzer:
    """
    Analyzes SQL queries to extract table dependencies and determine the correct sequence
    for table creation or importing, using sqlglot for SQL parsing.
    """
    
    def __init__(self):
        """Initialize the SQL Dependency Analyzer."""
        self.dialect = 'sqlite'  # Default dialect for parsing
        
    def _get_table_references(self, expression: Expression) -> Set[str]:
        """
        Extract all table references from a SQL expression.
        
        Args:
            expression (Expression): A sqlglot expression object
            
        Returns:
            Set[str]: A set of all referenced table names (converted to lowercase for case-insensitive comparison)
        """
        tables = set()
        
        # Get tables directly from the expression using sqlglot's built-in functions
        for table in expression.find_all(Table):
            if hasattr(table, 'name'):
                if hasattr(table, 'db') and table.db:
                    # Convert table names to lowercase for case-insensitive comparison
                    table_name = f"{table.db.lower()}.{table.name.lower()}"
                else:
                    # Convert table name to lowercase for case-insensitive comparison
                    table_name = table.name.lower()
                tables.add(table_name)
            
        return tables
        
    def _extract_tables_from_query(self, query: str) -> Tuple[Set[str], Set[str]]:
        """
        Extract created and referenced tables from a SQL query using sqlglot.
        
        Args:
            query (str): The SQL query to analyze
            
        Returns:
            Tuple[Set[str], Set[str]]: A tuple containing sets of created tables and referenced tables
        """
        created_tables = set()
        referenced_tables = set()
        cte_tables = set()
        
        try:
            # Parse the SQL query
            expressions = sqlglot.parse(query, read=self.dialect)
            
            for expression in expressions:
                # Get CTEs/WITH clause tables to exclude them from dependencies
                for cte in expression.find_all(CTE):
                    if hasattr(cte, 'alias') and hasattr(cte.alias, 'name'):
                        cte_tables.add(cte.alias.name.lower())
                
                # Extract created tables (CREATE TABLE/VIEW)
                if isinstance(expression, Create):
                    table = expression.this
                    if table and hasattr(table, 'name'):
                        # Convert table names to lowercase for case-insensitive comparison
                        if hasattr(table, 'db') and table.db:
                            table_name = f"{table.db.lower()}.{table.name.lower()}"
                        else:
                            table_name = table.name.lower()
                        created_tables.add(table_name)
                
                # Extract referenced tables
                tables = self._get_table_references(expression)
                
                # Filter out CTE tables which aren't real database tables
                tables = {t.lower() for t in tables if t.lower() not in cte_tables}
                
                # Add to referenced tables
                referenced_tables.update(tables)
            
            # Remove created tables from referenced tables (to avoid self-references)
            referenced_tables = referenced_tables - created_tables
            
        except Exception as e:
            logger.warning(f"Error parsing SQL query with sqlglot: {e}")
            logger.warning(f"Query: {query}")
            # Fall back to empty sets if parsing fails
            
        return created_tables, referenced_tables
    
    def _build_dependency_graph(self, queries: List[str]) -> Dict[str, Set[str]]:
        """
        Build a dependency graph from a list of SQL queries.
        
        Args:
            queries (List[str]): The list of SQL queries to analyze
            
        Returns:
            Dict[str, Set[str]]: A dependency graph where keys are table names and values are
                                sets of tables that they depend on
        """
        # Maps each table to the tables it depends on
        graph = defaultdict(set)
        
        # Keep track of all tables discovered (both created and referenced)
        all_tables = set()
        
        # First pass: Identify all tables created by the queries
        for query in queries:
            created, referenced = self._extract_tables_from_query(query)
            
            for table in created:
                all_tables.add(table)
            
            for table in referenced:
                all_tables.add(table)
                
            # Add dependencies
            for created_table in created:
                for ref_table in referenced:
                    graph[created_table].add(ref_table)
        
        # Add tables with no dependencies (standalone tables)
        for table in all_tables:
            if table not in graph:
                graph[table] = set()
        
        return graph
    
    def _topological_sort(self, graph: Dict[str, Set[str]]) -> List[str]:
        """
        Perform a topological sort on the dependency graph to determine the order
        in which tables should be created or imported.
        
        Args:
            graph (Dict[str, Set[str]]): The dependency graph
            
        Returns:
            List[str]: A list of table names in the order they should be created
        """
        # Count incoming edges for each node
        in_degree = {node: 0 for node in graph}
        for node in graph:
            for dependent in graph[node]:
                if dependent in in_degree:
                    in_degree[dependent] += 1
        
        # Queue nodes with no dependencies
        queue = deque([node for node in in_degree if in_degree[node] == 0])
        
        result = []
        
        # Process nodes
        while queue:
            node = queue.popleft()
            result.append(node)
            
            # Remove edges from this node
            for dependent in list(graph.keys()):
                if node in graph[dependent]:
                    graph[dependent].remove(node)
                    in_degree[dependent] -= 1
                    
                    # If the dependent now has no dependencies, add it to the queue
                    if in_degree[dependent] == 0:
                        queue.append(dependent)
        
        # Check for cycles
        if len(result) != len(graph):
            logger.warning("Circular dependencies detected in SQL queries")
            
            # Include remaining nodes (they form cycles)
            cycles = [node for node in graph if node not in result]
            logger.warning(f"Tables in cycles: {', '.join(cycles)}")
            
            # Add them to the result anyway
            result.extend(cycles)
        
        return result
    
    def analyze_queries(self, queries: List[str]) -> List[str]:
        """
        Analyze a list of SQL queries and determine the order in which tables
        should be created or imported.
        
        Args:
            queries (List[str]): The list of SQL queries to analyze
            
        Returns:
            List[str]: A list of table names in the order they should be created or imported
        """
        # Build the dependency graph
        graph = self._build_dependency_graph(queries)
        
        # Perform topological sort to get the order
        return self._topological_sort(graph)
    
    def split_batch_queries(self, sql_batch: str) -> List[str]:
        """
        Split a batch of SQL queries into individual queries using a combination of
        methods for improved reliability with large query sets.
        
        Args:
            sql_batch (str): A string containing multiple SQL queries separated by semicolons
            
        Returns:
            List[str]: A list of individual SQL queries
        """
        # Start timing for performance monitoring with large batches
        start_time = time.time()
        
        # For very large batches, we'll use a more efficient regex-based approach first
        if len(sql_batch) > 1000000:  # 1MB+ batches get special treatment
            logger.info(f"Large SQL batch detected ({len(sql_batch)/1000000:.2f}MB). Using optimized processing.")
            
            # Use regex to extract create statements first - most important for dependencies
            create_pattern = re.compile(r'CREATE\s+(OR\s+REPLACE\s+)?(TABLE|VIEW|MATERIALIZED\s+VIEW)\s+([^\s(;]+)', 
                                       re.IGNORECASE)
            
            # Pre-process to handle some common Oracle syntax that sqlglot might struggle with
            clean_batch = re.sub(r'NOLOGGING', '', sql_batch, flags=re.IGNORECASE)
            clean_batch = re.sub(r'PARALLEL \d+', '', clean_batch, flags=re.IGNORECASE)
            
            # Process in chunks to reduce memory usage
            return self._process_large_batch(clean_batch)
        
        # Regular approach for normal-sized batches
        try:
            # Try using sqlglot to parse the queries
            parsed_queries = sqlglot.parse(sql_batch, read=self.dialect)
            queries = [str(q) for q in parsed_queries]
            
            # If sqlglot parsed something successfully, return the results
            if queries:
                logger.info(f"Successfully parsed {len(queries)} queries with sqlglot in {time.time() - start_time:.2f}s")
                return queries
                
            # If sqlglot couldn't parse anything but there is content, try regex-based fallback
            if sql_batch.strip():
                logger.warning("sqlglot couldn't parse the batch. Falling back to regex-based parsing.")
                return self._split_with_regex(sql_batch)
                
            return []
            
        except Exception as e:
            logger.warning(f"Error splitting SQL batch with sqlglot: {e}")
            return self._split_with_regex(sql_batch)
    
    def _split_with_regex(self, sql_batch: str) -> List[str]:
        """
        Split SQL batch using regex pattern matching, which is more tolerant of syntax errors
        than sqlglot parsing but less accurate for complex queries.
        
        Args:
            sql_batch (str): SQL batch to split
            
        Returns:
            List[str]: List of individual SQL queries
        """
        # Replace comments with empty strings
        sql_batch = re.sub(r'--.*?$', '', sql_batch, flags=re.MULTILINE)
        sql_batch = re.sub(r'/\*.*?\*/', '', sql_batch, flags=re.DOTALL)
        
        # Use regex to split on semicolons that are not inside quotes or parentheses
        queries = []
        current_query = ""
        in_quotes = False
        quote_char = None
        paren_level = 0
        
        for char in sql_batch:
            current_query += char
            
            if char in ["'", '"']:
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif char == quote_char:
                    in_quotes = False
                    quote_char = None
            elif char == '(' and not in_quotes:
                paren_level += 1
            elif char == ')' and not in_quotes:
                paren_level = max(0, paren_level - 1)  # Ensure we don't go negative
            elif char == ';' and not in_quotes and paren_level == 0:
                # End of a query - add it to the list and reset current_query
                if current_query.strip():
                    queries.append(current_query.strip())
                current_query = ""
        
        # Add the last query if it exists
        if current_query.strip():
            queries.append(current_query.strip())
            
        logger.info(f"Split SQL batch into {len(queries)} queries using regex")
        return queries
        
    def _process_large_batch(self, sql_batch: str) -> List[str]:
        """
        Process a very large SQL batch in a memory-efficient way by streaming through it.
        
        Args:
            sql_batch (str): The large SQL batch to process
            
        Returns:
            List[str]: The extracted SQL queries
        """
        logger.info("Processing large batch using streaming approach")
        
        # First try a simple semicolon split to get a rough estimate of query count
        quick_count = len(sql_batch.split(';'))
        logger.info(f"Quick estimate: batch contains approximately {quick_count} queries")
        
        # If the batch is very large, use the streaming regex approach
        if quick_count > 1000:
            logger.info("Very large batch detected, using memory-efficient processing")
            return self._stream_split_queries(sql_batch)
        else:
            # For moderately large batches, the standard regex approach is fine
            return self._split_with_regex(sql_batch)
            
    def _stream_split_queries(self, sql_batch: str) -> List[str]:
        """
        Stream through a large SQL batch to split it into individual queries
        without loading everything into memory at once.
        
        Args:
            sql_batch (str): The SQL batch to split
            
        Returns:
            List[str]: List of individual SQL queries
        """
        # Process in chunks to avoid memory issues
        chunk_size = 10000  # characters
        queries = []
        buffer = ""
        
        # Split the SQL into chunks
        chunks = [sql_batch[i:i+chunk_size] for i in range(0, len(sql_batch), chunk_size)]
        
        for chunk in chunks:
            buffer += chunk
            
            # Look for query boundaries (semicolons outside of quotes/comments)
            while True:
                # Find the next semicolon
                semicolon_pos = buffer.find(';')
                
                if semicolon_pos == -1:
                    # No more semicolons in this buffer
                    break
                    
                # Extract potential query
                query = buffer[:semicolon_pos+1]
                buffer = buffer[semicolon_pos+1:]
                
                # Check if this is actually a query end (not in a comment, string, etc.)
                # This is a simplified check; you might need more complex logic
                # depending on your SQL dialect
                if self._is_valid_query_boundary(query):
                    if query.strip():
                        queries.append(query.strip())
        
        # Add any remaining content in the buffer as the final query
        if buffer.strip():
            queries.append(buffer.strip())
            
        logger.info(f"Split large SQL batch into {len(queries)} queries using streaming approach")
        return queries
        
    def _is_valid_query_boundary(self, query: str) -> bool:
        """
        Check if a query string ends at a valid boundary (semicolon not in a string or comment).
        
        Args:
            query (str): The query string to check
            
        Returns:
            bool: True if the query ends at a valid boundary
        """
        # Simple check: make sure quotes and parentheses are balanced
        quote_count = 0
        paren_level = 0
        in_comment = False
        in_block_comment = False
        
        for i, char in enumerate(query):
            if in_comment:
                if char == '\n':
                    in_comment = False
                continue
                
            if in_block_comment:
                if char == '*' and i+1 < len(query) and query[i+1] == '/':
                    in_block_comment = False
                continue
                
            if char == '-' and i+1 < len(query) and query[i+1] == '-':
                in_comment = True
                continue
                
            if char == '/' and i+1 < len(query) and query[i+1] == '*':
                in_block_comment = True
                continue
                
            if char in ["'", '"']:
                quote_count += 1
                
            elif char == '(':
                paren_level += 1
                
            elif char == ')':
                paren_level = max(0, paren_level - 1)
        
        # If quotes are paired (even count) and parentheses are balanced, it's valid
        return quote_count % 2 == 0 and paren_level == 0
    
    def analyze_batch(self, sql_batch: str) -> Dict:
        """
        Analyze a batch of SQL queries and return detailed dependency information.
        Optimized to handle large batches with thousands of queries.
        
        Args:
            sql_batch (str): A string containing multiple SQL queries
            
        Returns:
            Dict: A dictionary containing table dependencies and ordering information
        """
        start_time = time.time()
        logger.info("Starting dependency analysis of SQL batch")
        
        # Split batch into individual queries
        queries = self.split_batch_queries(sql_batch)
        logger.info(f"Split batch into {len(queries)} individual queries in {time.time() - start_time:.2f}s")
        
        # For very large batches, use the optimized approach
        if len(queries) > 1000:
            logger.info(f"Large number of queries detected ({len(queries)}). Using optimized analysis.")
            return self._analyze_large_batch(queries)
        
        # Regular analysis for normal-sized batches
        table_extraction_start = time.time()
        
        # Extract tables from each query
        query_tables = []
        all_created_tables = set()
        all_referenced_tables = set()
        
        # Process in chunks to avoid excessive memory usage
        chunk_size = 100  # Process 100 queries at a time
        for i in range(0, len(queries), chunk_size):
            chunk = queries[i:i+chunk_size]
            
            # Process this chunk
            for query in chunk:
                created, referenced = self._extract_tables_from_query(query)
                query_tables.append({
                    "query": query,
                    "creates": list(created),
                    "references": list(referenced)
                })
                
                all_created_tables.update(created)
                all_referenced_tables.update(referenced)
                
            # Log progress for large batches
            if i % 500 == 0 and i > 0:
                logger.info(f"Processed {i}/{len(queries)} queries...")
        
        logger.info(f"Extracted tables from all queries in {time.time() - table_extraction_start:.2f}s")
        
        # Find tables that are referenced but never created
        external_dependencies = all_referenced_tables - all_created_tables
        
        # Build the dependency graph and get table creation order
        dependency_start = time.time()
        table_order = self.analyze_queries(queries)
        logger.info(f"Built dependency graph and sorted tables in {time.time() - dependency_start:.2f}s")
        
        total_time = time.time() - start_time
        logger.info(f"Total dependency analysis time: {total_time:.2f}s for {len(queries)} queries")
        
        return {
            "table_creation_order": table_order,
            "external_dependencies": list(external_dependencies),
            "query_details": query_tables,
            "total_queries": len(queries),
            "total_tables": len(all_created_tables) + len(external_dependencies),
            "analysis_time_seconds": round(total_time, 2)
        }
        
    def _analyze_large_batch(self, queries: List[str]) -> Dict:
        """
        Specialized method for analyzing very large batches of queries.
        This uses a more memory-efficient approach by focusing primarily on CREATE statements
        and doing minimal detailed analysis of other statements.
        
        Args:
            queries (List[str]): List of SQL queries to analyze
            
        Returns:
            Dict: A dictionary containing dependency information
        """
        start_time = time.time()
        
        # First pass: focus on extracting CREATE TABLE statements as they're most important for dependencies
        create_statements = []
        other_statements = []
        
        # Identify CREATE statements first
        for query in queries:
            if re.search(r'^\s*CREATE\s', query, re.IGNORECASE):
                create_statements.append(query)
            else:
                other_statements.append(query)
                
        logger.info(f"Identified {len(create_statements)} CREATE statements out of {len(queries)} total queries")
        
        # Process CREATE statements to build table list
        all_created_tables = set()
        create_details = []
        
        for query in create_statements:
            created, referenced = self._extract_tables_from_query(query)
            create_details.append({
                "query": query,
                "creates": list(created),
                "references": list(referenced)
            })
            all_created_tables.update(created)
        
        logger.info(f"Processed {len(create_statements)} CREATE statements, found {len(all_created_tables)} tables")
        
        # For other statements, process in batches to extract references
        all_referenced_tables = set()
        other_details = []
        
        # Process in chunks to avoid excessive memory usage
        chunk_size = 200  # Process more queries at a time for non-CREATE statements
        for i in range(0, len(other_statements), chunk_size):
            chunk = other_statements[i:i+chunk_size]
            
            for query in chunk:
                _, referenced = self._extract_tables_from_query(query)
                all_referenced_tables.update(referenced)
                
                # Only keep detailed info for first 1000 queries to avoid excessive memory usage
                if len(other_details) < 1000:
                    other_details.append({
                        "query": query,
                        "creates": [],
                        "references": list(referenced)
                    })
            
            # Log progress for large batches
            if i % 1000 == 0 and i > 0:
                logger.info(f"Processed {i}/{len(other_statements)} non-CREATE queries...")
        
        # Combine query details, prioritizing CREATE statements
        query_tables = create_details
        if len(other_details) > 0:
            # Add a limited number of non-CREATE statements to avoid excessive result size
            query_tables.extend(other_details[:min(1000, len(other_details))])
            
            if len(other_details) > 1000:
                logger.info(f"Note: Only including details for first 1000 non-CREATE queries in results")
        
        # Find tables that are referenced but never created
        external_dependencies = all_referenced_tables - all_created_tables
        
        # Build the dependency graph focusing on CREATE statements for efficiency
        # but including critical dependencies from other statements
        dependency_start = time.time()
        
        # We'll use create statements plus a subset of other statements that reference tables
        # created in the batch to build the dependency graph
        critical_queries = create_statements.copy()
        
        # Add a subset of other statements that reference created tables
        # to ensure we capture important dependencies
        for query in other_statements[:1000]:  # Limit to first 1000 to avoid excessive processing
            _, referenced = self._extract_tables_from_query(query)
            if referenced.intersection(all_created_tables):
                critical_queries.append(query)
        
        table_order = self.analyze_queries(critical_queries)
        logger.info(f"Built dependency graph and sorted tables in {time.time() - dependency_start:.2f}s")
        
        total_time = time.time() - start_time
        logger.info(f"Total large batch analysis time: {total_time:.2f}s for {len(queries)} queries")
        
        return {
            "table_creation_order": table_order,
            "external_dependencies": list(external_dependencies),
            "query_details": query_tables,
            "total_queries": len(queries),
            "total_tables": len(all_created_tables) + len(external_dependencies),
            "created_tables": len(all_created_tables),
            "analysis_time_seconds": round(total_time, 2),
            "note": "Large batch optimization applied: detailed query analysis limited to CREATE statements and critical dependencies."
        }


def analyze_sql_batch(sql_batch: str) -> Dict:
    """
    Analyze a batch of SQL queries to determine table dependencies using sqlglot.
    
    Args:
        sql_batch (str): A string containing multiple SQL queries
        
    Returns:
        Dict: A dictionary containing table dependencies and ordering information
    """
    analyzer = SQLDependencyAnalyzer()
    return analyzer.analyze_batch(sql_batch)