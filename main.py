"""
SQL Converter Web Application.

This script provides a web interface for the SQL converter functionality.
"""

import os
import logging
from flask import Flask, render_template, request, jsonify
from sql_converter.api import (
    convert_sql, 
    get_supported_dialects,
    get_optimization_status,
    get_optimization_suggestions
)
from sql_converter.dependency_analyzer import analyze_sql_batch

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Create the Flask application
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "sql_converter_secret_key")

@app.route('/')
def index():
    """Display the main page."""
    dialects = get_supported_dialects()
    return render_template('index.html', dialects=dialects)

@app.route('/dependency-analyzer')
def dependency_analyzer():
    """Display the SQL dependency analyzer page."""
    return render_template('dependency_analyzer.html')

@app.route('/api/convert', methods=['POST'])
def api_convert():
    """API endpoint to convert SQL between dialects."""
    data = request.json
    
    if not data:
        return jsonify({"error": "No data provided"}), 400
    
    # Get conversion parameters
    sql = data.get('sql', '')
    source_dialect = data.get('source_dialect', '')
    target_dialect = data.get('target_dialect', '')
    custom_removals = data.get('custom_removals', None)
    
    # Validate parameters
    if not sql:
        return jsonify({"error": "SQL query is required"}), 400
    
    dialects = get_supported_dialects()
    if source_dialect not in dialects:
        return jsonify({"error": f"Source dialect '{source_dialect}' is not supported"}), 400
    
    if target_dialect not in dialects:
        return jsonify({"error": f"Target dialect '{target_dialect}' is not supported"}), 400
    
    # Validate custom_removals format if provided
    if custom_removals is not None and not isinstance(custom_removals, list):
        return jsonify({"error": "custom_removals must be a list of strings"}), 400
    
    # Perform conversion
    try:
        converted_sql = convert_sql(sql, source_dialect, target_dialect, custom_removals)
        return jsonify({
            "converted_sql": converted_sql,
            "source_dialect": source_dialect,
            "target_dialect": target_dialect
        })
    except Exception as e:
        logger.error(f"Error converting SQL: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/analyze-dependencies', methods=['POST'])
def api_analyze_dependencies():
    """
    API endpoint to analyze SQL dependencies in a batch of queries.
    This identifies tables and their dependencies to determine the correct creation order.
    """
    data = request.json
    
    if not data:
        return jsonify({"error": "No data provided"}), 400
    
    # Get SQL batch
    sql_batch = data.get('sql_batch', '')
    
    # Validate parameters
    if not sql_batch:
        return jsonify({"error": "SQL batch is required"}), 400
    
    # Perform dependency analysis
    try:
        analysis_result = analyze_sql_batch(sql_batch)
        return jsonify(analysis_result)
    except Exception as e:
        logger.error(f"Error analyzing SQL dependencies: {e}")
        return jsonify({"error": str(e)}), 500
        
@app.route('/api/optimization-status', methods=['GET', 'POST'])
def api_optimization_status():
    """
    API endpoint to check the availability status of the AI-powered SQL optimization feature.
    This feature is optional and requires an OpenAI API key to be set in the environment
    or provided directly in the request.
    """
    api_key = None
    
    # Check if API key is provided in the request
    if request.method == 'POST' and request.json:
        api_key = request.json.get('api_key')
    
    try:
        # We need to modify the API module to accept the API key
        # For now, let's directly use the ai_optimizer module
        from sql_converter.ai_optimizer import get_optimization_status
        status = get_optimization_status(api_key)
        return jsonify(status)
    except Exception as e:
        logger.error(f"Error checking optimization status: {e}")
        return jsonify({
            "available": False,
            "message": f"Error checking optimization status: {str(e)}"
        })

@app.route('/api/optimize-sql', methods=['POST'])
def api_optimize_sql():
    """
    API endpoint to get AI-powered optimization suggestions for a SQL query.
    This feature is optional and requires an OpenAI API key to be set in the environment
    or provided directly in the request.
    """
    data = request.json
    
    if not data:
        return jsonify({"error": "No data provided"}), 400
    
    # Get optimization parameters
    sql = data.get('sql', '')
    dialect = data.get('dialect', '')
    api_key = data.get('api_key')  # Get API key from request if provided
    
    # Validate parameters
    if not sql:
        return jsonify({"error": "SQL query is required"}), 400
    
    dialects = get_supported_dialects()
    if dialect not in dialects:
        return jsonify({"error": f"Dialect '{dialect}' is not supported"}), 400
    
    # Get optimization suggestions
    try:
        # We need to modify the API module to accept the API key
        # For now, let's directly use the ai_optimizer module
        from sql_converter.ai_optimizer import optimize_sql_query
        optimization_result = optimize_sql_query(sql, dialect, api_key)
        return jsonify(optimization_result)
    except Exception as e:
        logger.error(f"Error getting optimization suggestions: {e}")
        return jsonify({
            "available": False,
            "message": f"Error getting optimization suggestions: {str(e)}",
            "suggestions": []
        })

@app.route('/examples')
def examples():
    """Show example SQL queries and conversions."""
    # Create example queries manually
    examples = [
        # MySQL to PostgreSQL
        {
            "title": "MySQL to PostgreSQL: Simple SELECT with LIMIT",
            "source_dialect": "mysql",
            "target_dialect": "postgresql",
            "sql": "SELECT id, name FROM users LIMIT 10"
        },
        {
            "title": "MySQL to PostgreSQL: Date Functions",
            "source_dialect": "mysql",
            "target_dialect": "postgresql",
            "sql": """
            SELECT 
                DATE_FORMAT(created_at, '%Y-%m-%d') AS date,
                COUNT(*) AS count
            FROM orders
            WHERE created_at >= NOW() - INTERVAL 30 DAY
            GROUP BY DATE_FORMAT(created_at, '%Y-%m-%d')
            """
        },
        {
            "title": "MySQL to PostgreSQL: JOINs and Aggregations",
            "source_dialect": "mysql",
            "target_dialect": "postgresql",
            "sql": """
            SELECT 
                u.id,
                u.name,
                COUNT(o.id) AS order_count,
                IFNULL(SUM(o.amount), 0) AS total_spent
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            WHERE u.created_at >= '2020-01-01'
            GROUP BY u.id, u.name
            HAVING COUNT(o.id) > 0
            ORDER BY total_spent DESC
            LIMIT 100
            """
        },
        
        # Oracle to PySpark
        {
            "title": "Oracle to PySpark: Pagination with ROWNUM",
            "source_dialect": "oracle",
            "target_dialect": "pyspark",
            "sql": """
            SELECT * FROM (
                SELECT a.*, ROWNUM rnum FROM (
                    SELECT * FROM employees ORDER BY hire_date DESC
                ) a
                WHERE ROWNUM <= 30
            ) WHERE rnum > 20
            """
        },
        {
            "title": "Oracle to PySpark: Date Functions",
            "source_dialect": "oracle",
            "target_dialect": "pyspark",
            "sql": """
            SELECT 
                employee_id,
                first_name,
                last_name,
                TO_CHAR(hire_date, 'YYYY-MM-DD') AS hire_date
            FROM employees
            WHERE hire_date > ADD_MONTHS(SYSDATE, -12)
            """
        },
        {
            "title": "Oracle to PySpark: NVL and String Concatenation",
            "source_dialect": "oracle",
            "target_dialect": "pyspark",
            "sql": """
            SELECT 
                employee_id,
                first_name || ' ' || last_name AS full_name,
                NVL(department_id, 0) AS dept_id,
                NVL(commission_pct, 0) * salary AS commission
            FROM employees
            WHERE department_id IS NOT NULL
            """
        },
        {
            "title": "Oracle to PySpark: Complex Analytics with DECODE and NVL2",
            "source_dialect": "oracle",
            "target_dialect": "pyspark",
            "sql": """
            /* Complex Oracle query with multiple functions */
            SELECT /*+ PARALLEL(e 4) */
                e.employee_id,
                e.department_id,
                d.department_name,
                e.first_name || ' ' || e.last_name AS employee_name,
                TRUNC(MONTHS_BETWEEN(SYSDATE, e.hire_date) / 12) AS years_of_service,
                DECODE(e.job_id, 
                    'IT_PROG', 'Information Technology',
                    'SA_REP', 'Sales Representative',
                    'FI_ACCOUNT', 'Finance',
                    'Other') AS job_category,
                NVL2(e.commission_pct, 
                    'Commission-based', 
                    'Non-commission') AS compensation_type,
                CASE 
                    WHEN e.salary < 5000 THEN 'Low'
                    WHEN e.salary BETWEEN 5000 AND 10000 THEN 'Medium'
                    WHEN e.salary > 10000 THEN 'High'
                    ELSE 'Unknown'
                END AS salary_band,
                ROUND(e.salary / (1 + NVL(e.commission_pct, 0)), 2) AS base_salary,
                TO_CHAR(e.hire_date, 'YYYY-Q') AS hire_quarter
            FROM 
                employees e,
                departments d
            WHERE 
                e.department_id = d.department_id
                AND e.hire_date BETWEEN TO_DATE('2015-01-01', 'YYYY-MM-DD') 
                    AND ADD_MONTHS(TO_DATE('2020-01-01', 'YYYY-MM-DD'), 24)
            ORDER BY 
                years_of_service DESC,
                base_salary DESC
            """
        },
        {
            "title": "Oracle to PySpark: Complex Subqueries and Analytics",
            "source_dialect": "oracle",
            "target_dialect": "pyspark",
            "sql": """
            WITH 
            dept_summary AS (
                SELECT 
                    department_id,
                    COUNT(*) AS emp_count,
                    ROUND(AVG(salary), 2) AS avg_salary,
                    MIN(hire_date) AS first_hire_date,
                    MAX(salary) AS max_salary
                FROM employees
                GROUP BY department_id
            ),
            manager_info AS (
                SELECT 
                    e.employee_id,
                    e.first_name || ' ' || e.last_name AS manager_name,
                    COUNT(s.employee_id) AS direct_reports
                FROM employees e
                LEFT JOIN employees s ON e.employee_id = s.manager_id
                GROUP BY e.employee_id, e.first_name, e.last_name
                HAVING COUNT(s.employee_id) > 0
            )
            SELECT 
                d.department_id,
                d.department_name,
                l.city,
                l.country_id,
                ds.emp_count,
                ds.avg_salary,
                TO_CHAR(ds.first_hire_date, 'YYYY-MM-DD') AS first_hire,
                TRUNC(MONTHS_BETWEEN(SYSDATE, ds.first_hire_date) / 12, 1) AS dept_age_years,
                m.manager_name AS dept_manager,
                m.direct_reports,
                NVL2(c.country_name, c.country_name, l.country_id) AS country,
                CASE 
                    WHEN ds.emp_count > 10 THEN 'Large'
                    WHEN ds.emp_count > 4 THEN 'Medium'
                    ELSE 'Small'
                END AS dept_size,
                DECODE(SIGN(ds.avg_salary - (SELECT AVG(salary) FROM employees)),
                    1, 'Above Average',
                    0, 'Average',
                    -1, 'Below Average') AS salary_comparison
            FROM 
                departments d
                JOIN locations l ON d.location_id = l.location_id
                LEFT JOIN countries c ON l.country_id = c.country_id
                JOIN dept_summary ds ON d.department_id = ds.department_id
                LEFT JOIN manager_info m ON d.manager_id = m.employee_id
            WHERE 
                ds.emp_count > 0
            ORDER BY 
                ds.emp_count DESC,
                ds.avg_salary DESC
            """
        },
        {
            "title": "Oracle to PySpark: Window Functions and Analytical Queries",
            "source_dialect": "oracle",
            "target_dialect": "pyspark",
            "sql": """
            SELECT 
                e.employee_id,
                e.first_name || ' ' || e.last_name AS employee_name,
                e.department_id,
                d.department_name,
                e.salary,
                e.commission_pct,
                e.hire_date,
                RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS dept_salary_rank,
                PERCENT_RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary) AS salary_percentile,
                ROUND(AVG(e.salary) OVER (PARTITION BY e.department_id), 2) AS dept_avg_salary,
                MAX(e.salary) OVER (PARTITION BY e.department_id) AS dept_max_salary,
                MIN(e.salary) OVER (PARTITION BY e.department_id) AS dept_min_salary,
                COUNT(*) OVER (PARTITION BY e.department_id) AS dept_emp_count,
                ROUND(e.salary / (SUM(e.salary) OVER (PARTITION BY e.department_id)) * 100, 2) AS salary_pct_of_dept,
                ROUND(e.salary / (SUM(e.salary) OVER ()), 4) * 100 AS salary_pct_of_company,
                FIRST_VALUE(e.employee_id) OVER (PARTITION BY e.department_id ORDER BY e.hire_date) AS most_senior_emp_id,
                LAST_VALUE(e.employee_id) OVER (
                    PARTITION BY e.department_id 
                    ORDER BY e.hire_date 
                    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS most_recent_emp_id,
                LEAD(e.salary, 1, NULL) OVER (PARTITION BY e.department_id ORDER BY e.salary) AS next_higher_salary,
                LAG(e.salary, 1, NULL) OVER (PARTITION BY e.department_id ORDER BY e.salary) AS next_lower_salary
            FROM 
                employees e
                JOIN departments d ON e.department_id = d.department_id
            WHERE 
                e.department_id IS NOT NULL
            ORDER BY 
                e.department_id,
                dept_salary_rank
            """
        },
        
        # PostgreSQL to MySQL
        {
            "title": "PostgreSQL to MySQL: Window Functions",
            "source_dialect": "postgresql",
            "target_dialect": "mysql",
            "sql": """
            SELECT 
                product_id,
                product_name,
                price,
                category_id,
                ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY price DESC) AS price_rank
            FROM products
            """
        },
        {
            "title": "PostgreSQL to MySQL: Interval and COALESCE",
            "source_dialect": "postgresql",
            "target_dialect": "mysql",
            "sql": """
            SELECT 
                id, 
                username, 
                COALESCE(email, 'No Email') AS email 
            FROM users 
            WHERE last_login > CURRENT_DATE - INTERVAL '30 days'
            """
        }
    ]
    
    # Convert each example
    for example in examples:
        example['converted_sql'] = convert_sql(
            example['sql'], 
            example['source_dialect'], 
            example['target_dialect']
        )
    
    return render_template('examples.html', examples=examples)

@app.route('/about')
def about():
    """Display information about the SQL Converter."""
    dialects = get_supported_dialects()
    
    features = [
        {
            "title": "SQL Query Cleanup",
            "description": "Removes comments, Oracle hints, and normalizes whitespace and syntax."
        },
        {
            "title": "Oracle to PySpark Specialized Conversion",
            "description": "Comprehensive mapping of Oracle functions to their PySpark equivalents with special handling for complex cases."
        },
        {
            "title": "Old-Style Joins Conversion",
            "description": "Converts Oracle's comma-separated joins to ANSI JOIN syntax for better readability and compatibility."
        },
        {
            "title": "Special Function Handling",
            "description": "Intelligent conversion of complex Oracle functions like DECODE, NVL2, and date/time functions to their appropriate equivalents."
        },
        {
            "title": "ROWNUM Pagination Transformation",
            "description": "Converts Oracle's ROWNUM-based pagination to LIMIT/OFFSET style pagination in other dialects."
        },
        {
            "title": "Custom Pattern Removal",
            "description": "Ability to specify custom strings or regex patterns to be removed from SQL queries before conversion, allowing for tailored cleanup of SQL code."
        },
        {
            "title": "AI-Powered SQL Optimization",
            "description": "Optional feature that provides intelligent optimization suggestions to improve the performance of your SQL queries. Requires an OpenAI API key."
        },
    ]
    
    return render_template('about.html', dialects=dialects, features=features)

# Create templates directory and templates if needed
def create_templates():
    """Create the templates directory and template files if they don't exist."""
    templates_dir = os.path.join(os.path.dirname(__file__), 'templates')
    os.makedirs(templates_dir, exist_ok=True)
    
    # Create the base template
    base_template = os.path.join(templates_dir, 'base.html')
    if not os.path.exists(base_template):
        with open(base_template, 'w') as f:
            f.write("""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{% block title %}SQL Converter{% endblock %}</title>
    <link href="https://cdn.replit.com/agent/bootstrap-agent-dark-theme.min.css" rel="stylesheet">
    <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.3/codemirror.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.3/mode/sql/sql.min.js"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.3/codemirror.min.css">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.3/theme/darcula.min.css">
    <style>
        .CodeMirror {
            height: auto;
            min-height: 150px;
            border: 1px solid var(--bs-dark);
            border-radius: 4px;
        }
        .navbar {
            margin-bottom: 20px;
        }
    </style>
</head>
<body class="bg-dark text-light">
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark">
        <div class="container">
            <a class="navbar-brand" href="/">SQL Converter</a>
            <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNav">
                <span class="navbar-toggler-icon"></span>
            </button>
            <div class="collapse navbar-collapse" id="navbarNav">
                <ul class="navbar-nav">
                    <li class="nav-item">
                        <a class="nav-link" href="/">Converter</a>
                    </li>
                    <li class="nav-item">
                        <a class="nav-link" href="/dependency-analyzer">Dependency Analyzer</a>
                    </li>
                    <li class="nav-item">
                        <a class="nav-link" href="/examples">Examples</a>
                    </li>
                    <li class="nav-item">
                        <a class="nav-link" href="/about">About</a>
                    </li>
                </ul>
            </div>
        </div>
    </nav>

    <div class="container my-4">
        {% block content %}{% endblock %}
    </div>

    <footer class="bg-dark text-light py-3 mt-5">
        <div class="container text-center">
            <p>SQL Converter &copy; 2025</p>
            <p>devms :) &copy;</p>
        </div>
    </footer>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    {% block scripts %}{% endblock %}
</body>
</html>
""")
    
    # Create the index template
    index_template = os.path.join(templates_dir, 'index.html')
    if not os.path.exists(index_template):
        with open(index_template, 'w') as f:
            f.write("""{% extends 'base.html' %}

{% block title %}SQL Converter{% endblock %}

{% block content %}
<div class="row">
    <div class="col-12">
        <div class="card bg-dark border-primary">
            <div class="card-header bg-primary text-white">
                <h2>SQL Dialect Converter</h2>
            </div>
            <div class="card-body">
                <form id="converter-form">
                    <div class="row mb-3">
                        <div class="col-md-6">
                            <label for="source-dialect" class="form-label">Source Dialect</label>
                            <select id="source-dialect" class="form-select bg-dark text-light">
                                {% for dialect in dialects %}
                                <option value="{{ dialect }}">{{ dialect|upper }}</option>
                                {% endfor %}
                            </select>
                        </div>
                        <div class="col-md-6">
                            <label for="target-dialect" class="form-label">Target Dialect</label>
                            <select id="target-dialect" class="form-select bg-dark text-light">
                                {% for dialect in dialects %}
                                <option value="{{ dialect }}" {% if dialect != dialects[0] %}selected{% endif %}>{{ dialect|upper }}</option>
                                {% endfor %}
                            </select>
                        </div>
                    </div>
                    
                    <div class="mb-3">
                        <label for="source-sql" class="form-label">SQL Query</label>
                        <textarea id="source-sql" class="form-control bg-dark text-light" rows="5" placeholder="Enter your SQL query here..."></textarea>
                    </div>
                    
                    <div class="mb-3">
                        <div class="form-check form-switch">
                            <input class="form-check-input" type="checkbox" id="custom-removals-toggle">
                            <label class="form-check-label" for="custom-removals-toggle">Enable Custom Removals</label>
                        </div>
                        <div id="custom-removals-container" class="mt-2 d-none">
                            <label for="custom-removals" class="form-label">
                                Custom Strings/Patterns to Remove 
                                <small class="text-muted">(Enter one per line, can include regex patterns)</small>
                            </label>
                            <textarea id="custom-removals" class="form-control bg-dark text-light" rows="3" 
                                placeholder="Example:&#10;NOLOGGING&#10;/\\* \\*/$&#10;STATS_MODE"></textarea>
                            <small class="text-muted">These strings or regex patterns will be removed from your SQL before conversion.</small>
                        </div>
                    </div>
                    
                    <div class="d-grid">
                        <button type="submit" class="btn btn-primary">Convert</button>
                    </div>
                </form>
            </div>
        </div>
    </div>
</div>

<div id="result-container" class="row mt-4 d-none">
    <div class="col-12">
        <div class="card bg-dark border-success">
            <div class="card-header bg-success text-white">
                <h3>Converted SQL</h3>
            </div>
            <div class="card-body">
                <div id="converted-sql-container">
                    <textarea id="converted-sql" class="form-control bg-dark text-light" rows="5" readonly></textarea>
                </div>
                <div class="mt-3 d-flex gap-2">
                    <button id="copy-btn" class="btn btn-outline-light">Copy to Clipboard</button>
                    <div class="d-flex align-items-center ms-3">
                        <div class="form-check form-switch">
                            <input class="form-check-input" type="checkbox" id="ai-optimization-toggle">
                            <label class="form-check-label" for="ai-optimization-toggle">AI Optimization</label>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>

<div id="optimization-container" class="row mt-4 d-none">
    <div class="col-12">
        <div class="card bg-dark border-warning">
            <div class="card-header bg-warning text-dark">
                <h3>SQL Optimization Suggestions</h3>
            </div>
            <div class="card-body">
                <div id="optimization-loading" class="text-center d-none">
                    <div class="spinner-border text-warning" role="status">
                        <span class="visually-hidden">Loading...</span>
                    </div>
                    <p class="mt-2">Generating optimization suggestions...</p>
                </div>
                <div id="optimization-status-container" class="d-none">
                    <div class="alert alert-info">
                        <h5 class="alert-heading" id="optimization-status-heading">AI Optimization Status</h5>
                        <p id="optimization-status-message"></p>
                    </div>
                </div>
                <div id="optimization-results-container" class="d-none">
                    <h5>Suggestions:</h5>
                    <ul id="optimization-suggestions" class="list-group list-group-flush bg-dark"></ul>
                </div>
            </div>
        </div>
    </div>
</div>

<div id="error-container" class="row mt-4 d-none">
    <div class="col-12">
        <div class="alert alert-danger">
            <h4 class="alert-heading">Error</h4>
            <p id="error-message"></p>
        </div>
    </div>
</div>
{% endblock %}

{% block scripts %}
<script>
    // Initialize CodeMirror for SQL editor
    const sourceEditor = CodeMirror.fromTextArea(document.getElementById('source-sql'), {
        mode: 'text/x-sql',
        theme: 'darcula',
        lineNumbers: true,
        indentWithTabs: true,
        smartIndent: true,
        lineWrapping: true,
        extraKeys: {"Ctrl-Space": "autocomplete"}
    });
    
    let resultEditor;
    
    // Toggle custom removals section when checkbox is clicked
    document.getElementById('custom-removals-toggle').addEventListener('change', function() {
        const container = document.getElementById('custom-removals-container');
        if (this.checked) {
            container.classList.remove('d-none');
        } else {
            container.classList.add('d-none');
        }
    });
    
    document.getElementById('converter-form').addEventListener('submit', async function(e) {
        e.preventDefault();
        
        const sourceDialect = document.getElementById('source-dialect').value;
        const targetDialect = document.getElementById('target-dialect').value;
        const sql = sourceEditor.getValue();
        
        // Hide previous results and errors
        document.getElementById('result-container').classList.add('d-none');
        document.getElementById('error-container').classList.add('d-none');
        
        // Validate input
        if (!sql.trim()) {
            showError('Please enter a SQL query');
            return;
        }
        
        // Get custom removals if enabled
        let customRemovalsArray = null;
        const customRemovalsToggle = document.getElementById('custom-removals-toggle');
        if (customRemovalsToggle.checked) {
            const customRemovalsText = document.getElementById('custom-removals').value;
            if (customRemovalsText.trim()) {
                // Split by lines and filter out empty lines
                customRemovalsArray = customRemovalsText
                    .split('\n')
                    .map(line => line.trim())
                    .filter(line => line.length > 0);
            }
        }
        
        try {
            const response = await fetch('/api/convert', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    sql: sql,
                    source_dialect: sourceDialect,
                    target_dialect: targetDialect,
                    custom_removals: customRemovalsArray
                })
            });
            
            const data = await response.json();
            
            if (!response.ok) {
                showError(data.error || 'An error occurred during conversion');
                return;
            }
            
            // Show result
            document.getElementById('result-container').classList.remove('d-none');
            
            // Update the result textarea and initialize CodeMirror if not done
            if (!resultEditor) {
                document.getElementById('converted-sql').value = data.converted_sql;
                resultEditor = CodeMirror.fromTextArea(document.getElementById('converted-sql'), {
                    mode: 'text/x-sql',
                    theme: 'darcula',
                    lineNumbers: true,
                    readOnly: true,
                    lineWrapping: true
                });
            } else {
                resultEditor.setValue(data.converted_sql);
            }
        } catch (error) {
            showError('Network error: ' + error.message);
        }
    });
    
    document.getElementById('copy-btn').addEventListener('click', function() {
        const convertedSql = resultEditor ? resultEditor.getValue() : document.getElementById('converted-sql').value;
        navigator.clipboard.writeText(convertedSql)
            .then(() => {
                const copyBtn = this;
                copyBtn.textContent = 'Copied!';
                copyBtn.classList.add('btn-success');
                copyBtn.classList.remove('btn-outline-light');
                
                setTimeout(() => {
                    copyBtn.textContent = 'Copy to Clipboard';
                    copyBtn.classList.remove('btn-success');
                    copyBtn.classList.add('btn-outline-light');
                }, 2000);
            })
            .catch(err => {
                console.error('Failed to copy: ', err);
            });
    });
    
    // AI Optimization Toggle
    document.getElementById('ai-optimization-toggle').addEventListener('change', async function() {
        const optimizationContainer = document.getElementById('optimization-container');
        
        if (this.checked) {
            // Show optimization container
            optimizationContainer.classList.remove('d-none');
            
            // Get the current SQL from the result editor
            const sql = resultEditor ? resultEditor.getValue() : document.getElementById('converted-sql').value;
            
            // Get the current target dialect
            const targetDialect = document.getElementById('target-dialect').value;
            
            try {
                // First check if optimization is available
                document.getElementById('optimization-loading').classList.remove('d-none');
                document.getElementById('optimization-status-container').classList.add('d-none');
                document.getElementById('optimization-results-container').classList.add('d-none');
                
                const statusResponse = await fetch('/api/optimization-status');
                const statusData = await statusResponse.json();
                
                if (!statusResponse.ok) {
                    showOptimizationStatus('Error', statusData.message || 'An error occurred while checking optimization status');
                    return;
                }
                
                if (!statusData.available) {
                    showOptimizationStatus('Feature Not Available', statusData.message || 'AI optimization requires an OpenAI API key');
                    return;
                }
                
                // If available, get optimization suggestions
                const optimizeResponse = await fetch('/api/optimize-sql', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        sql: sql,
                        dialect: targetDialect
                    })
                });
                
                const optimizeData = await optimizeResponse.json();
                
                if (!optimizeResponse.ok) {
                    showOptimizationStatus('Error', optimizeData.message || 'An error occurred during optimization');
                    return;
                }
                
                if (!optimizeData.available) {
                    showOptimizationStatus('Feature Not Available', optimizeData.message);
                    return;
                }
                
                // Show optimization suggestions
                document.getElementById('optimization-loading').classList.add('d-none');
                document.getElementById('optimization-results-container').classList.remove('d-none');
                
                const suggestionsList = document.getElementById('optimization-suggestions');
                suggestionsList.innerHTML = '';
                
                if (optimizeData.suggestions && optimizeData.suggestions.length > 0) {
                    optimizeData.suggestions.forEach(suggestion => {
                        const listItem = document.createElement('li');
                        listItem.className = 'list-group-item bg-dark text-light border-secondary';
                        
                        if (suggestion.title) {
                            const title = document.createElement('h6');
                            title.textContent = suggestion.title;
                            listItem.appendChild(title);
                        }
                        
                        const description = document.createElement('p');
                        description.textContent = suggestion.description;
                        listItem.appendChild(description);
                        
                        suggestionsList.appendChild(listItem);
                    });
                } else {
                    const listItem = document.createElement('li');
                    listItem.className = 'list-group-item bg-dark text-light border-secondary';
                    listItem.textContent = 'No optimization suggestions available for this query.';
                    suggestionsList.appendChild(listItem);
                }
            } catch (error) {
                showOptimizationStatus('Error', 'Network error: ' + error.message);
            }
        } else {
            // Hide optimization container
            optimizationContainer.classList.add('d-none');
        }
    });
    
    function showOptimizationStatus(heading, message) {
        document.getElementById('optimization-loading').classList.add('d-none');
        document.getElementById('optimization-status-container').classList.remove('d-none');
        document.getElementById('optimization-status-heading').textContent = heading;
        document.getElementById('optimization-status-message').textContent = message;
    }
    
    function showError(message) {
        const errorContainer = document.getElementById('error-container');
        errorContainer.classList.remove('d-none');
        document.getElementById('error-message').textContent = message;
    }
</script>
{% endblock %}
""")
    
    # Create the examples template
    examples_template = os.path.join(templates_dir, 'examples.html')
    if not os.path.exists(examples_template):
        with open(examples_template, 'w') as f:
            f.write("""{% extends 'base.html' %}

{% block title %}SQL Converter Examples{% endblock %}

{% block content %}
<div class="row">
    <div class="col-12">
        <div class="card bg-dark border-info">
            <div class="card-header bg-info text-white">
                <h2>Example Conversions</h2>
            </div>
            <div class="card-body">
                <p class="lead">Explore these example SQL conversions between different dialects.</p>
                
                <div class="accordion" id="examplesAccordion">
                    {% for example in examples %}
                    <div class="accordion-item bg-dark text-light border-secondary">
                        <h2 class="accordion-header" id="heading{{ loop.index }}">
                            <button class="accordion-button bg-dark text-light collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#collapse{{ loop.index }}">
                                {{ example.title }}
                            </button>
                        </h2>
                        <div id="collapse{{ loop.index }}" class="accordion-collapse collapse" data-bs-parent="#examplesAccordion">
                            <div class="accordion-body">
                                <div class="row">
                                    <div class="col-md-6">
                                        <h5>Source SQL ({{ example.source_dialect|upper }})</h5>
                                        <div class="source-sql-example" data-index="{{ loop.index }}">{{ example.sql }}</div>
                                    </div>
                                    <div class="col-md-6">
                                        <h5>Converted SQL ({{ example.target_dialect|upper }})</h5>
                                        <div class="converted-sql-example" data-index="{{ loop.index }}">{{ example.converted_sql }}</div>
                                    </div>
                                </div>
                                <div class="mt-3">
                                    <button class="btn btn-primary btn-sm try-example" data-index="{{ loop.index }}">
                                        Try this example
                                    </button>
                                </div>
                            </div>
                        </div>
                    </div>
                    {% endfor %}
                </div>
            </div>
        </div>
    </div>
</div>
{% endblock %}

{% block scripts %}
<script>
    document.addEventListener('DOMContentLoaded', function() {
        // Initialize CodeMirror for all examples
        document.querySelectorAll('.source-sql-example').forEach(function(element) {
            const sourceEditor = CodeMirror(element, {
                value: element.textContent.trim(),
                mode: 'text/x-sql',
                theme: 'darcula',
                lineNumbers: true,
                readOnly: true,
                lineWrapping: true
            });
            element.textContent = '';
        });
        
        document.querySelectorAll('.converted-sql-example').forEach(function(element) {
            const resultEditor = CodeMirror(element, {
                value: element.textContent.trim(),
                mode: 'text/x-sql',
                theme: 'darcula',
                lineNumbers: true,
                readOnly: true,
                lineWrapping: true
            });
            element.textContent = '';
        });
        
        // Handle "Try this example" button clicks
        document.querySelectorAll('.try-example').forEach(function(button) {
            button.addEventListener('click', function() {
                const index = this.getAttribute('data-index');
                const sourceSQL = document.querySelector(`.source-sql-example[data-index="${index}"] .CodeMirror`).CodeMirror.getValue();
                const sourceDialect = this.closest('.accordion-item').querySelector('.accordion-button').textContent.includes('MySQL to') ? 'mysql' : 
                                     this.closest('.accordion-item').querySelector('.accordion-button').textContent.includes('Oracle to') ? 'oracle' : 'postgresql';
                const targetDialect = this.closest('.accordion-item').querySelector('.accordion-button').textContent.includes('to PostgreSQL') ? 'postgresql' : 
                                     this.closest('.accordion-item').querySelector('.accordion-button').textContent.includes('to PySpark') ? 'pyspark' : 'mysql';
                
                // Store in localStorage
                localStorage.setItem('sqlExample', sourceSQL);
                localStorage.setItem('sourceDialect', sourceDialect);
                localStorage.setItem('targetDialect', targetDialect);
                
                // Redirect to converter page
                window.location.href = '/';
            });
        });
    });
</script>
{% endblock %}
""")
    
    # Create the about template
    about_template = os.path.join(templates_dir, 'about.html')
    if not os.path.exists(about_template):
        with open(about_template, 'w') as f:
            f.write("""{% extends 'base.html' %}

{% block title %}About SQL Converter{% endblock %}

{% block content %}
<div class="row">
    <div class="col-12">
        <div class="card bg-dark border-info">
            <div class="card-header bg-info text-white">
                <h2>About SQL Converter</h2>
            </div>
            <div class="card-body">
                <p class="lead">SQL Converter is a tool that helps you convert SQL queries between different database dialects.</p>
                
                <h3>Supported Dialects</h3>
                <p>Current version supports the following SQL dialects:</p>
                <ul>
                    {% for dialect in dialects %}
                    <li><strong>{{ dialect|upper }}</strong></li>
                    {% endfor %}
                </ul>
                
                <h3>Key Features</h3>
                <div class="row mt-4">
                    {% for feature in features %}
                    <div class="col-md-6 mb-4">
                        <div class="card h-100 bg-dark border-secondary">
                            <div class="card-header bg-secondary text-white">
                                <h5>{{ feature.title }}</h5>
                            </div>
                            <div class="card-body">
                                <p>{{ feature.description }}</p>
                            </div>
                        </div>
                    </div>
                    {% endfor %}
                </div>
                
                <h3>Supported SQL Features</h3>
                <ul>
                    <li>SQL syntax cleanup and normalization</li>
                    <li>Conversion of old-style joins to ANSI JOIN syntax</li>
                    <li>Comprehensive handling of date/time functions and formats</li>
                    <li>Complex Oracle function mapping (DECODE, NVL2, etc.)</li>
                    <li>String operation translation</li>
                    <li>Dialect-specific pagination methods</li>
                    <li>Comment and hint removal</li>
                </ul>
                
                <h3>How It Works</h3>
                <p>The SQL Converter works by:</p>
                <ol>
                    <li>Cleaning and normalizing the input SQL query (removing comments, hints, etc.)</li>
                    <li>Converting old-style joins to modern ANSI JOIN syntax where applicable</li>
                    <li>Converting dialect-specific syntax using regex pattern matching</li>
                    <li>Mapping function names and syntax to target dialect equivalents</li>
                    <li>Special handling for complex cases like DECODE, NVL2, and date functions</li>
                </ol>
                
                <h3>Oracle to PySpark Focus</h3>
                <p>The converter has specialized support for Oracle to PySpark conversions, including:</p>
                <ul>
                    <li>Complete mapping of Oracle functions to PySpark equivalents</li>
                    <li>Special handling for Oracle-specific syntax</li>
                    <li>ROWNUM pagination conversion</li>
                    <li>Old-style join conversion</li>
                    <li>Date format pattern conversion</li>
                </ul>
                
                <h3>Limitations</h3>
                <p>While the converter handles many common SQL patterns, there are some limitations:</p>
                <ul>
                    <li>Very complex queries with nested subqueries may not convert perfectly</li>
                    <li>Some advanced analytics functions may require manual adjustment</li>
                    <li>Database-specific extensions and custom functions may need manual translation</li>
                </ul>
                
                <h3>Usage Examples</h3>
                <p>Check out the <a href="/examples">Examples</a> page to see sample conversions between different dialects.</p>
            </div>
        </div>
    </div>
</div>
{% endblock %}
""")

# Create templates before app runs
create_templates()

def main():
    """
    Main function to run the Flask application.
    """    
    # Example conversions to demonstrate the functionality
    dialects = get_supported_dialects()
    logger.info("Supported SQL dialects: %s", ", ".join(dialects))
    
    app.run(host="0.0.0.0", port=5000, debug=True)
    return 0

if __name__ == "__main__":
    main()
