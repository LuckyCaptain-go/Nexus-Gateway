#!/usr/bin/env python3
"""
SQL Dialect Translator using sqlglot
This script receives JSON input with SQL and dialects and returns translated SQL.
"""

import sys
import json
import sqlglot
from sqlglot import parse_one, transpile

def translate_sql(sql, source_dialect, target_dialect):
    """
    Translate SQL from source dialect to target dialect using sqlglot
    """
    try:
        # Map our dialect names to sqlglot dialects if needed
        sqlglot_source = map_dialect(source_dialect)
        sqlglot_target = map_dialect(target_dialect)
        
        # Parse and transpile the SQL
        result = transpile(
            sql,
            read=sqlglot_source,
            write=sqlglot_target,
            pretty=True
        )
        
        if result:
            return result[0]  # transpile returns a list, take the first element
        else:
            return sql  # If no translation occurred, return original
    except Exception as e:
        # Return error as part of JSON response
        return f"ERROR: {str(e)}"

def map_dialect(dialect):
    """
    Map our dialect names to sqlglot dialect names
    """
    dialect_mapping = {
        'mysql': 'mysql',
        'postgres': 'postgres',
        'trino': 'trino', 
        'spark': 'spark',
        'oracle': 'oracle',
        'sqlserver': 'tsql',  # sqlglot uses tsql for SQL Server
        'snowflake': 'snowflake',
        'sqlite': 'sqlite',
        'duckdb': 'duckdb',
        'redshift': 'redshift'
    }
    
    return dialect_mapping.get(dialect.lower(), dialect.lower())

def main():
    # Read JSON input from stdin
    input_data = sys.stdin.read().strip()
    
    try:
        request = json.loads(input_data)
        sql = request.get('sql', '')
        source_dialect = request.get('source_dialect', '')
        target_dialect = request.get('target_dialect', '')
        
        if not sql:
            response = {'error': 'Missing SQL in request'}
        elif not source_dialect:
            response = {'error': 'Missing source dialect in request'}
        elif not target_dialect:
            response = {'error': 'Missing target dialect in request'}
        else:
            translated_sql = translate_sql(sql, source_dialect, target_dialect)
            
            if translated_sql.startswith('ERROR:'):
                response = {'error': translated_sql}
            else:
                response = {'translated_sql': translated_sql}
        
        # Output JSON response
        print(json.dumps(response))
        
    except json.JSONDecodeError:
        error_response = {'error': 'Invalid JSON input'}
        print(json.dumps(error_response))
    except Exception as e:
        error_response = {'error': f'Unexpected error: {str(e)}'}
        print(json.dumps(error_response))

if __name__ == "__main__":
    main()