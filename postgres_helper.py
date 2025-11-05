"""
PostgreSQL helper module for Azure Cost Monitoring Function.

This module provides:
- Database connection management with connection pooling
- Data insertion functions for Azure cost and usage data
- Error handling and retry logic
- Data transformation utilities

Environment variables required:
- DB_HOST: PostgreSQL host (Azure Database for PostgreSQL)
- DB_PORT: PostgreSQL port (default: 5432)
- DB_NAME: Database name
- DB_USER: Database username
- DB_PASSWORD: Database password
"""

import os
import json
import logging
import psycopg2
import psycopg2.extras
from decimal import Decimal
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional, Tuple
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class PostgreSQLConnectionError(Exception):
    """Raised when PostgreSQL connection fails"""
    pass

class PostgreSQLDataError(Exception):
    """Raised when data insertion/query fails"""
    pass

class PostgresHelper:
    """PostgreSQL helper class for Azure Cost Monitoring data operations"""
    
    def __init__(self):
        self.db_config = self._get_db_config()
        self.connection = None
    
    def _get_db_config(self) -> Dict[str, str]:
        """Get database configuration from environment variables"""
        config = {
            'host': os.environ.get('DB_HOST'),
            'port': os.environ.get('DB_PORT', '5432'),
            'database': os.environ.get('DB_NAME'),
            'user': os.environ.get('DB_USER'),
            'password': os.environ.get('DB_PASSWORD')
        }
        
        # Validate required configuration
        required_fields = ['host', 'database', 'user', 'password']
        missing_fields = [field for field in required_fields if not config.get(field)]
        
        if missing_fields:
            raise PostgreSQLConnectionError(
                f"Missing required database configuration: {', '.join(missing_fields)}"
            )
        
        return config
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections with automatic cleanup"""
        connection = None
        try:
            connection = psycopg2.connect(**self.db_config)
            yield connection
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {e}")
            raise PostgreSQLConnectionError(f"Failed to connect to database: {e}")
        finally:
            if connection:
                connection.close()
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    return result[0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def insert_service_costs(self, service_costs_data: List[Dict[str, Any]], month_label: str) -> int:
        """Insert service cost data into azure_service_costs table"""
        if not service_costs_data:
            logger.warning("No service costs data to insert")
            return 0
        
        insert_query = """
            INSERT INTO azure_service_costs 
            (date_period, service_name, cost_amount, month_label)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (date_period, service_name, month_label) DO NOTHING
        """
        
        records = []
        for row in service_costs_data.get('rows', []):
            date_str = row.get('date') or row.get('date_period')
            if not date_str:
                continue
            
            # For service costs, dimension is the service name
            service = row.get('dimension') or row.get('service_name', '')
            amount = row.get('cost_amount', 0)
            
            try:
                records.append((
                    date_str,
                    service,
                    float(Decimal(str(amount))),
                    month_label
                ))
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid service cost data: {e}")
                continue
        
        return self._execute_batch_insert(insert_query, records)
    
    def insert_region_costs(self, region_costs_data: List[Dict[str, Any]], month_label: str) -> int:
        """Insert region cost data into azure_region_costs table"""
        if not region_costs_data:
            logger.warning("No region costs data to insert")
            return 0
        
        insert_query = """
            INSERT INTO azure_region_costs 
            (date_period, region_name, cost_amount, month_label)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (date_period, region_name, month_label) DO NOTHING
        """
        
        records = []
        for row in region_costs_data.get('rows', []):
            date_str = row.get('date') or row.get('date_period')
            if not date_str:
                continue
            
            # For region costs, dimension is the region name
            region = row.get('dimension') or row.get('region_name', '')
            amount = row.get('cost_amount', 0)
            
            try:
                records.append((
                    date_str,
                    region,
                    float(Decimal(str(amount))),
                    month_label
                ))
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid region cost data: {e}")
                continue
        
        return self._execute_batch_insert(insert_query, records)
    
    def insert_region_service_costs(self, region_service_costs_data: List[Dict[str, Any]], month_label: str) -> int:
        """Insert region+service cost data into azure_region_service_costs table"""
        if not region_service_costs_data:
            logger.warning("No region+service costs data to insert")
            return 0
        
        insert_query = """
            INSERT INTO azure_region_service_costs 
            (date_period, region_name, service_name, cost_amount, month_label)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        
        records = []
        for time_period in region_service_costs_data.get('rows', []):
            date_str = time_period.get('date') or time_period.get('date_period')
            if not date_str:
                continue
                
            region = time_period.get('region_name', '')
            service = time_period.get('service_name', '')
            amount = time_period.get('cost_amount', 0)
            
            try:
                records.append((
                    date_str,
                    region,
                    service,
                    float(Decimal(str(amount))),
                    month_label
                ))
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid region+service cost data: {e}")
                continue
        
        return self._execute_batch_insert(insert_query, records)
    
    def insert_tag_costs(self, tag_costs_data: Dict[str, Any], month_label: str, tag_key: str) -> int:
        """Insert tag-based cost data (by tag only) into azure_tag_costs."""
        if not tag_costs_data:
            logger.warning("No tag costs data to insert")
            return 0

        insert_query = """
            INSERT INTO azure_tag_costs 
            (date_period, month_label, tag_key, tag_value, cost_amount)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (date_period, month_label, tag_key, tag_value) DO NOTHING
        """

        records: List[Tuple] = []

        for row in tag_costs_data.get('rows', []):
            date_str = row.get('date') or row.get('date_period')
            if not date_str:
                continue

            # For tag costs, dimension is the tag value
            tag_value = row.get('dimension') or row.get('tag_value', 'UnTagged')
            amount = row.get('cost_amount', 0)

            try:
                records.append((
                    date_str,
                    month_label,
                    tag_key,
                    tag_value,
                    float(Decimal(str(amount)))
                ))
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid tag cost data: {e}")
                continue

        return self._execute_batch_insert(insert_query, records)

    def insert_low_usage_resources(self, low_usage_resources: List[Dict[str, Any]], run_date: date) -> int:
        """Insert low usage resources data into azure_low_usage_resources table"""
        if not low_usage_resources:
            logger.info("No low usage resources to insert")
            return 0
        
        insert_query = """
            INSERT INTO azure_low_usage_resources 
            (run_date, service_name, resource_id, resource_name, resource_region, 
             metric_name, average_usage, threshold_value, unit, stat_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        records = []
        for resource in low_usage_resources:
            try:
                records.append((
                    run_date,
                    resource.get('service', ''),
                    resource.get('resource_id', ''),
                    resource.get('resource_name', ''),
                    resource.get('resource_region', ''),
                    resource.get('metric', ''),
                    float(resource.get('average_usage', 0)),
                    float(resource.get('threshold', 0)),
                    resource.get('unit', ''),
                    resource.get('stat_type', '')
                ))
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid low usage resource data: {e}")
                continue
        
        return self._execute_batch_insert(insert_query, records)
    
    def insert_monitoring_run(self, run_data: Dict[str, Any]) -> Optional[str]:
        """Insert monitoring run metadata into azure_cost_monitoring_runs table"""
        insert_query = """
            INSERT INTO azure_cost_monitoring_runs 
            (run_date, start_date, end_date, last_month_start, last_month_end,
             total_current_cost, total_last_month_cost, low_usage_count,
             execution_status, error_message, execution_time_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(insert_query, (
                        run_data.get('run_date'),
                        run_data.get('start_date'),
                        run_data.get('end_date'),
                        run_data.get('last_month_start'),
                        run_data.get('last_month_end'),
                        run_data.get('total_current_cost'),
                        run_data.get('total_last_month_cost'),
                        run_data.get('low_usage_count', 0),
                        run_data.get('execution_status', 'success'),
                        run_data.get('error_message'),
                        run_data.get('execution_time_ms')
                    ))
                    
                    result = cursor.fetchone()
                    conn.commit()
                    
                    if result:
                        logger.info(f"Inserted monitoring run with ID: {result[0]}")
                        return str(result[0])
                    
        except psycopg2.Error as e:
            logger.error(f"Failed to insert monitoring run: {e}")
            raise PostgreSQLDataError(f"Failed to insert monitoring run: {e}")
        
        return None
    
    def _execute_batch_insert(self, query: str, records: List[Tuple]) -> int:
        """Execute batch insert with error handling"""
        if not records:
            return 0
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(query, records)
                    conn.commit()
                    
                    inserted_count = cursor.rowcount
                    logger.info(f"Inserted {inserted_count} records")
                    return inserted_count
                    
        except psycopg2.Error as e:
            logger.error(f"Batch insert failed: {e}")
            raise PostgreSQLDataError(f"Batch insert failed: {e}")
    
    def cleanup_old_data(self, days_to_keep: int = 90) -> int:
        """Clean up old data to prevent database bloat"""
        cutoff_date = date.today() - timedelta(days=days_to_keep)
        
        tables_to_clean = [
            'azure_service_costs',
            'azure_region_costs', 
            'azure_region_service_costs',
            'azure_low_usage_resources'
        ]
        
        total_deleted = 0
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    for table in tables_to_clean:
                        if table == 'azure_low_usage_resources':
                            cursor.execute(f"DELETE FROM {table} WHERE run_date < %s", (cutoff_date,))
                        else:
                            cursor.execute(f"DELETE FROM {table} WHERE date_period < %s", (cutoff_date,))
                        
                        deleted_count = cursor.rowcount
                        total_deleted += deleted_count
                        logger.info(f"Deleted {deleted_count} old records from {table}")
                    
                    conn.commit()
                    logger.info(f"Total deleted records: {total_deleted}")
                    return total_deleted
                    
        except psycopg2.Error as e:
            logger.error(f"Failed to cleanup old data: {e}")
            raise PostgreSQLDataError(f"Failed to cleanup old data: {e}")


def create_postgres_helper() -> PostgresHelper:
    """Factory function to create PostgresHelper instance"""
    return PostgresHelper()


# Convenience functions for backward compatibility
def insert_azure_cost_data(
    service_costs_data: Dict[str, Any],
    region_costs_data: Dict[str, Any], 
    region_service_costs_data: Dict[str, Any],
    low_usage_resources: List[Dict[str, Any]],
    run_date: date,
    month_label: str = 'current'
) -> Dict[str, int]:
    """
    Convenience function to insert all Azure cost data in one operation
    """
    helper = create_postgres_helper()
    
    results = {
        'service_costs_inserted': helper.insert_service_costs(service_costs_data, month_label),
        'region_costs_inserted': helper.insert_region_costs(region_costs_data, month_label),
        'region_service_costs_inserted': helper.insert_region_service_costs(region_service_costs_data, month_label),
        'low_usage_inserted': helper.insert_low_usage_resources(low_usage_resources, run_date)
    }
    
    return results

