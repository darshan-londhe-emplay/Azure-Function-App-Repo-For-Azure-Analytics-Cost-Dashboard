"""
Azure Cost Monitoring Function - PostgreSQL Version
- Cost Management API with proper date handling
- Azure Monitor metrics analysis for low usage detection  
- PostgreSQL database storage for cost and usage data
"""

import datetime
import json
import logging
import os
import time
from decimal import Decimal
from typing import List, Dict, Any, Optional

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.mgmt.costmanagement import CostManagementClient
from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.resource import ResourceManagementClient

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION VARIABLES
# =============================================================================

# PostgreSQL configuration
ENABLE_POSTGRES = os.environ.get('ENABLE_POSTGRES', 'true').lower() == 'true'

# Azure configuration
SUBSCRIPTION_ID = os.environ.get('AZURE_SUBSCRIPTION_ID')
SCOPE = os.environ.get('AZURE_SCOPE', f'/subscriptions/{SUBSCRIPTION_ID}') if SUBSCRIPTION_ID else None

# Performance configuration
MAX_RESOURCES_TO_ANALYZE = 100  # Limit for very large subscriptions
MAX_METRICS_PER_RESOURCE = 5    # Reduce if hitting timeouts

# =============================================================================
# POSTGRESQL HELPER IMPORTS
# =============================================================================

# Import PostgreSQL helper
if ENABLE_POSTGRES:
    try:
        import sys
        import os
        # Add parent directory to path to import postgres_helper
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from postgres_helper import (
            create_postgres_helper,
            insert_azure_cost_data,
            PostgreSQLConnectionError,
            PostgreSQLDataError
        )
    except Exception as e:
        logger.error(f"Failed to import PostgreSQL helpers: {e}")
        ENABLE_POSTGRES = False

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def convert_decimals_to_float(obj):
    """Recursively convert Decimal objects to float for JSON serialization"""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {key: convert_decimals_to_float(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals_to_float(item) for item in obj]
    else:
        return obj

# =============================================================================
# AZURE COST DATA RETRIEVAL FUNCTIONS
# =============================================================================

def get_cost_data(cost_client: CostManagementClient, scope: str, start_date: str, end_date: str, 
                  group_by: List[Dict[str, str]] = None) -> Dict[str, Any]:
    """Fetch cost data from Azure Cost Management API"""
    try:
        from azure.mgmt.costmanagement.models import QueryDefinition, QueryTimePeriod, QueryGrouping
        
        query_definitions = []
        
        if group_by:
            groupings = [QueryGrouping(type=gb['type'], name=gb['name']) for gb in group_by]
        else:
            groupings = []
        
        query_definition = QueryDefinition(
            type="ActualCost",
            timeframe="Custom",
            time_period=QueryTimePeriod(
                from_property=start_date,
                to=end_date
            ),
            dataset={
                "granularity": "Daily",
                "aggregation": {
                    "totalCost": {
                        "name": "PreTaxCost",
                        "function": "Sum"
                    }
                },
                "grouping": groupings
            }
        )
        
        response = cost_client.query.usage(scope=scope, parameters=query_definition)
        
        logger.info(f"Cost Management API call successful")
        return response
    except Exception as e:
        logger.error(f"Cost Management API error: {e}")
        return {'rows': []}

def get_cost_breakdown_by_service(cost_client: CostManagementClient, scope: str, start: str, end: str) -> Dict[str, Any]:
    """Fetch cost data grouped by service"""
    return get_cost_data(
        cost_client, scope, start, end,
        group_by=[{'type': 'Dimension', 'name': 'ServiceName'}]
    )

def get_cost_breakdown_by_region(cost_client: CostManagementClient, scope: str, start: str, end: str) -> Dict[str, Any]:
    """Fetch cost data grouped by region"""
    return get_cost_data(
        cost_client, scope, start, end,
        group_by=[{'type': 'Dimension', 'name': 'ResourceLocation'}]
    )

def get_cost_breakdown_by_region_and_service(cost_client: CostManagementClient, scope: str, start: str, end: str) -> Dict[str, Any]:
    """Fetch cost data grouped by both region and service"""
    return get_cost_data(
        cost_client, scope, start, end,
        group_by=[
            {'type': 'Dimension', 'name': 'ResourceLocation'},
            {'type': 'Dimension', 'name': 'ServiceName'}
        ]
    )

def get_tag_costs(cost_client: CostManagementClient, scope: str, start: str, end: str, tag_key: str) -> Dict[str, Any]:
    """Fetch cost data grouped by a TAG (Team/Owner)"""
    try:
        return get_cost_data(
            cost_client, scope, start, end,
            group_by=[{'type': 'Tag', 'name': tag_key}]
        )
    except Exception as e:
        logger.error(f"Tag Cost Management API error for {tag_key}: {e}")
        return {'rows': []}

def process_cost_response(response: Any) -> List[Dict[str, Any]]:
    """Process Azure Cost Management API response into standardized format"""
    rows = []
    
    try:
        # Azure Cost Management API returns QueryResult object with rows property
        if hasattr(response, 'rows') and response.rows:
            for row in response.rows:
                # Row format varies based on grouping
                # For service: [date, service_name, cost]
                # For region: [date, region_name, cost]
                # For region+service: [date, region_name, service_name, cost]
                if len(row) >= 3:
                    date_str = str(row[0]) if row[0] else None
                    cost_amount = float(row[-1]) if row[-1] else 0.0  # Last element is usually cost
                    
                    # Determine dimension based on row length
                    if len(row) == 3:
                        # Single dimension grouping
                        dimension = str(row[1]) if row[1] else 'Unknown'
                        rows.append({
                            'date': date_str,
                            'dimension': dimension,
                            'cost_amount': cost_amount
                        })
                    elif len(row) >= 4:
                        # Multiple dimension grouping (region + service)
                        region = str(row[1]) if row[1] else 'Unknown'
                        service = str(row[2]) if row[2] else 'Unknown'
                        rows.append({
                            'date': date_str,
                            'region_name': region,
                            'service_name': service,
                            'cost_amount': cost_amount
                        })
    except Exception as e:
        logger.error(f"Error processing cost response: {e}")
    
    return rows

# =============================================================================
# AZURE RESOURCE USAGE ANALYSIS FUNCTIONS
# =============================================================================

def get_primary_metrics_for_service(resource_type: str):
    """Get the most important metrics for each Azure service"""
    service_metrics = {
        'Microsoft.Compute/virtualMachines': [
            {'metric': 'Percentage CPU', 'stat': 'Average', 'threshold': 10.0, 'unit': 'Percent'},
            {'metric': 'Network In', 'stat': 'Average', 'threshold': 1000000, 'unit': 'Bytes'},
            {'metric': 'Disk Read Bytes', 'stat': 'Average', 'threshold': 1000000, 'unit': 'Bytes'}
        ],
        'Microsoft.DBforPostgreSQL/servers': [
            {'metric': 'cpu_percent', 'stat': 'Average', 'threshold': 15.0, 'unit': 'Percent'},
            {'metric': 'connections_active', 'stat': 'Average', 'threshold': 5, 'unit': 'Count'},
            {'metric': 'io_consumption_percent', 'stat': 'Average', 'threshold': 10.0, 'unit': 'Percent'}
        ],
        'Microsoft.Storage/storageAccounts': [
            {'metric': 'Transactions', 'stat': 'Sum', 'threshold': 1000, 'unit': 'Count'},
            {'metric': 'UsedCapacity', 'stat': 'Average', 'threshold': 1000000000, 'unit': 'Bytes'}
        ],
        'Microsoft.Web/sites': [
            {'metric': 'HttpRequests', 'stat': 'Sum', 'threshold': 100, 'unit': 'Count'},
            {'metric': 'AverageResponseTime', 'stat': 'Average', 'threshold': 1000, 'unit': 'Milliseconds'}
        ],
        'Microsoft.ContainerService/managedClusters': [
            {'metric': 'cpuUsageNanoCores', 'stat': 'Average', 'threshold': 1000000000, 'unit': 'NanoCores'},
            {'metric': 'memoryWorkingSetBytes', 'stat': 'Average', 'threshold': 1000000000, 'unit': 'Bytes'}
        ],
        'Microsoft.Network/loadBalancers': [
            {'metric': 'ByteCount', 'stat': 'Sum', 'threshold': 1000000, 'unit': 'Bytes'},
            {'metric': 'PacketCount', 'stat': 'Sum', 'threshold': 1000, 'unit': 'Count'}
        ]
    }
    
    # Default metrics for unknown services
    default_metrics = [
        {'metric': 'Requests', 'stat': 'Sum', 'threshold': 10, 'unit': 'Count'},
        {'metric': 'Errors', 'stat': 'Sum', 'threshold': 1, 'unit': 'Count'}
    ]
    
    return service_metrics.get(resource_type, default_metrics)

def get_all_resources(resource_client: ResourceManagementClient, subscription_id: str) -> List[Dict[str, Any]]:
    """Get all Azure resources in the subscription"""
    try:
        resources = []
        for resource in resource_client.resources.list():
            if resource.id:
                resources.append({
                    'id': resource.id,
                    'name': resource.name,
                    'type': resource.type,
                    'location': resource.location,
                    'resource_group': resource.id.split('/')[4] if len(resource.id.split('/')) > 4 else 'Unknown'
                })
                if len(resources) >= MAX_RESOURCES_TO_ANALYZE:
                    break
        logger.info(f"Found {len(resources)} resources to analyze")
        return resources
    except Exception as e:
        logger.error(f"Error discovering resources: {e}")
        return []

def get_metric_statistics(monitor_client: MonitorManagementClient, resource_id: str, metric_name: str, 
                         start_time: datetime.datetime, end_time: datetime.datetime, 
                         aggregation: str = 'Average') -> Optional[float]:
    """Get metric statistics for a resource"""
    try:
        from azure.mgmt.monitor.models import MetricSettings, TimeSeriesElement
        
        # Calculate time span
        timespan = f"{start_time.isoformat()}/{end_time.isoformat()}"
        
        # Get metrics
        metrics_data = monitor_client.metrics.list(
            resource_uri=resource_id,
            timespan=timespan,
            interval=datetime.timedelta(days=1),
            metricnames=metric_name,
            aggregation=aggregation
        )
        
        if metrics_data.value and len(metrics_data.value) > 0:
            metric = metrics_data.value[0]
            if metric.timeseries and len(metric.timeseries) > 0:
                timeseries = metric.timeseries[0]
                if timeseries.data and len(timeseries.data) > 0:
                    values = [dp.value for dp in timeseries.data if dp.value is not None]
                    if values:
                        if aggregation == 'Average':
                            return sum(values) / len(values)
                        elif aggregation == 'Sum':
                            return sum(values)
                        elif aggregation == 'Maximum':
                            return max(values)
                        elif aggregation == 'Minimum':
                            return min(values)
        
        return None
    except Exception as e:
        logger.warning(f"Error getting metric {metric_name} for {resource_id}: {e}")
        return None

def get_comprehensive_low_usage_resources(monitor_client: MonitorManagementClient, 
                                         resource_client: ResourceManagementClient,
                                         subscription_id: str, start: datetime.date, end: datetime.date) -> List[Dict[str, Any]]:
    """Analyze Azure resources for low-usage patterns"""
    low_usage = []
    
    # Get all resources
    resources = get_all_resources(resource_client, subscription_id)
    logger.info(f"Analyzing {len(resources)} Azure resources for usage patterns")
    
    start_dt = datetime.datetime.combine(start, datetime.time.min)
    end_dt = datetime.datetime.combine(end, datetime.time.min)
    
    # Analyze each resource
    for resource in resources:
        resource_type = resource.get('type', '')
        resource_id = resource.get('id', '')
        resource_name = resource.get('name', 'Unknown')
        resource_location = resource.get('location', 'Unknown')
        
        logger.info(f"Analyzing {resource_type}/{resource_name}...")
        
        try:
            # Get metrics for this resource type
            metrics_config = get_primary_metrics_for_service(resource_type)
            
            for metric_config in metrics_config[:MAX_METRICS_PER_RESOURCE]:
                try:
                    avg_usage = get_metric_statistics(
                        monitor_client,
                        resource_id,
                        metric_config['metric'],
                        start_dt,
                        end_dt,
                        metric_config['stat']
                    )
                    
                    if avg_usage is not None and avg_usage < metric_config['threshold']:
                        low_usage.append({
                            'service': resource_type.split('/')[-1] if '/' in resource_type else resource_type,
                            'resource_id': resource_id,
                            'resource_name': resource_name,
                            'resource_region': resource_location,
                            'metric': metric_config['metric'],
                            'average_usage': round(float(avg_usage), 2),
                            'threshold': metric_config['threshold'],
                            'unit': metric_config['unit'],
                            'stat_type': metric_config['stat']
                        })
                        
                        logger.info(f"Low usage: {resource_type}/{resource_name} - {metric_config['metric']}: {avg_usage:.2f}")
                
                except Exception as e:
                    logger.warning(f"Error processing {resource_type} metric: {e}")
                    continue
        
        except Exception as e:
            logger.error(f"Error analyzing {resource_type}: {e}")
            continue
    
    logger.info(f"Completed analysis: Found {len(low_usage)} low-usage resources")
    return low_usage

# =============================================================================
# MAIN AZURE FUNCTION HANDLER
# =============================================================================

def main(mytimer: func.TimerRequest) -> None:
    """Main Azure Function handler"""
    start_time = time.time()
    
    try:
        logger.info("Starting Azure Cost Monitoring Analysis")
        
        # Validate configuration
        if not SUBSCRIPTION_ID:
            raise ValueError("AZURE_SUBSCRIPTION_ID environment variable is required")
        
        scope = SCOPE if SCOPE else f'/subscriptions/{SUBSCRIPTION_ID}'
        
        # Initialize Azure clients
        credential = DefaultAzureCredential()
        
        cost_client = CostManagementClient(credential, SUBSCRIPTION_ID)
        monitor_client = MonitorManagementClient(credential, SUBSCRIPTION_ID)
        resource_client = ResourceManagementClient(credential, SUBSCRIPTION_ID)
        
        # Calculate date range
        today = datetime.date.today()
        
        # This Month Cost: From 1st of current month to today
        start_date = today.replace(day=1)
        end_date = today
        end_date_plus1 = end_date + datetime.timedelta(days=1)
        
        # Last Month Cost: Complete previous month
        if today.month == 1:
            last_month_start = today.replace(year=today.year-1, month=12, day=1)
            last_month_end = today.replace(year=today.year-1, month=12, day=31)
        else:
            last_month_start = today.replace(month=today.month-1, day=1)
            # Get last day of previous month
            if today.month-1 in [1,3,5,7,8,10,12]:
                last_day = 31
            elif today.month-1 in [4,6,9,11]:
                last_day = 30
            else:  # February
                year = today.year
                if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
                    last_day = 29
                else:
                    last_day = 28
            last_month_end = today.replace(month=today.month-1, day=last_day)
        
        last_month_end_plus1 = last_month_end + datetime.timedelta(days=1)
        
        logger.info(f"Analyzing current month: {start_date} to {end_date}")
        logger.info(f"Analyzing last month: {last_month_start} to {last_month_end}")
        
        # Fetch cost data - BY SERVICE
        cost_data_service_raw = get_cost_breakdown_by_service(cost_client, scope, str(start_date), str(end_date_plus1))
        cost_data_service = {'rows': process_cost_response(cost_data_service_raw)}
        logger.info(f"Retrieved service cost data: {len(cost_data_service['rows'])} records")
        
        # Fetch cost data - BY REGION
        cost_data_region_raw = get_cost_breakdown_by_region(cost_client, scope, str(start_date), str(end_date_plus1))
        cost_data_region = {'rows': process_cost_response(cost_data_region_raw)}
        logger.info(f"Retrieved region cost data: {len(cost_data_region['rows'])} records")
        
        # Fetch cost data - BY REGION AND SERVICE
        cost_data_region_service_raw = get_cost_breakdown_by_region_and_service(cost_client, scope, str(start_date), str(end_date_plus1))
        cost_data_region_service = {'rows': process_cost_response(cost_data_region_service_raw)}
        logger.info(f"Retrieved region+service cost data: {len(cost_data_region_service['rows'])} records")
        
        # Fetch LAST MONTH cost data
        last_month_cost_data_service_raw = get_cost_breakdown_by_service(cost_client, scope, str(last_month_start), str(last_month_end_plus1))
        last_month_cost_data_service = {'rows': process_cost_response(last_month_cost_data_service_raw)}
        logger.info(f"Retrieved last month service cost data")
        
        last_month_cost_data_region_raw = get_cost_breakdown_by_region(cost_client, scope, str(last_month_start), str(last_month_end_plus1))
        last_month_cost_data_region = {'rows': process_cost_response(last_month_cost_data_region_raw)}
        logger.info(f"Retrieved last month region cost data")
        
        last_month_cost_data_region_service_raw = get_cost_breakdown_by_region_and_service(cost_client, scope, str(last_month_start), str(last_month_end_plus1))
        last_month_cost_data_region_service = {'rows': process_cost_response(last_month_cost_data_region_service_raw)}
        logger.info(f"Retrieved last month region+service cost data")
        
        # Analyze resources for low usage
        low_usage_resources = get_comprehensive_low_usage_resources(
            monitor_client, resource_client, SUBSCRIPTION_ID, start_date, end_date
        )
        logger.info(f"Found {len(low_usage_resources)} low-usage resources")
        
        # Calculate totals
        current_total_cost = Decimal('0.0')
        for row in cost_data_service.get('rows', []):
            current_total_cost += Decimal(str(row.get('cost_amount', 0)))
        
        last_total_cost = Decimal('0.0')
        for row in last_month_cost_data_service.get('rows', []):
            last_total_cost += Decimal(str(row.get('cost_amount', 0)))
        
        # Store data in PostgreSQL if enabled
        postgres_results = {}
        execution_time_ms = int((time.time() - start_time) * 1000)
        
        if ENABLE_POSTGRES:
            try:
                logger.info("Storing Azure cost data in PostgreSQL database")
                
                # Test database connection first
                postgres_helper = create_postgres_helper()
                if not postgres_helper.test_connection():
                    raise PostgreSQLConnectionError("Database connection test failed")
                
                # Insert current month data
                current_month_results = insert_azure_cost_data(
                    service_costs_data=cost_data_service,
                    region_costs_data=cost_data_region,
                    region_service_costs_data=cost_data_region_service,
                    low_usage_resources=low_usage_resources,
                    run_date=today,
                    month_label='current'
                )
                
                # Insert last month data
                last_month_low_usage_resources = get_comprehensive_low_usage_resources(
                    monitor_client, resource_client, SUBSCRIPTION_ID, last_month_start, last_month_end
                )
                
                last_month_results = insert_azure_cost_data(
                    service_costs_data=last_month_cost_data_service,
                    region_costs_data=last_month_cost_data_region,
                    region_service_costs_data=last_month_cost_data_region_service,
                    low_usage_resources=last_month_low_usage_resources,
                    run_date=today,
                    month_label='last_month'
                )
                
                # Tag-based costs (Team, Owner) - if available
                # COMMENTED OUT FOR NOW
                # try:
                #     team_current = get_tag_costs(cost_client, scope, str(start_date), str(end_date_plus1), 'Team')
                #     owner_current = get_tag_costs(cost_client, scope, str(start_date), str(end_date_plus1), 'Owner')
                #     team_last = get_tag_costs(cost_client, scope, str(last_month_start), str(last_month_end_plus1), 'Team')
                #     owner_last = get_tag_costs(cost_client, scope, str(last_month_start), str(last_month_end_plus1), 'Owner')
                #     
                #     inserted_team_current = postgres_helper.insert_tag_costs({'rows': process_cost_response(team_current)}, 'current', 'Team')
                #     inserted_owner_current = postgres_helper.insert_tag_costs({'rows': process_cost_response(owner_current)}, 'current', 'Owner')
                #     inserted_team_last = postgres_helper.insert_tag_costs({'rows': process_cost_response(team_last)}, 'last_month', 'Team')
                #     inserted_owner_last = postgres_helper.insert_tag_costs({'rows': process_cost_response(owner_last)}, 'last_month', 'Owner')
                # except Exception as e:
                #     logger.warning(f"Tag costs insertion failed: {e}")
                #     inserted_team_current = inserted_owner_current = inserted_team_last = inserted_owner_last = 0
                inserted_team_current = inserted_owner_current = inserted_team_last = inserted_owner_last = 0
                
                # Insert monitoring run metadata
                run_metadata = {
                    'run_date': today,
                    'start_date': start_date,
                    'end_date': end_date,
                    'last_month_start': last_month_start,
                    'last_month_end': last_month_end,
                    'total_current_cost': float(current_total_cost),
                    'total_last_month_cost': float(last_total_cost),
                    'low_usage_count': len(low_usage_resources),
                    'execution_status': 'success',
                    'execution_time_ms': execution_time_ms
                }
                
                run_id = postgres_helper.insert_monitoring_run(run_metadata)
                
                postgres_results = {
                    'current_month_inserted': current_month_results,
                    'last_month_inserted': last_month_results,
                    'tag_inserts': {
                        'team_current': inserted_team_current,
                        'owner_current': inserted_owner_current,
                        'team_last': inserted_team_last,
                        'owner_last': inserted_owner_last
                    },
                    'run_id': run_id,
                    'status': 'success'
                }
                
                logger.info(f"Successfully stored data in PostgreSQL. Run ID: {run_id}")
                logger.info(f"Current month records inserted: {current_month_results}")
                logger.info(f"Last month records inserted: {last_month_results}")
                
            except (PostgreSQLConnectionError, PostgreSQLDataError) as e:
                logger.error(f"PostgreSQL operation failed: {e}")
                postgres_results = {'status': 'failed', 'error': str(e)}
            except Exception as e:
                logger.error(f"Unexpected error during PostgreSQL operations: {e}")
                postgres_results = {'status': 'failed', 'error': str(e)}
        
        # Log summary
        logger.info("=" * 50)
        logger.info("AZURE COST MONITORING REPORT - COMPLETE")
        logger.info("=" * 50)
        logger.info(f"Current Month Total Cost: ${current_total_cost:.2f}")
        logger.info(f"Last Month Total Cost: ${last_total_cost:.2f}")
        logger.info(f"Low Usage Resources: {len(low_usage_resources)}")
        logger.info(f"Execution Time: {execution_time_ms}ms")
        logger.info("=" * 50)
        
        # Optional: Clean up old data
        if ENABLE_POSTGRES and postgres_results.get('status') == 'success':
            try:
                postgres_helper = create_postgres_helper()
                deleted_count = postgres_helper.cleanup_old_data(days_to_keep=90)
                if deleted_count > 0:
                    logger.info(f"Cleaned up {deleted_count} old records from database")
            except Exception as e:
                logger.warning(f"Database cleanup failed (non-critical): {e}")
        
    except Exception as e:
        error_msg = f"Azure Function execution failed: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Error details: {type(e).__name__}")
        
        # Try to log error to database if possible
        if ENABLE_POSTGRES:
            try:
                postgres_helper = create_postgres_helper()
                run_metadata = {
                    'run_date': datetime.date.today(),
                    'start_date': datetime.date.today().replace(day=1),
                    'end_date': datetime.date.today(),
                    'last_month_start': datetime.date.today().replace(day=1),
                    'last_month_end': datetime.date.today(),
                    'total_current_cost': 0.0,
                    'total_last_month_cost': 0.0,
                    'low_usage_count': 0,
                    'execution_status': 'failed',
                    'error_message': error_msg,
                    'execution_time_ms': int((time.time() - start_time) * 1000)
                }
                postgres_helper.insert_monitoring_run(run_metadata)
            except:
                pass
        
        raise
