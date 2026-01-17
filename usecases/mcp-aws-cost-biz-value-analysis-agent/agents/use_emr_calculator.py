from strands import tool
from typing import Optional, Dict, Any
import logging

# Configure logger for this module
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


@tool
def use_emr_calculator(params: dict) -> dict:
    """
    Calculates Amazon ElasticMapReduce (EMR) cluster sizing and provides detailed recommendations based on workload requirements.
    Supports three deployment modes: EC2, Serverless, and EKS.
    
    Input: dict with 'mode' key and mode-specific parameters
    
    Global parameters:
    - mode: Deployment mode - 'ec2', 'serverless', or 'eks' (required)
    
    EC2 mode parameters:
    - data_size_tb: Input data size in terabytes (required)
    - job_type: Workload type - 'batch', 'streaming', 'etl', or 'ml' (default: 'batch')
    - application: Primary application - 'spark', 'hive', or 'presto' (default: 'spark')
    - cores_per_executor: Spark cores per executor (default: 4)
    - memory_per_executor_gb: Spark memory per executor in GB (default: 16)
    - num_executors: Target number of executors (default: 10)
    - instance_type_core: EC2 instance type name for core nodes (default: 'm5.xlarge')
    - core_instance_vcpus: vCPUs for core instance type (default: 4)
    - core_instance_memory_gb: Memory in GB for core instance type (default: 16)
    - core_instance_storage_gb: Storage in GB for core instance type (default: 0)
    - instance_type_task: EC2 instance type name for task nodes (default: 'm5.xlarge')
    - task_instance_vcpus: vCPUs for task instance type (default: 4)
    - task_instance_memory_gb: Memory in GB for task instance type (default: 16)
    - task_instance_storage_gb: Storage in GB for task instance type (default: 0)
    - ebs_storage_core_gb: EBS storage per core node in GB (default: 64)
    - ebs_storage_task_gb: EBS storage per task node in GB (default: 32)
    - join_shuffle_complexity: Join/shuffle intensity - 'low', 'medium', or 'high' (default: 'medium')
    - caching_required: Whether caching is needed (default: False)
    
    Serverless mode parameters:
    - job_type: Workload type - 'batch', 'streaming', 'etl', or 'ml' (default: 'batch')
    - application: Primary application - 'spark' or 'hive' (default: 'spark')
    - worker_vcpus: Worker vCPUs (default: 4)
    - worker_memory_gb: Worker memory in GB (default: 16)
    - expected_workers: Expected parallel workers (default: 10)
    - max_workers: Maximum workers for peak load (default: 20)
    
    EKS mode parameters:
    - data_size_tb: Input data size in terabytes (required)
    - job_type: Workload type - 'batch', 'streaming', 'etl', or 'ml' (default: 'batch')
    - application: Primary application - 'spark' or 'trino' (default: 'spark')
    - cores_per_worker: Worker cores (default: 4)
    - memory_per_worker_gb: Worker memory in GB (default: 16)
    - num_workers: Target number of workers (default: 10)
    - coordinator_cores: Coordinator/driver cores (default: 2 for Spark, 4 for Trino)
    - coordinator_memory_gb: Coordinator/driver memory in GB (default: 8 for Spark, 16 for Trino)
    - node_instance_type: EKS node instance type name (default: 'm5.xlarge')
    - node_instance_vcpus: vCPUs for node instance type (default: 4)
    - node_instance_memory_gb: Memory in GB for node instance type (default: 16)
    - node_instance_storage_gb: Storage in GB for node instance type (default: 0)
    
    Output: dict with sizing recommendations, resource calculations, and configuration details
    """
    results = {}
    
    # Validate mode parameter
    mode = params.get('mode', '').lower()
    if mode not in ['ec2', 'serverless', 'eks']:
        error_msg = "Invalid mode. Must be 'ec2', 'serverless', or 'eks'"
        logger.error(error_msg)
        return {'error': error_msg}
    
    results['mode'] = mode
    
    try:
        if mode == 'ec2':
            return _calculate_emr_ec2(params)
        elif mode == 'serverless':
            return _calculate_emr_serverless(params)
        elif mode == 'eks':
            return _calculate_emr_eks(params)
    except Exception as e:
        error_msg = f'EMR calculation failed: {str(e)}'
        logger.exception(error_msg)
        return {'error': error_msg}


def _get_instance_specs(params: dict, prefix: str) -> Dict[str, int]:
    """
    Get AWS EC2 instance specifications from parameters.
    
    Args:
        params: Parameter dictionary
        prefix: Prefix for parameter keys ('core_instance', 'task_instance', or 'node_instance')
    
    Returns:
        Dict with vcpus, memory_gb, and storage_gb
    """
    return {
        'vcpus': params.get(f'{prefix}_vcpus', 4),
        'memory_gb': params.get(f'{prefix}_memory_gb', 16),
        'storage_gb': params.get(f'{prefix}_storage_gb', 0)
    }


def _get_workload_recommendations(job_type: str, application: Optional[str] = None) -> Dict[str, Any]:
    """Get workload-specific recommendations."""
    recommendations = {
        'etl': {'instance_family': 'm5', 'cores_per_executor': 4, 'memory_per_core': 4},
        'ml': {'instance_family': 'r5', 'cores_per_executor': 2, 'memory_per_core': 8},
        'streaming': {'instance_family': 'c5', 'cores_per_executor': 6, 'memory_per_core': 2},
        'batch': {'instance_family': 'm5', 'cores_per_executor': 4, 'memory_per_core': 4}
    }
    
    # Trino-specific optimizations
    if application == 'trino':
        return {
            'instance_family': 'r5',
            'cores_per_executor': 8,
            'memory_per_core': 8
        }
    
    return recommendations.get(job_type, recommendations['batch'])


def _calculate_emr_ec2(params: dict) -> dict:
    """Calculate EMR EC2 cluster sizing."""
    # Extract parameters with defaults
    data_size_tb = params.get('data_size_tb')
    if data_size_tb is None:
        error_msg = 'EC2 mode requires data_size_tb parameter'
        logger.error(error_msg)
        return {'error': error_msg}
    
    job_type = params.get('job_type', 'batch').lower()
    application = params.get('application', 'spark').lower()
    
    # Get workload recommendations
    workload_rec = _get_workload_recommendations(job_type, application)
    
    cores_per_executor = params.get('cores_per_executor', workload_rec['cores_per_executor'])
    memory_per_executor_gb = params.get('memory_per_executor_gb', cores_per_executor * workload_rec['memory_per_core'])
    num_executors = params.get('num_executors', 10)
    
    instance_type_core = params.get('instance_type_core', 'm5.xlarge')
    instance_type_task = params.get('instance_type_task', 'm5.xlarge')
    
    ebs_storage_core_gb = params.get('ebs_storage_core_gb', 64)
    ebs_storage_task_gb = params.get('ebs_storage_task_gb', 32)
    
    join_shuffle_complexity = params.get('join_shuffle_complexity', 'medium').lower()
    caching_required = params.get('caching_required', False)
    
    # Get instance specifications from parameters
    core_specs = _get_instance_specs(params, 'core_instance')
    task_specs = _get_instance_specs(params, 'task_instance')
    
    # Calculate core nodes for EMRFS
    core_nodes = max(1, min(10, int(data_size_tb * 0.2)))
    
    # Calculate executors per node
    executors_per_core_node = min(
        core_specs['memory_gb'] // memory_per_executor_gb,
        core_specs['vcpus'] // cores_per_executor
    )
    total_executors_from_cores = core_nodes * executors_per_core_node
    
    # Calculate task nodes
    if num_executors > total_executors_from_cores:
        additional_executors_needed = num_executors - total_executors_from_cores
        executors_per_task_node = min(
            task_specs['memory_gb'] // memory_per_executor_gb,
            task_specs['vcpus'] // cores_per_executor
        )
        task_nodes = int(-(-additional_executors_needed // executors_per_task_node))
    else:
        task_nodes = 0
        executors_per_task_node = 0
    
    # Adjust for caching and shuffle requirements
    if caching_required or join_shuffle_complexity == "high":
        task_nodes = max(task_nodes, int(core_nodes * 0.5))
    
    # Calculate totals
    total_memory_gb = (core_nodes * core_specs['memory_gb']) + (task_nodes * task_specs['memory_gb'])
    total_vcpus = (core_nodes * core_specs['vcpus']) + (task_nodes * task_specs['vcpus'])
    actual_executors = (core_nodes * executors_per_core_node) + (task_nodes * executors_per_task_node)
    total_local_storage_gb = (core_nodes * ebs_storage_core_gb) + (task_nodes * ebs_storage_task_gb)
    
    # Create calculation explanations
    explanations = [
        f"1/ core_nodes ({core_nodes}) = max(1, min(10, int(data_size_tb ({data_size_tb}) * 0.2)))",
        f"2/ executors_per_core_node ({executors_per_core_node}) = min(memory_gb ({core_specs['memory_gb']}) // memory_per_executor_gb ({memory_per_executor_gb}), vcpus ({core_specs['vcpus']}) // cores_per_executor ({cores_per_executor}))",
        f"3/ total_executors_from_cores ({total_executors_from_cores}) = core_nodes ({core_nodes}) * executors_per_core_node ({executors_per_core_node})",
        f"4/ task_nodes ({task_nodes}) = calculated based on additional executor needs and adjusted for caching/shuffle complexity",
        f"5/ actual_executors ({actual_executors}) = (core_nodes ({core_nodes}) * executors_per_core_node ({executors_per_core_node})) + (task_nodes ({task_nodes}) * executors_per_task_node ({executors_per_task_node}))",
        f"6/ total_memory_gb ({total_memory_gb}) = (core_nodes ({core_nodes}) * {core_specs['memory_gb']}) + (task_nodes ({task_nodes}) * {task_specs['memory_gb']})",
        f"7/ total_vcpus ({total_vcpus}) = (core_nodes ({core_nodes}) * {core_specs['vcpus']}) + (task_nodes ({task_nodes}) * {task_specs['vcpus']})"
    ]
    
    return {
        'mode': 'ec2',
        'job_type': job_type,
        'application': application,
        'core_nodes': {
            'count': core_nodes,
            'instance_type': instance_type_core,
            'vcpus_per_node': core_specs['vcpus'],
            'memory_gb_per_node': core_specs['memory_gb'],
            'ebs_storage_gb_per_node': ebs_storage_core_gb,
            'executors_per_node': executors_per_core_node
        },
        'task_nodes': {
            'count': task_nodes,
            'instance_type': instance_type_task,
            'vcpus_per_node': task_specs['vcpus'],
            'memory_gb_per_node': task_specs['memory_gb'],
            'ebs_storage_gb_per_node': ebs_storage_task_gb,
            'executors_per_node': executors_per_task_node
        },
        'cluster_totals': {
            'total_nodes': core_nodes + task_nodes,
            'total_vcpus': total_vcpus,
            'total_memory_gb': total_memory_gb,
            'total_executors': actual_executors,
            'total_local_storage_gb': total_local_storage_gb,
            'data_storage_tb': data_size_tb,
            'storage_type': 'EMRFS (S3)'
        },
        'executor_configuration': {
            'cores_per_executor': cores_per_executor,
            'memory_per_executor_gb': memory_per_executor_gb,
            'target_executors': num_executors,
            'actual_executors': actual_executors
        },
        'workload_settings': {
            'join_shuffle_complexity': join_shuffle_complexity,
            'caching_required': caching_required
        },
        'calculation_explanations': explanations,
        'recommendations': [
            f"Recommended instance family for {job_type} workload: {workload_rec['instance_family']}",
            "Use EMRFS (S3) for data storage",
            "Consider managed scaling for dynamic workloads",
            "Use spot instances for task nodes to reduce costs" if task_nodes > 0 else "Consider adding task nodes for better parallelism"
        ]
    }


def _calculate_emr_serverless(params: dict) -> dict:
    """Calculate EMR Serverless sizing."""
    job_type = params.get('job_type', 'batch').lower()
    application = params.get('application', 'spark').lower()
    
    # Get workload recommendations
    workload_rec = _get_workload_recommendations(job_type, application)
    
    worker_vcpus = params.get('worker_vcpus', workload_rec['cores_per_executor'])
    worker_memory_gb = params.get('worker_memory_gb', worker_vcpus * workload_rec['memory_per_core'])
    expected_workers = params.get('expected_workers', 10)
    max_workers = params.get('max_workers', expected_workers * 2)
    
    # Calculate resource totals
    expected_total_vcpus = expected_workers * worker_vcpus
    expected_total_memory_gb = expected_workers * worker_memory_gb
    max_total_vcpus = max_workers * worker_vcpus
    max_total_memory_gb = max_workers * worker_memory_gb
    
    # Create calculation explanations
    explanations = [
        f"1/ worker_vcpus ({worker_vcpus}) = based on {job_type} workload recommendation",
        f"2/ worker_memory_gb ({worker_memory_gb}) = worker_vcpus ({worker_vcpus}) * memory_per_core ({workload_rec['memory_per_core']})",
        f"3/ expected_total_vcpus ({expected_total_vcpus}) = expected_workers ({expected_workers}) * worker_vcpus ({worker_vcpus})",
        f"4/ expected_total_memory_gb ({expected_total_memory_gb}) = expected_workers ({expected_workers}) * worker_memory_gb ({worker_memory_gb})",
        f"5/ max_total_vcpus ({max_total_vcpus}) = max_workers ({max_workers}) * worker_vcpus ({worker_vcpus})",
        f"6/ max_total_memory_gb ({max_total_memory_gb}) = max_workers ({max_workers}) * worker_memory_gb ({worker_memory_gb})"
    ]
    
    # Generate recommendations based on workload
    recommendations = [
        f"EMR Serverless is optimized for {application.upper()} workloads",
        "No infrastructure management required - fully serverless",
        "Automatic scaling between expected and max workers"
    ]
    
    if application == 'hive':
        recommendations.append("Hive on EMR Serverless is optimized for SQL workloads")
    elif job_type == 'ml':
        recommendations.append("Consider larger workers (8+ vCPUs) for ML workloads")
    elif job_type == 'streaming':
        recommendations.append("Use smaller, more numerous workers for streaming workloads")
    
    return {
        'mode': 'serverless',
        'job_type': job_type,
        'application': application,
        'worker_configuration': {
            'worker_vcpus': worker_vcpus,
            'worker_memory_gb': worker_memory_gb,
            'expected_workers': expected_workers,
            'max_workers': max_workers
        },
        'resource_totals': {
            'expected_total_vcpus': expected_total_vcpus,
            'expected_total_memory_gb': expected_total_memory_gb,
            'max_total_vcpus': max_total_vcpus,
            'max_total_memory_gb': max_total_memory_gb
        },
        'scaling': {
            'auto_scaling': True,
            'min_workers': expected_workers,
            'max_workers': max_workers,
            'scaling_factor': max_workers / expected_workers
        },
        'calculation_explanations': explanations,
        'recommendations': recommendations
    }


def _calculate_emr_eks(params: dict) -> dict:
    """Calculate EMR on EKS sizing."""
    data_size_tb = params.get('data_size_tb')
    if data_size_tb is None:
        error_msg = 'EKS mode requires data_size_tb parameter'
        logger.error(error_msg)
        return {'error': error_msg}
    
    job_type = params.get('job_type', 'batch').lower()
    application = params.get('application', 'spark').lower()
    
    # Get workload recommendations
    workload_rec = _get_workload_recommendations(job_type, application)
    
    cores_per_worker = params.get('cores_per_worker', workload_rec['cores_per_executor'])
    memory_per_worker_gb = params.get('memory_per_worker_gb', cores_per_worker * workload_rec['memory_per_core'])
    num_workers = params.get('num_workers', 10)
    
    # Set coordinator defaults based on application
    if application == 'trino':
        default_coordinator_cores = 4
        default_coordinator_memory = 16
    else:
        default_coordinator_cores = 2
        default_coordinator_memory = 8
    
    coordinator_cores = params.get('coordinator_cores', default_coordinator_cores)
    coordinator_memory_gb = params.get('coordinator_memory_gb', default_coordinator_memory)
    
    node_instance_type = params.get('node_instance_type', 'm5.xlarge')
    
    # Get instance specifications from parameters
    node_specs = _get_instance_specs(params, 'node_instance')
    
    # Calculate node requirements (20% overhead for system pods)
    usable_cores_per_node = int(node_specs['vcpus'] * 0.8)
    usable_memory_per_node = int(node_specs['memory_gb'] * 0.8)
    
    # Calculate workers per node
    workers_per_node_cpu = usable_cores_per_node // cores_per_worker
    workers_per_node_memory = usable_memory_per_node // memory_per_worker_gb
    workers_per_node = max(1, min(workers_per_node_cpu, workers_per_node_memory))
    
    # Calculate total nodes needed
    nodes_for_workers = int(-(-num_workers // workers_per_node))  # Ceiling division
    coordinator_nodes = 1
    total_nodes = nodes_for_workers + coordinator_nodes
    
    # Calculate totals
    total_worker_memory_gb = num_workers * memory_per_worker_gb
    total_worker_cores = num_workers * cores_per_worker
    total_cluster_memory_gb = total_nodes * node_specs['memory_gb']
    total_cluster_vcpus = total_nodes * node_specs['vcpus']
    
    # Create calculation explanations
    explanations = [
        f"1/ usable_cores_per_node ({usable_cores_per_node}) = int(node_vcpus ({node_specs['vcpus']}) * 0.8) [20% overhead for system pods]",
        f"2/ usable_memory_per_node ({usable_memory_per_node}) = int(node_memory_gb ({node_specs['memory_gb']}) * 0.8) [20% overhead for system pods]",
        f"3/ workers_per_node ({workers_per_node}) = min(usable_cores ({usable_cores_per_node}) // cores_per_worker ({cores_per_worker}), usable_memory ({usable_memory_per_node}) // memory_per_worker_gb ({memory_per_worker_gb}))",
        f"4/ nodes_for_workers ({nodes_for_workers}) = ceiling(num_workers ({num_workers}) / workers_per_node ({workers_per_node}))",
        f"5/ total_nodes ({total_nodes}) = nodes_for_workers ({nodes_for_workers}) + coordinator_nodes (1)",
        f"6/ total_cluster_vcpus ({total_cluster_vcpus}) = total_nodes ({total_nodes}) * node_vcpus ({node_specs['vcpus']})",
        f"7/ total_cluster_memory_gb ({total_cluster_memory_gb}) = total_nodes ({total_nodes}) * node_memory_gb ({node_specs['memory_gb']})"
    ]
    
    # Generate recommendations
    recommendations = [
        f"Recommended instance family for {job_type} with {application}: {workload_rec['instance_family']}",
        "Use cluster autoscaler for dynamic workloads",
        "Configure resource quotas per namespace",
        "Monitor pod resource utilization and adjust requests/limits"
    ]
    
    if application == 'trino':
        recommendations.extend([
            "Use separate node pools for coordinator and workers",
            "Configure Trino memory settings: query.max-memory-per-node",
            "Consider spot instances for worker nodes"
        ])
    else:
        recommendations.extend([
            "Use Spark operator for job management",
            "Consider spot instances for executor nodes"
        ])
    
    return {
        'mode': 'eks',
        'job_type': job_type,
        'application': application,
        'node_group': {
            'instance_type': node_instance_type,
            'total_nodes': total_nodes,
            'vcpus_per_node': node_specs['vcpus'],
            'memory_gb_per_node': node_specs['memory_gb'],
            'usable_vcpus_per_node': usable_cores_per_node,
            'usable_memory_gb_per_node': usable_memory_per_node,
            'workers_per_node': workers_per_node
        },
        'coordinator_configuration': {
            'count': 1,
            'cores': coordinator_cores,
            'memory_gb': coordinator_memory_gb,
            'role': 'coordinator' if application == 'trino' else 'driver'
        },
        'worker_configuration': {
            'count': num_workers,
            'cores_per_worker': cores_per_worker,
            'memory_per_worker_gb': memory_per_worker_gb,
            'role': 'worker' if application == 'trino' else 'executor'
        },
        'cluster_totals': {
            'total_nodes': total_nodes,
            'total_cluster_vcpus': total_cluster_vcpus,
            'total_cluster_memory_gb': total_cluster_memory_gb,
            'total_worker_vcpus': total_worker_cores,
            'total_worker_memory_gb': total_worker_memory_gb,
            'data_storage_tb': data_size_tb,
            'storage_type': 'EMRFS (S3)'
        },
        'kubernetes_settings': {
            'system_overhead_percent': 20,
            'pod_scheduling': f"Use node affinity for {application} workloads"
        },
        'calculation_explanations': explanations,
        'recommendations': recommendations
    }


@tool
def emr_what_if_analysis(
    base_params: dict,
    primary_variable: str,
    primary_range: list,
    secondary_variable: Optional[str] = None,
    secondary_range: Optional[list] = None
) -> dict:
    """
    Performs what-if analysis on EMR cluster sizing by varying 1-2 parameters while keeping others constant.
    Perfect for sensitivity analysis and comparing different configurations.
    
    Args:
        base_params: Base configuration dict (same format as use_emr_calculator)
        primary_variable: Parameter name to vary (e.g., "data_size_tb", "num_executors", "core_instance_vcpus")
        primary_range: List of values for primary variable (e.g., [1, 2, 5, 10] or [4, 8, 16])
        secondary_variable: Optional second parameter to vary for 2D analysis
        secondary_range: List of values for secondary variable
        
    Returns:
        dict with:
        - analysis_type: "1D" or "2D"
        - primary_variable: Name and range of primary variable
        - secondary_variable: Name and range of secondary variable (if 2D)
        - results: Sizing results for each scenario
        - scenarios: List of scenario descriptions
        - summary_metrics: Key metrics across all scenarios (min/max nodes, vcpus, memory)
    """
    
    # Initialize result containers
    results = []
    scenarios = []
    
    # Determine analysis type based on secondary variable presence
    is_2d = secondary_variable is not None and secondary_range is not None
    analysis_type = "2D" if is_2d else "1D"
    
    def set_nested_param(params_dict, param_path, value):
        """
        Set parameter that might be nested in the configuration.
        Examples: 
        - 'data_size_tb' -> params_dict['data_size_tb'] = value
        - 'num_executors' -> params_dict['num_executors'] = value
        """
        if '.' in param_path:
            # Handle nested parameters if needed in the future
            keys = param_path.split('.')
            current = params_dict
            # Navigate to the parent container
            for key in keys[:-1]:
                if key not in current:
                    current[key] = {}
                current = current[key]
            # Set the final value
            current[keys[-1]] = value
        else:
            # Handle top-level parameters
            params_dict[param_path] = value
    
    def extract_key_metrics(result: dict) -> dict:
        """Extract key metrics from EMR calculation result based on mode."""
        mode = result.get('mode', '')
        
        if 'error' in result:
            return {
                'error': result['error'],
                'total_nodes': 0,
                'total_vcpus': 0,
                'total_memory_gb': 0
            }
        
        if mode == 'ec2':
            cluster_totals = result.get('cluster_totals', {})
            return {
                'total_nodes': cluster_totals.get('total_nodes', 0),
                'total_vcpus': cluster_totals.get('total_vcpus', 0),
                'total_memory_gb': cluster_totals.get('total_memory_gb', 0),
                'total_executors': cluster_totals.get('total_executors', 0),
                'core_nodes': result.get('core_nodes', {}).get('count', 0),
                'task_nodes': result.get('task_nodes', {}).get('count', 0)
            }
        elif mode == 'serverless':
            resource_totals = result.get('resource_totals', {})
            worker_config = result.get('worker_configuration', {})
            return {
                'expected_workers': worker_config.get('expected_workers', 0),
                'max_workers': worker_config.get('max_workers', 0),
                'expected_total_vcpus': resource_totals.get('expected_total_vcpus', 0),
                'expected_total_memory_gb': resource_totals.get('expected_total_memory_gb', 0),
                'max_total_vcpus': resource_totals.get('max_total_vcpus', 0),
                'max_total_memory_gb': resource_totals.get('max_total_memory_gb', 0)
            }
        elif mode == 'eks':
            cluster_totals = result.get('cluster_totals', {})
            node_group = result.get('node_group', {})
            return {
                'total_nodes': cluster_totals.get('total_nodes', 0),
                'total_cluster_vcpus': cluster_totals.get('total_cluster_vcpus', 0),
                'total_cluster_memory_gb': cluster_totals.get('total_cluster_memory_gb', 0),
                'total_worker_vcpus': cluster_totals.get('total_worker_vcpus', 0),
                'total_worker_memory_gb': cluster_totals.get('total_worker_memory_gb', 0),
                'workers_per_node': node_group.get('workers_per_node', 0)
            }
        
        return {}
    
    try:
        if is_2d:
            # 2D Analysis: Create matrix by varying both parameters
            for secondary_val in secondary_range:
                for primary_val in primary_range:
                    # Create deep copy of base configuration for this scenario
                    import copy
                    scenario_params = copy.deepcopy(base_params)
                    
                    # Apply parameter variations
                    set_nested_param(scenario_params, primary_variable, primary_val)
                    set_nested_param(scenario_params, secondary_variable, secondary_val)
                    
                    # Calculate sizing for this parameter combination
                    result = use_emr_calculator(scenario_params)
                    
                    # Extract key metrics
                    metrics = extract_key_metrics(result)
                    
                    scenario_desc = f"{primary_variable}={primary_val}, {secondary_variable}={secondary_val}"
                    scenarios.append(scenario_desc)
                    
                    results.append({
                        'scenario': scenario_desc,
                        'primary_value': primary_val,
                        'secondary_value': secondary_val,
                        'key_metrics': metrics,
                        'detailed_results': result
                    })
        else:
            # 1D Analysis: Vary only the primary parameter
            for primary_val in primary_range:
                # Create deep copy of base configuration for this scenario
                import copy
                scenario_params = copy.deepcopy(base_params)
                
                # Apply parameter variation
                set_nested_param(scenario_params, primary_variable, primary_val)
                
                # Calculate sizing for this parameter value
                result = use_emr_calculator(scenario_params)
                
                # Extract key metrics
                metrics = extract_key_metrics(result)
                
                scenario_desc = f"{primary_variable}={primary_val}"
                scenarios.append(scenario_desc)
                
                results.append({
                    'scenario': scenario_desc,
                    'primary_value': primary_val,
                    'key_metrics': metrics,
                    'detailed_results': result
                })
        
        # Calculate summary metrics across all scenarios
        mode = base_params.get('mode', '')
        summary_metrics = {}
        
        if mode == 'ec2':
            all_nodes = [r['key_metrics'].get('total_nodes', 0) for r in results if 'error' not in r['key_metrics']]
            all_vcpus = [r['key_metrics'].get('total_vcpus', 0) for r in results if 'error' not in r['key_metrics']]
            all_memory = [r['key_metrics'].get('total_memory_gb', 0) for r in results if 'error' not in r['key_metrics']]
            all_executors = [r['key_metrics'].get('total_executors', 0) for r in results if 'error' not in r['key_metrics']]
            
            if all_nodes:
                summary_metrics = {
                    'min_total_nodes': min(all_nodes),
                    'max_total_nodes': max(all_nodes),
                    'min_total_vcpus': min(all_vcpus),
                    'max_total_vcpus': max(all_vcpus),
                    'min_total_memory_gb': min(all_memory),
                    'max_total_memory_gb': max(all_memory),
                    'min_total_executors': min(all_executors),
                    'max_total_executors': max(all_executors)
                }
        elif mode == 'serverless':
            all_expected_workers = [r['key_metrics'].get('expected_workers', 0) for r in results if 'error' not in r['key_metrics']]
            all_max_workers = [r['key_metrics'].get('max_workers', 0) for r in results if 'error' not in r['key_metrics']]
            all_expected_vcpus = [r['key_metrics'].get('expected_total_vcpus', 0) for r in results if 'error' not in r['key_metrics']]
            all_max_vcpus = [r['key_metrics'].get('max_total_vcpus', 0) for r in results if 'error' not in r['key_metrics']]
            
            if all_expected_workers:
                summary_metrics = {
                    'min_expected_workers': min(all_expected_workers),
                    'max_expected_workers': max(all_expected_workers),
                    'min_max_workers': min(all_max_workers),
                    'max_max_workers': max(all_max_workers),
                    'min_expected_vcpus': min(all_expected_vcpus),
                    'max_expected_vcpus': max(all_expected_vcpus),
                    'min_max_vcpus': min(all_max_vcpus),
                    'max_max_vcpus': max(all_max_vcpus)
                }
        elif mode == 'eks':
            all_nodes = [r['key_metrics'].get('total_nodes', 0) for r in results if 'error' not in r['key_metrics']]
            all_cluster_vcpus = [r['key_metrics'].get('total_cluster_vcpus', 0) for r in results if 'error' not in r['key_metrics']]
            all_cluster_memory = [r['key_metrics'].get('total_cluster_memory_gb', 0) for r in results if 'error' not in r['key_metrics']]
            
            if all_nodes:
                summary_metrics = {
                    'min_total_nodes': min(all_nodes),
                    'max_total_nodes': max(all_nodes),
                    'min_total_cluster_vcpus': min(all_cluster_vcpus),
                    'max_total_cluster_vcpus': max(all_cluster_vcpus),
                    'min_total_cluster_memory_gb': min(all_cluster_memory),
                    'max_total_cluster_memory_gb': max(all_cluster_memory)
                }
        
        # Compile final analysis results
        return {
            'analysis_type': analysis_type,
            'mode': mode,
            'primary_variable': {
                'name': primary_variable,
                'range': primary_range
            },
            'secondary_variable': {
                'name': secondary_variable,
                'range': secondary_range
            } if is_2d else None,
            'results': results,
            'scenarios': scenarios,
            'summary_metrics': summary_metrics,
            'total_scenarios': len(results)
        }
        
    except Exception as e:
        error_msg = f'What-if analysis failed: {str(e)}'
        logger.exception(error_msg)
        return {'error': error_msg}
