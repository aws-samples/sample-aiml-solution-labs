from strands import tool
from typing import Optional, List, Dict, Any
import logging

# Configure logger for this module
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

@tool
def use_agentcore_calculator(params: dict) -> dict:
    """
    Calculates monthly AWS AgentCore costs based on usage patterns, and returns each component's costs, total costs. It also returns step-by-step explanation of how each component cost was calculated.
    
    Input: dict with global parameters and component keys ('runtime', 'browser', 'code_interpreter', 'gateway', 'memory')
    
    Global parameters:
    - questions_per_day: Daily question volume (default: 333,333)
    - days_per_month: Days in billing month (default: 30)
    
    Runtime parameters:
    - cost_per_vcpu_hour: Hourly cost per virtual CPU
    - cost_per_gb_hour: Hourly cost per GB memory
    - percent_wait_time: Percentage of time waiting for model response (default: 90%)
    - num_cpus: Number of virtual CPUs (default: 1)
    - gb_memory: Memory allocation in GB (default: 2)
    - seconds_per_question: Agent processing time per question (default: 120)
    
    Browser parameters: Same as runtime plus
    - percent_wait_time: Percentage of time waiting for model response (default: 90%)
    - seconds_per_question: Agent processing time per question (default: 600)
    - percent_questions_using_browser: Percentage using browser tool (default: 0)
    
    Code_interpreter parameters: Same as runtime plus
    - percent_wait_time: Percentage of time waiting for model response (default: 20%)
    - seconds_per_question: Agent processing time per question (default: 60)
    - percent_questions_using_code_interpreter: Percentage using code interpreter (default: 0)
    
    Gateway parameters:
    - cost_per_invoke_tool_api: Cost per InvokeTool API call
    - cost_per_search_api_invocation: Cost per search API call
    - cost_per_tool_indexed_per_month: Monthly cost per indexed tool
    - tools_to_invoke_per_question: Tools invoked per question (default: 1)
    - search_api_calls_per_question: Search calls per question (default: 1)
    - total_tools_indexed: Total tools requiring indexing (default: 0)
    - percent_questions_using_tools: Percentage using any tools (default: 100)
    
    Memory parameters:
    - cost_per_raw_event: Cost per short-term memory event in short-term memory
    - cost_per_memory_record_per_month: Monthly cost per stored record in long-term memory
    - cost_per_memory_retrieval: Cost per memory retrieval call from long-term memory
    - events_per_question: Events created per question in short-term memory (default: 2)
    - percent_questions_storing_events: Percentage creating memory events to be stored in short-term memory (default: 100)
    - percent_events_stored_as_records: Percentage stored as records in long-term memory (default: 20)
    - months_to_store: Duration to retain records in long-term memory (default: 3)
    - records_retrieved_per_question: Records retrieved per question from long-term memory (default: 1)
    - percent_questions_retrieving_records: Percentage retrieving records from long-term memory (default: 100)
    
    Output: dict with calculated costs for each component and total costs. It also returns step by step instructions of how the costs were calculates for each component.
    """
    results = {}
    
    # Extract global parameters
    try:
        questions_per_day = params.get('questions_per_day', 33333)
        days_per_month = params.get('days_per_month', 30)
    except Exception as e:
        error_msg = f'Invalid global parameters: {str(e)}'
        logger.exception(error_msg)
        return {'error': error_msg}
    
    # Runtime costs calculation
    if 'runtime' in params:
        try:
            runtime_params = params['runtime']
            
            # Extract parameters
            cost_per_vcpu_hour = runtime_params.get('cost_per_vcpu_hour')
            cost_per_gb_hour = runtime_params.get('cost_per_gb_hour')
            
            if cost_per_vcpu_hour is None:
                error_msg = 'Runtime component missing required parameter: cost_per_vcpu_hour'
                logger.error(error_msg)
                return {'error': error_msg}
            if cost_per_gb_hour is None:
                error_msg = 'Runtime component missing required parameter: cost_per_gb_hour'
                logger.error(error_msg)
                return {'error': error_msg}
            
            percent_wait_time = runtime_params.get('percent_wait_time', 90) / 100
            percent_cpu_time = 1 - percent_wait_time  # CPU time is inverse of wait time
            num_cpus = runtime_params.get('num_cpus', 1)
            gb_memory = runtime_params.get('gb_memory', 2)
            seconds_per_question = runtime_params.get('seconds_per_question', 120)
        
            # Calculate total usage
            total_questions_per_month = questions_per_day * days_per_month
            total_seconds_per_month = total_questions_per_month * seconds_per_question
            total_hours_per_month = total_seconds_per_month / 3600
            
            # Calculate CPU and memory hours
            vcpu_hours = total_hours_per_month * num_cpus * percent_cpu_time
            gb_hours = total_hours_per_month * gb_memory  # 100% memory usage
            
            # Calculate costs
            cpu_cost = vcpu_hours * cost_per_vcpu_hour
            memory_cost = gb_hours * cost_per_gb_hour
            total_runtime_cost = cpu_cost + memory_cost
            
            # Create calculation explanations
            explanations = [
                f"1/ total_questions_per_month ({total_questions_per_month:,.0f}) = questions_per_day ({questions_per_day:,.0f}) * days_per_month ({days_per_month})",
                f"2/ total_hours_per_month ({total_hours_per_month:,.2f}) = total_questions_per_month ({total_questions_per_month:,.0f}) * seconds_per_question ({seconds_per_question}) / 3600",
                f"3/ vcpu_hours ({vcpu_hours:,.2f}) = total_hours_per_month ({total_hours_per_month:,.2f}) * num_cpus ({num_cpus}) * percent_cpu_time ({percent_cpu_time:.1%})",
                f"4/ gb_hours ({gb_hours:,.2f}) = total_hours_per_month ({total_hours_per_month:,.2f}) * gb_memory ({gb_memory})",
                f"5/ cpu_cost (${cpu_cost:,.2f}) = vcpu_hours ({vcpu_hours:,.2f}) * cost_per_vcpu_hour (${cost_per_vcpu_hour})",
                f"6/ memory_cost (${memory_cost:,.2f}) = gb_hours ({gb_hours:,.2f}) * cost_per_gb_hour (${cost_per_gb_hour})",
                f"7/ total_runtime_cost (${total_runtime_cost:,.2f}) = cpu_cost (${cpu_cost:,.2f}) + memory_cost (${memory_cost:,.2f})"
            ]
            
            results['runtime'] = {
                'cpu_cost': cpu_cost,
                'memory_cost': memory_cost,
                'total_cost': total_runtime_cost,
                'vcpu_hours': vcpu_hours,
                'gb_hours': gb_hours,
                'total_questions_per_month': total_questions_per_month,
                'calculation_explanations': explanations
            }
        except Exception as e:
            error_msg = f'Error calculating runtime costs: {str(e)}'
            logger.exception(error_msg)
            return {'error': error_msg}
    
    # Browser tool costs calculation
    if 'browser' in params:
        try:
            browser_params = params['browser']
            
            # Extract parameters
            cost_per_vcpu_hour = browser_params.get('cost_per_vcpu_hour')
            cost_per_gb_hour = browser_params.get('cost_per_gb_hour')
            
            if cost_per_vcpu_hour is None:
                error_msg = 'Browser component missing required parameter: cost_per_vcpu_hour'
                logger.error(error_msg)
                return {'error': error_msg}
            if cost_per_gb_hour is None:
                error_msg = 'Browser component missing required parameter: cost_per_gb_hour'
                logger.error(error_msg)
                return {'error': error_msg}
            
            percent_wait_time = browser_params.get('percent_wait_time', 90) / 100
            percent_cpu_time = 1 - percent_wait_time  # CPU time is inverse of wait time
            num_cpus = browser_params.get('num_cpus', 1)
            gb_memory = browser_params.get('gb_memory', 2)
            seconds_per_question = browser_params.get('seconds_per_question', 600)
            percent_questions_using_browser = browser_params.get('percent_questions_using_browser', 0) / 100
        
            # Calculate total usage (only for questions that use browser)
            browser_questions_per_day = questions_per_day * percent_questions_using_browser
            total_questions_per_month = browser_questions_per_day * days_per_month
            total_seconds_per_month = total_questions_per_month * seconds_per_question
            total_hours_per_month = total_seconds_per_month / 3600
            
            # Calculate CPU and memory hours
            vcpu_hours = total_hours_per_month * num_cpus * percent_cpu_time
            gb_hours = total_hours_per_month * gb_memory  # 100% memory usage
            
            # Calculate costs
            cpu_cost = vcpu_hours * cost_per_vcpu_hour
            memory_cost = gb_hours * cost_per_gb_hour
            total_browser_cost = cpu_cost + memory_cost
            
            # Create calculation explanations
            explanations = [
                f"1/ browser_questions_per_day ({browser_questions_per_day:,.0f}) = questions_per_day ({questions_per_day:,.0f}) * percent_questions_using_browser ({percent_questions_using_browser:.1%})",
                f"2/ total_questions_per_month ({total_questions_per_month:,.0f}) = browser_questions_per_day ({browser_questions_per_day:,.0f}) * days_per_month ({days_per_month})",
                f"3/ total_hours_per_month ({total_hours_per_month:,.2f}) = total_questions_per_month ({total_questions_per_month:,.0f}) * seconds_per_question ({seconds_per_question}) / 3600",
                f"4/ vcpu_hours ({vcpu_hours:,.2f}) = total_hours_per_month ({total_hours_per_month:,.2f}) * num_cpus ({num_cpus}) * percent_cpu_time ({percent_cpu_time:.1%})",
                f"5/ gb_hours ({gb_hours:,.2f}) = total_hours_per_month ({total_hours_per_month:,.2f}) * gb_memory ({gb_memory})",
                f"6/ cpu_cost (${cpu_cost:,.2f}) = vcpu_hours ({vcpu_hours:,.2f}) * cost_per_vcpu_hour (${cost_per_vcpu_hour})",
                f"7/ memory_cost (${memory_cost:,.2f}) = gb_hours ({gb_hours:,.2f}) * cost_per_gb_hour (${cost_per_gb_hour})",
                f"8/ total_browser_cost (${total_browser_cost:,.2f}) = cpu_cost (${cpu_cost:,.2f}) + memory_cost (${memory_cost:,.2f})"
            ]
            
            results['browser'] = {
                'cpu_cost': cpu_cost,
                'memory_cost': memory_cost,
                'total_cost': total_browser_cost,
                'vcpu_hours': vcpu_hours,
                'gb_hours': gb_hours,
                'total_questions_per_month': total_questions_per_month,
                'percent_questions_using_browser': percent_questions_using_browser * 100,
                'calculation_explanations': explanations
            }
        except Exception as e:
            error_msg = f'Error calculating browser costs: {str(e)}'
            logger.exception(error_msg)
            return {'error': error_msg}
    
    # Code Interpreter costs calculation
    if 'code_interpreter' in params:
        try:
            code_params = params['code_interpreter']
            
            # Extract parameters
            cost_per_vcpu_hour = code_params.get('cost_per_vcpu_hour')
            cost_per_gb_hour = code_params.get('cost_per_gb_hour')
            
            if cost_per_vcpu_hour is None:
                error_msg = 'Code interpreter component missing required parameter: cost_per_vcpu_hour'
                logger.error(error_msg)
                return {'error': error_msg}
            if cost_per_gb_hour is None:
                error_msg = 'Code interpreter component missing required parameter: cost_per_gb_hour'
                logger.error(error_msg)
                return {'error': error_msg}
            
            percent_wait_time = code_params.get('percent_wait_time', 20) / 100
            percent_cpu_time = 1 - percent_wait_time  # CPU time is inverse of wait time
            num_cpus = code_params.get('num_cpus', 1)
            gb_memory = code_params.get('gb_memory', 2)
            seconds_per_question = code_params.get('seconds_per_question', 60)
            percent_questions_using_code_interpreter = code_params.get('percent_questions_using_code_interpreter', 0) / 100
        
            # Calculate total usage (only for questions that use code interpreter)
            code_questions_per_day = questions_per_day * percent_questions_using_code_interpreter
            total_questions_per_month = code_questions_per_day * days_per_month
            total_seconds_per_month = total_questions_per_month * seconds_per_question
            total_hours_per_month = total_seconds_per_month / 3600
            
            # Calculate CPU and memory hours
            vcpu_hours = total_hours_per_month * num_cpus * percent_cpu_time
            gb_hours = total_hours_per_month * gb_memory  # 100% memory usage
            
            # Calculate costs
            cpu_cost = vcpu_hours * cost_per_vcpu_hour
            memory_cost = gb_hours * cost_per_gb_hour
            total_code_cost = cpu_cost + memory_cost
            
            # Create calculation explanations
            explanations = [
                f"1/ code_questions_per_day ({code_questions_per_day:,.0f}) = questions_per_day ({questions_per_day:,.0f}) * percent_questions_using_code_interpreter ({percent_questions_using_code_interpreter:.1%})",
                f"2/ total_questions_per_month ({total_questions_per_month:,.0f}) = code_questions_per_day ({code_questions_per_day:,.0f}) * days_per_month ({days_per_month})",
                f"3/ total_hours_per_month ({total_hours_per_month:,.2f}) = total_questions_per_month ({total_questions_per_month:,.0f}) * seconds_per_question ({seconds_per_question}) / 3600",
                f"4/ vcpu_hours ({vcpu_hours:,.2f}) = total_hours_per_month ({total_hours_per_month:,.2f}) * num_cpus ({num_cpus}) * percent_cpu_time ({percent_cpu_time:.1%})",
                f"5/ gb_hours ({gb_hours:,.2f}) = total_hours_per_month ({total_hours_per_month:,.2f}) * gb_memory ({gb_memory})",
                f"6/ cpu_cost (${cpu_cost:,.2f}) = vcpu_hours ({vcpu_hours:,.2f}) * cost_per_vcpu_hour (${cost_per_vcpu_hour})",
                f"7/ memory_cost (${memory_cost:,.2f}) = gb_hours ({gb_hours:,.2f}) * cost_per_gb_hour (${cost_per_gb_hour})",
                f"8/ total_code_cost (${total_code_cost:,.2f}) = cpu_cost (${cpu_cost:,.2f}) + memory_cost (${memory_cost:,.2f})"
            ]
            
            results['code_interpreter'] = {
                'cpu_cost': cpu_cost,
                'memory_cost': memory_cost,
                'total_cost': total_code_cost,
                'vcpu_hours': vcpu_hours,
                'gb_hours': gb_hours,
                'total_questions_per_month': total_questions_per_month,
                'percent_questions_using_code_interpreter': percent_questions_using_code_interpreter * 100,
                'calculation_explanations': explanations
            }
        except Exception as e:
            error_msg = f'Error calculating code interpreter costs: {str(e)}'
            logger.exception(error_msg)
            return {'error': error_msg}
    
    # Gateway costs calculation
    if 'gateway' in params:
        try:
            gateway_params = params['gateway']
            
            # Extract parameters
            cost_per_invoke_tool_api = gateway_params.get('cost_per_invoke_tool_api')
            cost_per_search_api_invocation = gateway_params.get('cost_per_search_api_invocation')
            cost_per_tool_indexed_per_month = gateway_params.get('cost_per_tool_indexed_per_month')
            
            if cost_per_invoke_tool_api is None:
                error_msg = 'Gateway component missing required parameter: cost_per_invoke_tool_api'
                logger.error(error_msg)
                return {'error': error_msg}
            if cost_per_search_api_invocation is None:
                error_msg = 'Gateway component missing required parameter: cost_per_search_api_invocation'
                logger.error(error_msg)
                return {'error': error_msg}
            if cost_per_tool_indexed_per_month is None:
                error_msg = 'Gateway component missing required parameter: cost_per_tool_indexed_per_month'
                logger.error(error_msg)
                return {'error': error_msg}
            
            tools_to_invoke_per_question = gateway_params.get('tools_to_invoke_per_question', 1)
            search_api_calls_per_question = gateway_params.get('search_api_calls_per_question', 1)
            total_tools_indexed = gateway_params.get('total_tools_indexed', 0)
            percent_questions_using_tools = gateway_params.get('percent_questions_using_tools', 100) / 100
        
            # Calculate monthly usage
            total_questions_per_month = questions_per_day * days_per_month
            questions_using_tools_per_month = total_questions_per_month * percent_questions_using_tools
            
            # Calculate API costs
            total_invoke_tool_calls = questions_using_tools_per_month * tools_to_invoke_per_question
            total_search_api_calls = questions_using_tools_per_month * search_api_calls_per_question
            
            invoke_tool_cost = total_invoke_tool_calls * cost_per_invoke_tool_api
            search_api_cost = total_search_api_calls * cost_per_search_api_invocation
            indexing_cost = total_tools_indexed * cost_per_tool_indexed_per_month
            
            total_gateway_cost = invoke_tool_cost + search_api_cost + indexing_cost
            
            # Create calculation explanations
            explanations = [
                f"1/ total_questions_per_month ({total_questions_per_month:,.0f}) = questions_per_day ({questions_per_day:,.0f}) * days_per_month ({days_per_month})",
                f"2/ questions_using_tools_per_month ({questions_using_tools_per_month:,.0f}) = total_questions_per_month ({total_questions_per_month:,.0f}) * percent_questions_using_tools ({percent_questions_using_tools:.1%})",
                f"3/ total_invoke_tool_calls ({total_invoke_tool_calls:,.0f}) = questions_using_tools_per_month ({questions_using_tools_per_month:,.0f}) * tools_to_invoke_per_question ({tools_to_invoke_per_question})",
                f"4/ total_search_api_calls ({total_search_api_calls:,.0f}) = questions_using_tools_per_month ({questions_using_tools_per_month:,.0f}) * search_api_calls_per_question ({search_api_calls_per_question})",
                f"5/ invoke_tool_cost (${invoke_tool_cost:,.2f}) = total_invoke_tool_calls ({total_invoke_tool_calls:,.0f}) * cost_per_invoke_tool_api (${cost_per_invoke_tool_api})",
                f"6/ search_api_cost (${search_api_cost:,.2f}) = total_search_api_calls ({total_search_api_calls:,.0f}) * cost_per_search_api_invocation (${cost_per_search_api_invocation})",
                f"7/ indexing_cost (${indexing_cost:,.2f}) = total_tools_indexed ({total_tools_indexed}) * cost_per_tool_indexed_per_month (${cost_per_tool_indexed_per_month})",
                f"8/ total_gateway_cost (${total_gateway_cost:,.2f}) = invoke_tool_cost (${invoke_tool_cost:,.2f}) + search_api_cost (${search_api_cost:,.2f}) + indexing_cost (${indexing_cost:,.2f})"
            ]
            
            results['gateway'] = {
                'invoke_tool_cost': invoke_tool_cost,
                'search_api_cost': search_api_cost,
                'indexing_cost': indexing_cost,
                'total_cost': total_gateway_cost,
                'total_invoke_tool_calls': total_invoke_tool_calls,
                'total_search_api_calls': total_search_api_calls,
                'questions_using_tools_per_month': questions_using_tools_per_month,
                'percent_questions_using_tools': percent_questions_using_tools * 100,
                'calculation_explanations': explanations
            }
        except Exception as e:
            error_msg = f'Error calculating gateway costs: {str(e)}'
            logger.exception(error_msg)
            return {'error': error_msg}
    
    # Memory costs calculation
    if 'memory' in params:
        try:
            memory_params = params['memory']
            
            # Extract parameters
            cost_per_raw_event = memory_params.get('cost_per_raw_event')
            cost_per_memory_record_per_month = memory_params.get('cost_per_memory_record_per_month')
            cost_per_memory_retrieval = memory_params.get('cost_per_memory_retrieval')
            
            if cost_per_raw_event is None:
                error_msg = 'Memory component missing required parameter: cost_per_raw_event'
                logger.error(error_msg)
                return {'error': error_msg}
            if cost_per_memory_record_per_month is None:
                error_msg = 'Memory component missing required parameter: cost_per_memory_record_per_month'
                logger.error(error_msg)
                return {'error': error_msg}
            if cost_per_memory_retrieval is None:
                error_msg = 'Memory component missing required parameter: cost_per_memory_retrieval'
                logger.error(error_msg)
                return {'error': error_msg}
            
            events_per_question = memory_params.get('events_per_question', 2)
            percent_questions_storing_events = memory_params.get('percent_questions_storing_events', 100) / 100
            percent_events_stored_as_records = memory_params.get('percent_events_stored_as_records', 20) / 100
            months_to_store = memory_params.get('months_to_store', 3)
            records_retrieved_per_question = memory_params.get('records_retrieved_per_question', 1)
            percent_questions_retrieving_records = memory_params.get('percent_questions_retrieving_records', 100) / 100
        
            # Calculate monthly usage
            total_questions_per_month = questions_per_day * days_per_month
            questions_storing_events = total_questions_per_month * percent_questions_storing_events
            
            # Short-term memory costs
            total_events_per_month = questions_storing_events * events_per_question
            short_term_cost = total_events_per_month * cost_per_raw_event
            
            # Long-term memory storage costs
            records_stored_per_month = total_events_per_month * percent_events_stored_as_records
            total_records_stored = records_stored_per_month * months_to_store
            long_term_storage_cost = total_records_stored * cost_per_memory_record_per_month
            
            # Long-term memory retrieval costs
            questions_retrieving_records = total_questions_per_month * percent_questions_retrieving_records
            total_retrievals_per_month = questions_retrieving_records * records_retrieved_per_question
            long_term_retrieval_cost = total_retrievals_per_month * cost_per_memory_retrieval
            
            total_memory_cost = short_term_cost + long_term_storage_cost + long_term_retrieval_cost
            
            # Create calculation explanations
            explanations = [
                f"1/ total_questions_per_month ({total_questions_per_month:,.0f}) = questions_per_day ({questions_per_day:,.0f}) * days_per_month ({days_per_month})",
                f"2/ questions_storing_events ({questions_storing_events:,.0f}) = total_questions_per_month ({total_questions_per_month:,.0f}) * percent_questions_storing_events ({percent_questions_storing_events:.1%})",
                f"3/ total_events_per_month ({total_events_per_month:,.0f}) = questions_storing_events ({questions_storing_events:,.0f}) * events_per_question ({events_per_question})",
                f"4/ short_term_cost (${short_term_cost:,.2f}) = total_events_per_month ({total_events_per_month:,.0f}) * cost_per_raw_event (${cost_per_raw_event})",
                f"5/ records_stored_per_month ({records_stored_per_month:,.0f}) = total_events_per_month ({total_events_per_month:,.0f}) * percent_events_stored_as_records ({percent_events_stored_as_records:.1%})",
                f"6/ total_records_stored ({total_records_stored:,.0f}) = records_stored_per_month ({records_stored_per_month:,.0f}) * months_to_store ({months_to_store})",
                f"7/ long_term_storage_cost (${long_term_storage_cost:,.2f}) = total_records_stored ({total_records_stored:,.0f}) * cost_per_memory_record_per_month (${cost_per_memory_record_per_month})",
                f"8/ questions_retrieving_records ({questions_retrieving_records:,.0f}) = total_questions_per_month ({total_questions_per_month:,.0f}) * percent_questions_retrieving_records ({percent_questions_retrieving_records:.1%})",
                f"9/ total_retrievals_per_month ({total_retrievals_per_month:,.0f}) = questions_retrieving_records ({questions_retrieving_records:,.0f}) * records_retrieved_per_question ({records_retrieved_per_question})",
                f"10/ long_term_retrieval_cost (${long_term_retrieval_cost:,.2f}) = total_retrievals_per_month ({total_retrievals_per_month:,.0f}) * cost_per_memory_retrieval (${cost_per_memory_retrieval})",
                f"11/ total_memory_cost (${total_memory_cost:,.2f}) = short_term_cost (${short_term_cost:,.2f}) + long_term_storage_cost (${long_term_storage_cost:,.2f}) + long_term_retrieval_cost (${long_term_retrieval_cost:,.2f})"
            ]
            
            results['memory'] = {
                'short_term_cost': short_term_cost,
                'long_term_storage_cost': long_term_storage_cost,
                'long_term_retrieval_cost': long_term_retrieval_cost,
                'total_cost': total_memory_cost,
                'total_events_per_month': total_events_per_month,
                'records_stored_per_month': records_stored_per_month,
                'total_retrievals_per_month': total_retrievals_per_month,
                'percent_questions_storing_events': percent_questions_storing_events * 100,
                'percent_events_stored_as_records': percent_events_stored_as_records * 100,
                'percent_questions_retrieving_records': percent_questions_retrieving_records * 100,
                'calculation_explanations': explanations
            }
        except Exception as e:
            error_msg = f'Error calculating memory costs: {str(e)}'
            logger.exception(error_msg)
            return {'error': error_msg}
    
    # Calculate total costs across all components
    try:
        total_cost = 0
        for component in results.values():
            total_cost += component.get('total_cost', 0)
        
        results['total_all_components'] = total_cost
    except Exception as e:
        error_msg = f'Error calculating total costs: {str(e)}'
        logger.exception(error_msg)
        return {'error': error_msg}
    
    return results

@tool
def agentcore_what_if_analysis(
    base_params: dict,
    primary_variable: str,
    primary_range: List[Any],
    secondary_variable: Optional[str] = None,
    secondary_range: Optional[List[Any]] = None
) -> dict:
    """
    Performs what-if analysis on AgentCore costs by varying 1-2 parameters while keeping others constant.
    Perfect for sensitivity analysis and heatmap visualization.
    
    Args:
        base_params: Base configuration dict (same format as use_agentcore_calculator)
        primary_variable: Parameter name to vary (e.g., "questions_per_day", "runtime.cost_per_vcpu_hour", "gateway.tools_to_invoke_per_question")
        primary_range: List of values for primary variable (e.g., [10000, 50000, 100000] or [0.1, 0.2, 0.3])
        secondary_variable: Optional second parameter to vary for 2D analysis
        secondary_range: List of values for secondary variable (any type)
        
    Returns:
        dict with:
        - analysis_type: "1D" or "2D"
        - primary_variable: Name and range of primary variable
        - secondary_variable: Name and range of secondary variable (if 2D)
        - results: Cost results for each scenario
        - costs_flat: Flattened cost array for heatmap visualization
        - scenarios: List of scenario descriptions
    """
    
    # Initialize result containers
    results = []
    costs_flat = []
    scenarios = []
    
    # Determine analysis type based on secondary variable presence
    is_2d = secondary_variable is not None and secondary_range is not None
    analysis_type = "2D" if is_2d else "1D"
    
    def set_nested_param(params_dict, param_path, value):
        """
        Set parameter that might be nested in the configuration.
        Examples: 
        - 'questions_per_day' -> params_dict['questions_per_day'] = value
        - 'runtime.cost_per_vcpu_hour' -> params_dict['runtime']['cost_per_vcpu_hour'] = value
        """
        if '.' in param_path:
            # Handle nested parameters (e.g., "runtime.cost_per_vcpu_hour")
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
            # Handle top-level parameters (e.g., "questions_per_day")
            params_dict[param_path] = value
    
    try:
        if is_2d:
            # 2D Analysis: Create cost matrix by varying both parameters
            for secondary_val in secondary_range:
                for primary_val in primary_range:
                    # Create deep copy of base configuration for this scenario
                    scenario_params = base_params.copy()
                    
                    # Apply parameter variations
                    set_nested_param(scenario_params, primary_variable, primary_val)
                    set_nested_param(scenario_params, secondary_variable, secondary_val)
                    
                    # Calculate costs for this parameter combination
                    result = use_agentcore_calculator(scenario_params)
                    
                    # Check for calculation errors
                    if 'error' in result:
                        error_msg = f'Calculation failed for {primary_variable}={primary_val}, {secondary_variable}={secondary_val}: {result["error"]}'
                        logger.error(error_msg)
                        return {'error': error_msg}
                    
                    # Extract total cost and store results
                    total_cost = result.get('total_all_components', 0)
                    costs_flat.append(total_cost)
                    
                    scenario_desc = f"{primary_variable}={primary_val}, {secondary_variable}={secondary_val}"
                    scenarios.append(scenario_desc)
                    
                    results.append({
                        'scenario': scenario_desc,
                        'primary_value': primary_val,
                        'secondary_value': secondary_val,
                        'total_cost': total_cost,
                        'detailed_results': result
                    })
        else:
            # 1D Analysis: Vary only the primary parameter
            for primary_val in primary_range:
                # Create deep copy of base configuration for this scenario
                scenario_params = base_params.copy()
                
                # Apply parameter variation
                set_nested_param(scenario_params, primary_variable, primary_val)
                
                # Calculate costs for this parameter value
                result = use_agentcore_calculator(scenario_params)
                
                # Check for calculation errors
                if 'error' in result:
                    error_msg = f'Calculation failed for {primary_variable}={primary_val}: {result["error"]}'
                    logger.error(error_msg)
                    return {'error': error_msg}
                
                # Extract total cost and store results
                total_cost = result.get('total_all_components', 0)
                costs_flat.append(total_cost)
                
                scenario_desc = f"{primary_variable}={primary_val}"
                scenarios.append(scenario_desc)
                
                results.append({
                    'scenario': scenario_desc,
                    'primary_value': primary_val,
                    'total_cost': total_cost,
                    'detailed_results': result
                })
        
        # Compile final analysis results
        return {
            'analysis_type': analysis_type,
            'primary_variable': {
                'name': primary_variable,
                'range': primary_range
            },
            'secondary_variable': {
                'name': secondary_variable,
                'range': secondary_range
            } if is_2d else None,
            'results': results,                    # Detailed results for each scenario
            'costs_flat': costs_flat,             # Flattened cost array for heatmap
            'scenarios': scenarios,               # Scenario descriptions for labels
            'min_cost': min(costs_flat),          # Cost sensitivity metrics
            'max_cost': max(costs_flat),
            'cost_range': max(costs_flat) - min(costs_flat)
        }
        
    except Exception as e:
        error_msg = f'What-if analysis failed: {str(e)}'
        logger.exception(error_msg)
        return {'error': error_msg}
