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
def use_bedrock_calculator(params: dict) -> dict:
    """
    Calculates monthly AWS Bedrock costs for LLMs based on usage patterns. It also returns a step by step detailed explanation of how the various costs were calculated.
    
    Input: dict with component keys (LLM models, optional vector_database, optional tools per model), each containing:
    
    Global parameters:
    - questions_per_month: Number of questions/requests per month (required)
    - system_prompt_tokens: Tokens used for system prompt per question (default: 500)
    - history_qa_pairs: Number of question-answer pairs stored in history context (default: 3)
    
    Vector database parameters (optional, global):
    - component_type: 'vector_database'
    - chunks_per_call: Number of chunks retrieved per call (default: 10)
    - tokens_per_chunk: Tokens per chunk (default: 300)
    
    LLM model parameters:
    - model_name: Name of the LLM model (required)
    - cost_per_million_input_tokens: Cost per million input tokens (required)
    - cost_per_million_output_tokens: Cost per million output tokens (required)
    - input_tokens_per_question: Input tokens per question (default: 10000)
    - output_tokens_per_question: Output tokens per question (default: 500)
    - percent_questions_for_model: Percentage of total questions handled by this model (default: equally distributed)
    - tools (optional): Tool configuration for this model
    
    Tools parameters (per model, optional):
    Tools should be considered if the use case is Agentic in nature.
    - number_of_tools: Total number of tools available to agent (required if tools specified)
    - tools_used_by_agent: Number of tools actually used by agent (required if tools specified)
    - tool_invocations_per_question: Average tool invocations per question (default: 1.5)
    - percent_questions_that_invoke_tools: Percentage of questions that invoke tools (default: 80%)
    - input_tokens_per_tool: Input tokens per tool description (default: 50)
    - output_tokens_per_tool: Output tokens per tool invocation (default: 75)
    
    Output: dict with calculated costs for each component, including:
    - For LLMs: input_cost, output_cost, total_cost (includes vector database, tool, system prompt, and history tokens)
    - calculation_explanations: List of strings showing step-by-step calculations
    - total_all_components: Sum of all component costs
    """
    results = {}
    vector_tokens_per_month = 0
    
    # Extract and validate global parameters
    questions_per_month = params.get('questions_per_month')
    system_prompt_tokens = params.get('system_prompt_tokens', 500)
    history_qa_pairs = params.get('history_qa_pairs', 3)
    
    if questions_per_month is None:
        error_msg = 'Missing required global parameter: questions_per_month'
        logger.error(error_msg)
        return {'error': error_msg}
    
    # Count LLM models for equal distribution default (exclude global params and vector_database)
    model_count = 0
    for key in params.keys():
        if key not in ['vector_database', 'questions_per_month', 'system_prompt_tokens', 'history_qa_pairs']:
            model_count += 1
    
    # Calculate default percentage per model (avoid division by zero)
    default_percent_per_model = 100 / model_count if model_count > 0 else 100
    
    try:
        # FIRST PASS: Calculate vector database tokens (global impact on all models)
        if 'vector_database' in params:
            vector_params = params['vector_database']
            
            # Extract vector database parameters with defaults
            chunks_per_call = vector_params.get('chunks_per_call', 10)
            tokens_per_chunk = vector_params.get('tokens_per_chunk', 300)
            
            # Calculate total vector tokens per month (applies to all models)
            vector_tokens_per_month = chunks_per_call * tokens_per_chunk * questions_per_month
            
            # Vector database explanation (no direct cost, tokens added to LLMs)
            explanations = [
                f"1/ tokens_per_call ({chunks_per_call * tokens_per_chunk:,}) = chunks_per_call ({chunks_per_call}) * tokens_per_chunk ({tokens_per_chunk})",
                f"2/ vector_tokens_per_month ({vector_tokens_per_month:,}) = tokens_per_call ({chunks_per_call * tokens_per_chunk:,}) * questions_per_month ({questions_per_month:,})",
                f"3/ These tokens are added to LLM input tokens for cost calculation"
            ]
            
            results['vector_database'] = {
                'component_type': 'vector_database',
                'vector_tokens_per_month': vector_tokens_per_month,
                'chunks_per_call': chunks_per_call,
                'tokens_per_chunk': tokens_per_chunk,
                'questions_per_month': questions_per_month,
                'calculation_explanations': explanations
            }
        
        # SECOND PASS: Process each LLM model
        for component_key, component_params in params.items():
            # Skip global parameters and vector_database
            if component_key in ['vector_database', 'questions_per_month', 'system_prompt_tokens', 'history_qa_pairs']:
                continue
                
            try:
                # Extract required LLM parameters
                model_name = component_params.get('model_name')
                cost_per_million_input_tokens = component_params.get('cost_per_million_input_tokens')
                cost_per_million_output_tokens = component_params.get('cost_per_million_output_tokens')
                input_tokens_per_question = component_params.get('input_tokens_per_question', 10000)
                output_tokens_per_question = component_params.get('output_tokens_per_question', 500)
                percent_questions_for_model = component_params.get('percent_questions_for_model', default_percent_per_model) / 100
                
                # Validate all required parameters
                if model_name is None:
                    error_msg = f'Model {component_key} missing required parameter: model_name'
                    logger.error(error_msg)
                    return {'error': error_msg}
                if cost_per_million_input_tokens is None:
                    error_msg = f'Model {component_key} missing required parameter: cost_per_million_input_tokens'
                    logger.error(error_msg)
                    return {'error': error_msg}
                if cost_per_million_output_tokens is None:
                    error_msg = f'Model {component_key} missing required parameter: cost_per_million_output_tokens'
                    logger.error(error_msg)
                    return {'error': error_msg}
                
                # Calculate model-specific question allocation
                questions_for_this_model = questions_per_month * percent_questions_for_model
                
                # Calculate base token usage for this model
                base_input_tokens_per_month = input_tokens_per_question * questions_for_this_model
                base_output_tokens_per_month = output_tokens_per_question * questions_for_this_model
                
                # Initialize tool token counters
                tool_input_tokens = 0
                tool_output_tokens = 0
                tool_explanations = []
                
                # Calculate tool tokens if tools are configured for this model
                if 'tools' in component_params:
                    tools_params = component_params['tools']
                    
                    # Extract required tool parameters
                    number_of_tools = tools_params.get('number_of_tools')
                    tools_used_by_agent = tools_params.get('tools_used_by_agent')
                    
                    # Validate required tool parameters
                    if number_of_tools is None:
                        error_msg = f'Model {component_key} tools missing required parameter: number_of_tools'
                        logger.error(error_msg)
                        return {'error': error_msg}
                    if tools_used_by_agent is None:
                        error_msg = f'Model {component_key} tools missing required parameter: tools_used_by_agent'
                        logger.error(error_msg)
                        return {'error': error_msg}
                    
                    # Extract optional tool parameters with defaults
                    tool_invocations_per_question = tools_params.get('tool_invocations_per_question', 1.5)
                    percent_questions_that_invoke_tools = tools_params.get('percent_questions_that_invoke_tools', 80) / 100
                    input_tokens_per_tool = tools_params.get('input_tokens_per_tool', 50)
                    output_tokens_per_tool = tools_params.get('output_tokens_per_tool', 75)
                    
                    # Calculate questions that actually use tools
                    questions_invoking_tools = questions_for_this_model * percent_questions_that_invoke_tools
                    
                    # Tool input tokens: all tool descriptions sent with each tool-using question
                    tool_input_tokens = number_of_tools * input_tokens_per_tool * questions_invoking_tools
                    
                    # Tool output tokens: only used tools generate output, multiplied by invocations
                    tool_output_tokens = tools_used_by_agent * output_tokens_per_tool * tool_invocations_per_question * questions_invoking_tools
                    
                    # Create tool calculation explanations
                    tool_explanations = [
                        f"Questions invoking tools ({questions_invoking_tools:,.0f}) = questions_for_this_model ({questions_for_this_model:,.0f}) * percent_questions_that_invoke_tools ({percent_questions_that_invoke_tools:.1%})",
                        f"Tool input tokens ({tool_input_tokens:,.0f}) = number_of_tools ({number_of_tools}) * input_tokens_per_tool ({input_tokens_per_tool}) * questions_invoking_tools ({questions_invoking_tools:,.0f})",
                        f"Tool output tokens ({tool_output_tokens:,.0f}) = tools_used_by_agent ({tools_used_by_agent}) * output_tokens_per_tool ({output_tokens_per_tool}) * tool_invocations_per_question ({tool_invocations_per_question}) * questions_invoking_tools ({questions_invoking_tools:,.0f})"
                    ]
                
                # Calculate system prompt tokens (sent with every question for this model)
                system_prompt_tokens_total = system_prompt_tokens * questions_for_this_model
                
                # Calculate conversation history tokens
                # Each question includes context from previous Q&A pairs
                tokens_per_qa_pair = input_tokens_per_question + output_tokens_per_question
                history_tokens_per_question = history_qa_pairs * tokens_per_qa_pair
                history_tokens_total = history_tokens_per_question * questions_for_this_model
                
                # Sum all input and output tokens
                total_input_tokens = (base_input_tokens_per_month + vector_tokens_per_month + 
                                    tool_input_tokens + system_prompt_tokens_total + history_tokens_total)
                total_output_tokens = base_output_tokens_per_month + tool_output_tokens
                
                # Calculate final costs
                input_cost = (total_input_tokens / 1_000_000) * cost_per_million_input_tokens
                output_cost = (total_output_tokens / 1_000_000) * cost_per_million_output_tokens
                total_model_cost = input_cost + output_cost
                
                # Build comprehensive calculation explanations
                explanations = [
                    f"Questions for this model ({questions_for_this_model:,.0f}) = questions_per_month ({questions_per_month:,}) * percent_questions_for_model ({percent_questions_for_model:.1%})",
                    f"Base input tokens per month ({base_input_tokens_per_month:,.0f}) = input_tokens_per_question ({input_tokens_per_question:,}) * questions_for_this_model ({questions_for_this_model:,.0f})",
                    f"Base output tokens per month ({base_output_tokens_per_month:,.0f}) = output_tokens_per_question ({output_tokens_per_question:,}) * questions_for_this_model ({questions_for_this_model:,.0f})"
                ]
                
                # Add tool explanations if tools are configured
                if tool_explanations:
                    explanations.extend(tool_explanations)
                
                # Add remaining calculations
                explanations.extend([
                    f"System prompt tokens ({system_prompt_tokens_total:,.0f}) = system_prompt_tokens ({system_prompt_tokens}) * questions_for_this_model ({questions_for_this_model:,.0f})",
                    f"History tokens per Q&A pair ({tokens_per_qa_pair:,}) = input_tokens_per_question ({input_tokens_per_question:,}) + output_tokens_per_question ({output_tokens_per_question:,})",
                    f"History tokens per question ({history_tokens_per_question:,}) = history_qa_pairs ({history_qa_pairs}) * tokens_per_qa_pair ({tokens_per_qa_pair:,})",
                    f"History tokens total ({history_tokens_total:,.0f}) = history_tokens_per_question ({history_tokens_per_question:,}) * questions_for_this_model ({questions_for_this_model:,.0f})",
                    f"Total input tokens ({total_input_tokens:,.0f}) = base_input_tokens ({base_input_tokens_per_month:,.0f}) + vector_tokens ({vector_tokens_per_month:,}) + tool_input_tokens ({tool_input_tokens:,.0f}) + system_prompt_tokens ({system_prompt_tokens_total:,.0f}) + history_tokens ({history_tokens_total:,.0f})",
                    f"Total output tokens ({total_output_tokens:,.0f}) = base_output_tokens ({base_output_tokens_per_month:,.0f}) + tool_output_tokens ({tool_output_tokens:,.0f})",
                    f"Input millions ({total_input_tokens / 1_000_000:,.2f}) = total_input_tokens ({total_input_tokens:,.0f}) / 1,000,000",
                    f"Output millions ({total_output_tokens / 1_000_000:,.2f}) = total_output_tokens ({total_output_tokens:,.0f}) / 1,000,000",
                    f"Input cost (${input_cost:,.2f}) = input_millions ({total_input_tokens / 1_000_000:,.2f}) * cost_per_million_input_tokens (${cost_per_million_input_tokens})",
                    f"Output cost (${output_cost:,.2f}) = output_millions ({total_output_tokens / 1_000_000:,.2f}) * cost_per_million_output_tokens (${cost_per_million_output_tokens})",
                    f"Total model cost (${total_model_cost:,.2f}) = input_cost (${input_cost:,.2f}) + output_cost (${output_cost:,.2f})"
                ])
                
                # Store model results
                results[component_key] = {
                    'component_type': 'llm',
                    'model_name': model_name,
                    'input_cost': input_cost,
                    'output_cost': output_cost,
                    'total_cost': total_model_cost,
                    'input_tokens_per_question': input_tokens_per_question,
                    'output_tokens_per_question': output_tokens_per_question,
                    'percent_questions_for_model': percent_questions_for_model * 100,
                    'questions_for_this_model': questions_for_this_model,
                    'base_input_tokens_per_month': base_input_tokens_per_month,
                    'base_output_tokens_per_month': base_output_tokens_per_month,
                    'vector_tokens_added': vector_tokens_per_month,
                    'tool_input_tokens_added': tool_input_tokens,
                    'tool_output_tokens_added': tool_output_tokens,
                    'system_prompt_tokens_added': system_prompt_tokens_total,
                    'history_tokens_added': history_tokens_total,
                    'total_input_tokens': total_input_tokens,
                    'total_output_tokens': total_output_tokens,
                    'questions_per_month': questions_per_month,
                    'history_qa_pairs': history_qa_pairs,
                    'calculation_explanations': explanations
                }
                
            except Exception as e:
                error_msg = f'Error calculating costs for component {component_key}: {str(e)}'
                logger.exception(error_msg)
                return {'error': error_msg}
        
        # Calculate grand total across all LLM models (exclude vector_database)
        total_cost = 0
        for component_key, component_data in results.items():
            if component_key != 'vector_database':
                total_cost += component_data.get('total_cost', 0)
        
        results['total_all_components'] = total_cost
        
    except Exception as e:
        error_msg = f'Error processing components: {str(e)}'
        logger.exception(error_msg)
        return {'error': error_msg}
    
    return results

@tool
def bedrock_what_if_analysis(
    base_params: dict,
    primary_variable: str,
    primary_range: List[Any],
    secondary_variable: Optional[str] = None,
    secondary_range: Optional[List[Any]] = None
) -> dict:
    """
    Performs what-if analysis on Bedrock costs by varying 1-2 parameters while keeping others constant.
    Perfect for sensitivity analysis and heatmap visualization.
    
    Args:
        base_params: Base configuration dict (same format as use_bedrock_calculator)
        primary_variable: Parameter name to vary (e.g., "questions_per_month", "model1.model_name", "input_tokens_per_question")
        primary_range: List of values for primary variable (e.g., [10000, 50000, 100000] or ["claude-3-haiku", "claude-3-sonnet"])
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
        - 'questions_per_month' -> params_dict['questions_per_month'] = value
        - 'model1.model_name' -> params_dict['model1']['model_name'] = value
        """
        if '.' in param_path:
            # Handle nested parameters (e.g., "model1.model_name")
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
            # Handle top-level parameters (e.g., "questions_per_month")
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
                    result = use_bedrock_calculator(scenario_params)
                    
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
                result = use_bedrock_calculator(scenario_params)
                
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
