"""
Chart Analysis Agent using Strands Agents framework.

This agent generates JSON data structures compatible with Amazon QuickSight
for creating visualizations including bar charts, line plots, and heat maps.
"""

import os
from typing import List, Optional
from pydantic import BaseModel, Field

from strands import Agent, tool
from strands.models import BedrockModel

# =============================================================================
# CONFIGURATION
# =============================================================================
REGION = os.environ.get("AWS_REGION", "us-east-1")
MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"


# =============================================================================
# PYDANTIC MODELS FOR CHART DATA
# =============================================================================
class BarChartDataPoint(BaseModel):
    """Data point for bar chart visualization."""
    category: str = Field(description="Category label for the bar (x-axis)")
    value: float = Field(description="Numeric value for the bar height (y-axis)")
    group: Optional[str] = Field(default=None, description="Optional grouping for stacked/grouped bars")


class BarChartData(BaseModel):
    """Bar chart data structure for QuickSight."""
    chart_type: str = Field(default="bar_chart", description="Chart type identifier")
    title: str = Field(description="Chart title")
    x_axis_label: str = Field(description="Label for x-axis")
    y_axis_label: str = Field(description="Label for y-axis")
    data_points: List[BarChartDataPoint] = Field(description="List of data points")
    currency: Optional[str] = Field(default="USD", description="Currency for monetary values")


class PlotChartDataPoint(BaseModel):
    """Data point for line/scatter plot visualization."""
    x_value: float = Field(description="X-axis value")
    y_value: float = Field(description="Y-axis value")
    series: Optional[str] = Field(default=None, description="Series name for multi-line plots")


class PlotChartData(BaseModel):
    """Line/scatter plot data structure for QuickSight."""
    chart_type: str = Field(default="plot_chart", description="Chart type identifier")
    title: str = Field(description="Chart title")
    x_axis_label: str = Field(description="Label for x-axis")
    y_axis_label: str = Field(description="Label for y-axis")
    data_points: List[PlotChartDataPoint] = Field(description="List of data points")
    plot_style: str = Field(default="line", description="Plot style: 'line', 'scatter', or 'area'")


class HeatMapCell(BaseModel):
    """Cell data for heat map visualization."""
    row: str = Field(description="Row label")
    column: str = Field(description="Column label")
    value: float = Field(description="Cell value determining color intensity")


class HeatMapData(BaseModel):
    """Heat map data structure for QuickSight."""
    chart_type: str = Field(default="heat_map", description="Chart type identifier")
    title: str = Field(description="Chart title")
    row_label: str = Field(description="Label for rows")
    column_label: str = Field(description="Label for columns")
    value_label: str = Field(description="Label for values")
    cells: List[HeatMapCell] = Field(description="List of heat map cells")
    color_scale: str = Field(default="blue_to_red", description="Color scale: 'blue_to_red', 'green_to_red', 'sequential'")


class ChartResponse(BaseModel):
    """Response containing one or more chart data structures."""
    charts: List[dict] = Field(description="List of chart data objects")
    summary: str = Field(description="Summary of the visualization data")


# =============================================================================
# SYSTEM PROMPT
# =============================================================================
CHART_AGENT_SYSTEM_PROMPT = f"""
You are a data visualization expert that generates JSON data structures for Amazon QuickSight charts.

Your task is to analyze cost data, metrics, or analysis results and transform them into chart-ready JSON formats.

SUPPORTED CHART TYPES:

1. BAR CHART - Use for comparing categories
   Schema: {BarChartData.model_json_schema()}

2. PLOT CHART (Line/Scatter) - Use for trends over time or correlations
   Schema: {PlotChartData.model_json_schema()}

3. HEAT MAP - Use for showing relationships between two categorical variables
   Schema: {HeatMapData.model_json_schema()}

CHART SELECTION GUIDELINES:
- Bar Chart: Cost comparisons, category breakdowns, before/after comparisons
- Plot Chart: Time series, what-if scenarios with varying parameters, trends
- Heat Map: Model vs. volume comparisons, region vs. service costs, multi-dimensional analysis

OUTPUT REQUIREMENTS:
- Return ONLY valid JSON matching the ChartResponse schema
- Include appropriate titles and axis labels
- Use descriptive category names
- Format monetary values consistently (no currency symbols in data, specify in metadata)
- Provide a brief summary explaining the visualization

RESPONSE SCHEMA:
{ChartResponse.model_json_schema()}

RULES:
- Output ONLY the JSON object, nothing else
- No markdown code blocks, no explanations outside the JSON
- Ensure all required fields are present
- Use proper JSON syntax with double quotes
- Numbers must be numeric types, not strings
"""


# =============================================================================
# AGENT CREATION
# =============================================================================
def create_chart_agent() -> Agent:
    """Create and configure the Chart Analysis agent."""
    model = BedrockModel(
        model_id=MODEL_ID,
        temperature=0.1,
    )
    
    return Agent(
        model=model,
        system_prompt=CHART_AGENT_SYSTEM_PROMPT,
        tools=[],
    )


# =============================================================================
# TOOL FUNCTION
# =============================================================================
@tool
def generate_chart_data(data_description: str, chart_type: str = "auto") -> str:
    """
    Generate QuickSight-compatible JSON chart data from analysis results.
    
    This agent transforms cost analysis data, metrics, or scenario results
    into structured JSON that can be used to create visualizations in
    Amazon QuickSight (bar charts, line plots, heat maps).
    
    Args:
        data_description: Description of the data to visualize, including:
            - The metrics or values to chart
            - Categories or time periods
            - Any groupings or series
            - Context about what the data represents
        chart_type: Preferred chart type ('bar_chart', 'plot_chart', 'heat_map', or 'auto')
            If 'auto', the agent will select the most appropriate chart type.
    
    Returns:
        JSON string containing chart data compatible with Amazon QuickSight,
        including chart configuration, data points, and a summary.
    
    Examples:
        >>> generate_chart_data(
        ...     "Monthly Bedrock costs: Jan $1200, Feb $1500, Mar $1800, Apr $2100",
        ...     chart_type="bar_chart"
        ... )
        
        >>> generate_chart_data(
        ...     "Cost comparison: Claude Haiku $500/month, Claude Sonnet $2000/month, "
        ...     "Claude Opus $5000/month for 100K requests",
        ...     chart_type="bar_chart"
        ... )
        
        >>> generate_chart_data(
        ...     "What-if analysis: costs at 10K, 50K, 100K, 500K, 1M requests/month "
        ...     "showing linear scaling from $100 to $10000",
        ...     chart_type="plot_chart"
        ... )
    """
    agent = create_chart_agent()
    
    prompt = f"Data to visualize: {data_description}"
    if chart_type != "auto":
        prompt += f"\nPreferred chart type: {chart_type}"
    
    response = agent(prompt)
    
    if hasattr(response, 'message') and 'content' in response.message:
        return response.message['content'][0]['text']
    return str(response)


# =============================================================================
# MAIN
# =============================================================================
def main():
    """Run the Chart Analysis agent in interactive mode."""
    print("Chart Analysis Agent")
    print("=" * 50)
    print("Generates QuickSight-compatible JSON for visualizations")
    print("Supported: bar_chart, plot_chart, heat_map")
    print("Type 'quit' to exit\n")
    
    agent = create_chart_agent()
    
    while True:
        user_input = input("You: ").strip()
        if user_input.lower() in ["quit", "exit", "q"]:
            print("Goodbye!")
            break
        if not user_input:
            continue
        
        response = agent(user_input)
        print(f"\nAgent: {response}\n")


if __name__ == "__main__":
    main()
