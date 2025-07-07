import sqlite3
import operator
from typing import TypedDict, Annotated, List, Tuple, Union
from typing_extensions import TypedDict
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.sqlite import SqliteSaver
from .utils import get_bin_documentation
from .QueryAgent import QueryAgent
from IPython.display import Image, display
from pydantic import BaseModel, Field
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class PlanExecuteState(TypedDict):
    input: str
    plan: List[str]
    past_steps: Annotated[List[Tuple], operator.add]
    response: str
    conversation_history: Annotated[List[Tuple[str, str]], operator.add]

class Query(BaseModel):
    question: str = Field(description="Natural language question to query")

class Analysis(BaseModel):
    table_name: str = Field(description="Table to analyze")

class Plan(BaseModel):
    steps: List[Union[Query, Analysis]] = Field(description="Steps to follow")

class DirectResponse(BaseModel):
    response: str = Field(description="Direct answer or a request for clarification")

class TreatUserInput(BaseModel):
    action: Union[Plan, DirectResponse] = Field(
        description="If the input is clear and answerable, respond directly. "
                    "If it requires multiple steps, return a plan. "
                    "If the question is unclear, ask a clarification question."
    )

class Response(BaseModel):
    """Response to user."""
    response: str


class Act(BaseModel):
    """Action to perform."""
    action: Union[Response, Plan] = Field(
        description="Action to perform. If you want to respond to user, use Response. "
        "If you need to further use tools to get the answer, use Plan."
    )

PLAN_OR_RESPONSE_PROMPT = """
    You are deciding whether a user query requires action (e.g., calling a tool, running a query), or can be answered directly with available context.
    If the input is clear and can be answered directly, respond with a definition or explanation based on the provided documentation. 
    If it requires multiple steps or a query to answer, return a plan with steps to follow. 
    5 steps is the maximum you should return in a plan.

    IMPORTANT DECISION RULES:
    - For specific questions (e.g., "What was max altitude?", "When did signal drop?"): Use Query, your questions must mention the relevant table and the relevant columns, e.g., "What was the maximum altitude from GPS table?"
    - For exploratory analysis (e.g., "Are there anomalies?", "What's wrong?"): Use Analysis first, then Query about informative columns
    - For definitions/explanations: Respond directly

    IMPORTANT: When creating a plan, use these exact formats:
    - For queries: {{"question": "your natural language question"}}
    - For analysis: {{"table_name": "table_name"}}

    You are provided with documentation:
    {documentation}
    For broad questions, you need to look at the documentation to know which columns might be relevant.

    Examples:
    Q: What is a GPS fix_type?  
    A: Respond (explain based on documentation)

    Q: What was the duration of the flight?
    A: Plan: [{{"question": "What long was the flight according to the GPS data ?"}}]

    Q: Are there anomalies in the GPS data?  
    A: Plan: [Analysis(table_name='GPS_0'), Analysis(table_name='ATT'), Query(question='Are there any anomalies in the ATT data based on the DesRoll, DesPitch, and ErrRP values?), ...]

    Q: What does ERR_YAW mean?  
    A: Respond (definition from documentation)
"""

REPLAN_PROMPT = """
    You are a replanner that decides whether to continue with more steps or provide a final answer.
    You are given the user's original question, the last plan you created, and the steps you have already completed.
    If the past steps have NOT fully answered the user's question, keep going with the plan and return a new plan with the remaining steps.

    Your task is to analyze the current situation and decide:
    1. If more steps are still needed to complete the objective, return a Plan with the remaining steps, run a query or an analysis of a table main statistics step
    2. If the objective is complete and you can provide a final answer, return a Response

    If an analysis reveal interesting information, you might want to query the table to learn more

    IMPORTANT DECISION CRITERIA:
    - If the past steps have NOT fully answered the user's question, return a Plan with the remaining steps
    - If the past steps HAVE fully answered the user's question, return a Response with the final answer
    - Only return a Response when you have enough information to completely and confidently answer the question
    - Never do a step you already did in the past steps

    Format:
    - Plan: [{{"question": "step 1"}}, {{"table_name": "step 2"}}]
    - Response: "final answer"

    You are provided with documentation:
    {documentation}
    For broad questions, you need to look at the documentation to know which columns might be relevant.

    EXAMPLES:

    Example 1:
    User Question: "What was the maximum altitude?"
    Previous Step: Ran SQL query on wrong table, got NULLs.
    → Plan: [{{"question": "What is the maximum altitude from the GPS table?"}}]

    Example 2:
    User Question: "Are there any anomalies in the GPS data?"
    Previous Step: Analyse the GPS table.
    → Plan: [Analysis(table_name='ATT'), Query(question='Are there any anomalies in the ATT data based on the DesRoll, DesPitch, and ErrRP values?), ...]

    Example 3:
    User Question: "When did RC signal first drop?"
    Previous Step: Found time_boot_ms = 34000ms with signal_strength < threshold.
    → Response: "The first RC signal drop occurred at 34000ms."
"""

class PlanExecuteAgent:
    def __init__(self, data, session_id):
        sqlite_conn = sqlite3.connect(f"./checkpoints/{session_id}_checkpoints.sqlite", check_same_thread=False)
        self.memory = SqliteSaver(sqlite_conn)
        self.session_id = session_id
        self.analysis_cache = {}
        self.model = ChatOpenAI(model="gpt-4o", 
                                temperature=0,
                                max_tokens=1000,
                                request_timeout=10,
                                max_retries=2)

        self.data = data
        self.documentation = get_bin_documentation(self.data)
        self.query_agent = QueryAgent(self.data, self.documentation, self.model)


        builder = StateGraph(PlanExecuteState)
        builder.add_node("get_plan_or_response", self.get_plan_or_response)
        builder.add_node("agent", self.execute_step)
        builder.add_node("replan", self.replan)
        builder.set_entry_point("get_plan_or_response")
        builder.add_conditional_edges(
            "get_plan_or_response",
            lambda state: "agent" if 'plan' in state and state['plan'] else "update_history",
            ["agent", "update_history"],
        )
        builder.add_edge("agent", "replan")
        builder.add_node("update_history", self.update_conversation_history)
        builder.add_edge("update_history", END)
        builder.add_conditional_edges(
            "replan",
            lambda state: "update_history" if ("response" in state and state["response"]) else "agent",
            ["agent", "update_history"],
        )
        self.graph = builder.compile(checkpointer=self.memory)
        # display(Image(self.graph.get_graph(xray=True).draw_mermaid_png()))


    def call_query_agent(self, question) -> str:
        """
            Generates a SQL query based on a flight-related user question and runs it on the telemetry database.
            Ideal for questions like: 'What was the maximum altitude?' or 'When did signal loss first occur?'
        """
        logger.info(f"Querying DB: {question}")
        answer = self.query_agent.call(question)
        logger.info(f"Got answer: {answer}")
        return answer
        
    def analyse_data(self, table_name: str) -> str:
        """
        Prepares a high-signal summary of telemetry table statistics.
        """
        logger.info(f"Analysing table: {table_name}")
        cache_key = f"analysis_{table_name}"
        if cache_key in self.analysis_cache:
            return self.analysis_cache[cache_key]
        
        if table_name not in self.data:
            error_msg = f"Error: Table '{table_name}' not found. Available tables: {list(self.data.keys())}"
            self.analysis_cache[cache_key] = error_msg
            return error_msg
        df = self.data[table_name]
        df = df.select_dtypes(include="number")

        if df.empty:
            return f"Table {table_name} has no numeric telemetry data."

        summary = [f"Summary of `{table_name}` Table\n"]

        for col in df.columns:
            col_data = df[col].dropna()
            if len(col_data) == 0:
                continue

            line = f"- **{col}**: "
            stats = {
                "min": col_data.min(),
                "max": col_data.max(),
                "mean": col_data.mean(),
                "std": col_data.std(),
                "skew": col_data.skew(),
                "kurt": col_data.kurtosis(),
                "n_unique": col_data.nunique()
            }

            notes = []
            if stats["n_unique"] == 1:
                notes.append("constant")
            elif stats["std"] > 2 * abs(stats["mean"]):
                notes.append("high variance")
            elif stats["std"] == 0:
                notes.append("no variation")
            if abs(stats["skew"]) > 2:
                notes.append("strongly skewed")
            if abs(stats["kurt"]) > 10:
                notes.append("peaked or heavy tails")

            line += (
                f"min={stats['min']:.2f}, max={stats['max']:.2f}, "
                f"mean={stats['mean']:.2f}, std={stats['std']:.2f}, "
                f"skew={stats['skew']:.2f}, kurtosis={stats['kurt']:.2f}, "
                f"unique={stats['n_unique']}"
            )
            if notes:
                line += f" — _{' | '.join(notes)}_"

            summary.append(line)

        result = "\n".join(summary)
        self.analysis_cache[cache_key] = result
        
        if len(self.analysis_cache) > 20:
            oldest_key = next(iter(self.analysis_cache))
            del self.analysis_cache[oldest_key]

        return "\n".join(summary)
    
    def get_plan_or_response(self, state):
        conversation_context = ""
        if state.get("conversation_history"):
            conversation_context = "\n\nPrevious conversation:\n"
            for i, (user_input, agent_response) in enumerate(state["conversation_history"]):
                conversation_context += f"User: {user_input}\nAgent: {agent_response}\n"
        
        messages = [
            SystemMessage(content=PLAN_OR_RESPONSE_PROMPT.format(documentation=self.documentation)),
            HumanMessage(content=f"Here is the user input: {state['input']}{conversation_context}")
        ]
        result = self.model.with_structured_output(TreatUserInput).invoke(messages)
        
        if isinstance(result.action, Plan):
            logger.info(f"Got Plan: {result.action.steps}")
            return {"plan": result.action.steps}
        else:
            logger.info(f"Answering: {result.action.response}")
            return {"response": result.action.response}

    def execute_step(self, state):
        if len(state["plan"]) == 0:
            return {"past_steps": [(None, "")]}
        step = state["plan"][0]  # Get first step
        
        if isinstance(step, Query):
            logger.info(f"Executing query: {step.question}")
            result = self.query_agent.call(step.question)
        elif isinstance(step, Analysis):
            logger.info(f"Executing analysis: {step.table_name}")
            result = self.analyse_data(step.table_name)
        else:
            logger.error(f"Unknown step type: {type(step)}")
            result = "Error: Unknown step type"
        
        return {"past_steps": [(step, result)]}

    def replan(self, state):
        conversation_context = ""
        if state.get("conversation_history"):
            conversation_context = "\n\nPrevious conversation:\n"
            for i, (user_input, agent_response) in enumerate(state["conversation_history"]):
                conversation_context += f"User: {user_input}\nAgent: {agent_response}\n"

        prompt = f"""Your objective was this: {state['input']}\n
                        Your last plan was this: {state['plan']}\n
                        You have currently done the following steps: {state['past_steps']}\n
                        Don't repeat any steps you already did.
                        Based on the completed steps, decide what are the next steps"""
        if len(state["past_steps"]) > 5:
            prompt += "\n\nYou have already done many steps, it is time to give a response."
        
        messages = [
            SystemMessage(content=REPLAN_PROMPT.format(documentation=self.documentation)),
            HumanMessage(content=prompt)
        ]
        output = self.model.with_structured_output(Act).invoke(messages)
        if isinstance(output.action, Response):
            return {"response": output.action.response}
        else:
            completed_steps = [step for step, _ in state["past_steps"]]
            new_plan = [
                step for step in output.action.steps if step not in completed_steps
            ]
            logger.info(f"Last step: {state['past_steps'][-1]}, New Plan: {new_plan}")
            return {"plan": new_plan}

    def update_conversation_history(self, state):
        logger.info("Updating conversation history")
        if len(state["conversation_history"]) >= 10:
            state["conversation_history"].pop(0)
        return {
            "conversation_history": [(state["input"], state["response"])]
        }

    def call(self, input_question: str) -> str:
        thread = {"configurable": {"thread_id": self.session_id}}
        final_step = None
        logger.info(f"Received user question: {input_question}")
        for step in self.graph.stream({
            "input": input_question,
            "plan": [],
            "past_steps": [],
            "response": "",
            "conversation_history": [],
        }, thread):
            final_step = step

        try:
            return final_step['update_history']["conversation_history"][-1][1]
        except Exception as e:
            return "Please reformulate."