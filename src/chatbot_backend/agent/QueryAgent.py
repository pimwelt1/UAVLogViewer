import os
from .utils import is_safe_query
from pandasql import sqldf
import json
from langgraph.graph import StateGraph
from IPython.display import Image, display
from typing import TypedDict, Annotated, List, Optional
from langchain_core.messages import SystemMessage, HumanMessage
from pydantic import BaseModel
from langgraph.errors import GraphRecursionError

class QueryAgentState(TypedDict):
    documentation: str
    question: str
    sql_query: Optional[str]
    sql_result: Optional[str]
    sql_error: Optional[str]
    attempts: List[str]
    final_answer: Optional[str]

class Query(BaseModel):
    query: Annotated[str, ..., "Syntactically valid SQL query."]

WRITE_QUERY_PROMPT = (
    "You are an expert data analyst. Given a user's question about a dataset and the dataset documentation,"
    "Write a syntactically valid SQL query that will answer the question"
    "Only output the SQL query, nothing else."
    "The query should target specific columns."
    "The result of the query should be a table with no more than 30 rows!."
    
)

REWRITE_QUERY_PROMPT = (
    "You are an expert data analyst. Given a user's question about a dataset, "
    "the dataset documentation, and a previously failed SQL query, "
    "write a corrected SQL query that avoids the same error. "
    "Only output the SQL query, nothing else."
    "The query should target specific columns."
    "The result of the query should be a table with no more than 30 rows!."
)

ANALYZE_RESULT_PROMPT = (
    "You are an expert data analyst. Given the user's question, the SQL query used, "
    "and the result of that query, analyze the result and provide a concise insight or summary. "
    "Focus on what the result means in the context of the question, you can find the unit of the columns in the documentation."
)

class QueryAgent:
    def __init__(self, data, documentation, model) -> None:
        self.data = data
        self.documentation = documentation
        self.model = model

        builder = StateGraph(QueryAgentState)
        builder.add_node("write_query", self.write_query)
        builder.add_node("execute_query", self.execute_query)
        builder.add_node("replan_query", self.replan_query)
        builder.add_node("analyze_result", self.analyze_result)

        builder.set_entry_point("write_query")
        builder.add_edge("write_query", "execute_query")

        builder.add_conditional_edges(
            "execute_query",
            lambda state: "sql_result" if ("sql_result" in state and state["sql_result"]) else "sql_error",
            {
                "sql_result": "analyze_result",
                "sql_error": "replan_query"
            }
        )

        builder.add_edge("replan_query", "execute_query")
        self.query_agent = builder.compile()
        # display(Image(self.query_agent.get_graph(xray=True).draw_mermaid_png()))

    
    def write_query(self, state: QueryAgentState):
        messages = [
            SystemMessage(content=WRITE_QUERY_PROMPT),
            HumanMessage(content=f"Here is the question: {state['question']}, here is the dataset documentation: {self.documentation}\n")
        ]
        query = self.model.with_structured_output(Query).invoke(messages)
        return {"sql_query": query.query, "attempts": state["attempts"] + [query]}

    def execute_query(self, state: QueryAgentState):
        if not is_safe_query(state['sql_query']):
            return {"sql_result": None, "sql_error": f"The query {state['query']} is not safe to execute."}
        try:
            df = sqldf(state['sql_query'], self.data)
            if len(df) > 100:
                df = df.head(100)
            result = json.dumps(df.to_dict(orient="records"), indent=2)
            return {"sql_result": result, "sql_error": None}
        except Exception as e:
            return {"sql_result": None, "sql_error": str(e)}
    
    def replan_query(self, state: QueryAgentState):
        if len(state["attempts"]) >= 5:
            return {"final_answer": "Failed after 5 attempts."}
        prompt = f"""
            The previous SQL query failed.
            Original question: {state['question']}
            Last query attempt: {state['sql_query']}
            Error: {state['sql_error']}
            Previous attempts: {state['attempts']}

            Please write a corrected SQL query that avoids the same error.
        """
        messages = [
            SystemMessage(content=REWRITE_QUERY_PROMPT),
            HumanMessage(content=prompt)
        ]
        new_query = self.model.invoke(messages).content
        return {"sql_query": new_query, "attempts": state["attempts"] + [new_query]}

    def analyze_result(self, state: QueryAgentState):
        messages = [
            SystemMessage(content=ANALYZE_RESULT_PROMPT),
            HumanMessage(content=f"Here is the question: {state['question']}\nHere is the query: {state['sql_query']}\nHere is the result: {state['sql_result']}\n Here is the documentation: {self.documentation}\n")
        ]
        response = self.model.invoke(messages).content
        return {"final_answer": response}

    def call(self, question):
        final_step = None
        try:
            for step in self.query_agent.stream({
                "question": question,
                "attempts": [],
            }, config={"recursion_limit": 5}):
                final_step = step
            return final_step['analyze_result']["final_answer"] if final_step else "Please reformulate."
        
        except GraphRecursionError as e:
            return "I couldn't access the information"
        
        except Exception as e:
            return "Something went wrong. Please try again."