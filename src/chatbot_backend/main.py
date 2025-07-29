from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from agent.PlanExecuteAgent import PlanExecuteAgent
from agent.utils import get_bin_data
import os
from cachetools import TTLCache
from uuid import uuid4
from threading import Lock
import json

os.environ["OPENAI_API_KEY"] = ""

agents = TTLCache(maxsize=20, ttl=1800)
agent_lock = Lock()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ChatRequest(BaseModel):
    message: str
    sessionId: str

class InitializeRequest(BaseModel):
    parsedMessages: dict

@app.post("/api/initialize")
async def initialize_endpoint(req: InitializeRequest):
    parsed_messages = req.parsedMessages
    if not parsed_messages:
        return {"error": "No messages provided for initialization"}
    
    dataframes = get_bin_data(parsed_messages)
    session_id = str(uuid4())
    agent = PlanExecuteAgent(dataframes, session_id)
    with agent_lock:
        agents[session_id] = agent
    return {"message": "Agent initialized successfully", "session_id": session_id}

# @app.post("/api/chat")
# def chat_endpoint(req: ChatRequest):
#     with agent_lock:
#         agent = agents.get(req.sessionId)
#     if not agent:
#         return {"error": "Invalid or expired session ID"}
#     answer = agent.call(req.message)
#     return {"response": answer}

@app.get("/api/chat/stream")
def chat_stream_endpoint(sessionId: str, message: str):
    with agent_lock:
        agent = agents.get(sessionId)
    if not agent:
        def error_stream():
            yield json.dumps({"error": "Invalid or expired session ID"})
        return StreamingResponse(error_stream(), media_type="application/json")
    
    def event_stream():
        for step in agent.call_stream(message):
            print("step", step)
            yield f"data: {json.dumps(step)}\n\n"
    return StreamingResponse(event_stream(), media_type="text/event-stream")