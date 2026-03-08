from typing import List
from models.entity import LegalEntity
from langchain_openai import ChatOpenAI        # NEW — correct
from langchain_core.messages import HumanMessage   # NEW — correct
from dotenv import load_dotenv
import os

llm = ChatOpenAI(
    model_name="gpt-4",
    temperature=0,
    openai_api_key=os.getenv("OPENAI_API_KEY")
)

def query_entities_with_llm(entities: List[LegalEntity], question: str) -> str:
    context = "\n".join([f"{e.lei}: {e.name}, {e.country}, {e.status}" for e in entities])
    prompt = f"""
    You are an expert in legal entity data.
    Only use the following data to answer questions. Do not make assumptions.

    Data:
    {context}

    Question: {question}
    """
    response = llm([HumanMessage(content=prompt)])
    return response.content
