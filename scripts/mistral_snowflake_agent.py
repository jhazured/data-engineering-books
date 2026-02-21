import os
import re
from typing import List, Any

try:
    from scripts import snowflake_helper
except ImportError:
    import snowflake_helper

# HuggingFaceEndpoint reads HUGGINGFACEHUB_API_TOKEN from env automatically; do not overwrite.

_llm = None
_rag_chain = None


def get_llm():
    """Lazy-init LLM so module can be imported without valid repo_id/token."""
    global _llm
    if _llm is None:
        repo_id = os.getenv("MISTRAL_REPO_ID", "YOUR_MISTRAL_MODEL_REPO")
        from langchain_community.llms import HuggingFaceEndpoint
        _llm = HuggingFaceEndpoint(repo_id=repo_id, temperature=0.5)
    return _llm


def get_rag_chain():
    """Lazy-init RAG chain using LCEL (prompt | llm | StrOutputParser)."""
    global _rag_chain
    if _rag_chain is None:
        from langchain_core.prompts import PromptTemplate
        from langchain_core.output_parsers import StrOutputParser
        prompt = PromptTemplate.from_template(
            "Answer the question based only on the following context. If the context does not contain enough information, say so.\n\nContext:\n{context}\n\nQuestion: {question}\n\nAnswer:"
        )
        llm = get_llm()
        _rag_chain = prompt | llm | StrOutputParser()
    return _rag_chain


def _run_rag(question: str, context_str: str) -> str:
    """Run RAG chain: single invocation with context and question."""
    chain = get_rag_chain()
    return chain.invoke({"context": context_str, "question": question})


def ask_mistral(question: str) -> str:
    """Simple Q&A (no context)."""
    return str(get_llm().invoke(question))


def personal_mistral(question: str, db: Any, docs: Any = None) -> str:
    """RAG: answer using your book chunks from a vector DB. If docs is provided, use them (one less Snowflake round-trip)."""
    if docs is None:
        docs = db.similarity_search(query=question, k=4)
    else:
        docs = docs[:4] if len(docs) > 4 else docs
    context_str = "\n".join([getattr(doc, "page_content", str(doc)) for doc in docs])
    return _run_rag(question, context_str)


def personal_mistral_snowflake(question: str, db: Any) -> List:
    """
    Search vector DB for context, generate SQL using Mistral, extract code, execute in Snowflake.
    Returns list of results for each query.
    WARNING: Executes LLM-generated SQL. Do not use with untrusted input or production data.
    """
    docs = db.similarity_search(query=question, k=4)
    context_str = "\n".join([doc.page_content for doc in docs])
    result = _run_rag(question, context_str)

    sql_blocks = re.findall(r"```sql(.*?)```", result, re.DOTALL | re.IGNORECASE)
    results_list = []
    if not sql_blocks:
        print("No SQL code found in LLM output.")
        return []

    for sql_code in sql_blocks:
        sql_code = sql_code.strip()
        print("---- SQL ----")
        print(sql_code)
        print("-------------")
        try:
            res = snowflake_helper.snowflake_run_new(sql_code)
            results_list.append(res)
        except Exception as e:
            print(f"Error executing SQL: {e}")
            results_list.append(None)
    return results_list


def mistral_csv(df: Any, question: str) -> Any:
    """Query a pandas DataFrame via Mistral agent."""
    from langchain_experimental.agents.agent_toolkits import create_pandas_dataframe_agent
    df_agent = create_pandas_dataframe_agent(get_llm(), df)

    def handle_parsing_error(error):
        print(f"Error parsing LLM output: {error}")

    response = df_agent.invoke(question, handle_parsing_errors=handle_parsing_error)
    return response.get("output", None)
