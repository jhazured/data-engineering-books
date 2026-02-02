import os
import re
from langchain_experimental.agents.agent_toolkits import create_pandas_dataframe_agent
from langchain_community.llms import HuggingFaceEndpoint
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
try:
    from scripts import snowflake_helper
except ImportError:
    import snowflake_helper
_snowflake_run = snowflake_helper.snowflake_run_new
from typing import List

# -----------------------------
# 1️⃣ HuggingFace / Mistral setup (generic)
# -----------------------------
os.environ["HUGGINGFACEHUB_API_TOKEN"] = os.getenv("HUGGINGFACEHUB_API_TOKEN")  # set your token in environment
repo_id = "YOUR_MISTRAL_MODEL_REPO"  # Replace with your Mistral model repo name
llm = HuggingFaceEndpoint(repo_id=repo_id, temperature=0.5)

# -----------------------------
# 2️⃣ PromptTemplate
# -----------------------------
template = """
Following is the context. Based on the context, answer the question:
Context:
{context}

Question:
{question}
"""
prompt_final = PromptTemplate(template=template, input_variables=['context', 'question'])
llm_chain = LLMChain(prompt=prompt_final, llm=llm)

# -----------------------------
# 3️⃣ Simple Mistral QA
# -----------------------------
def ask_mistral(question: str) -> str:
    return llm(question)

# -----------------------------
# 4️⃣ Personal QA using vector DB
# -----------------------------
def personal_mistral(question: str, db) -> str:
    docs = db.similarity_search(query=question, fetch_k=4)
    context_str = "\n".join([doc.page_content for doc in docs])
    return llm_chain.run(question=question, context=context_str)

# -----------------------------
# 5️⃣ Personal Snowflake QA
# -----------------------------
def personal_mistral_snowflake(question: str, db) -> List:
    """
    Search vector DB for context, generate SQL using Mistral, extract code, execute in Snowflake
    Returns list of results for each query.
    """
    docs = db.similarity_search(query=question, fetch_k=4)
    context_str = "\n".join([doc.page_content for doc in docs])
    
    result = llm_chain.run(question=question, context=context_str)
    
    # Extract SQL code blocks using regex
    sql_blocks = re.findall(r"```sql(.*?)```", result, re.DOTALL | re.IGNORECASE)
    
    results_list = []
    if not sql_blocks:
        print("⚠️ No SQL code found in LLM output.")
        return []
    
    for sql_code in sql_blocks:
        sql_code = sql_code.strip()
        print("---- SQL ----")
        print(sql_code)
        print("-------------")
        # Execute SQL using Snowflake helper
        try:
            res = _snowflake_run(sql_code)
            results_list.append(res)
        except Exception as e:
            print(f"❌ Error executing SQL: {e}")
            results_list.append(None)
    
    return results_list

# -----------------------------
# 6️⃣ Pandas / CSV Agent
# -----------------------------
def mistral_csv(df, question: str):
    df_agent = create_pandas_dataframe_agent(llm, df)
    
    def handle_parsing_error(error):
        print(f"⚠️ Error parsing LLM output: {error}")
    
    response = df_agent.invoke(question, handle_parsing_errors=handle_parsing_error)
    return response.get('output', None)
