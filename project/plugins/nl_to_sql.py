from http import HTTPStatus
from operator import itemgetter

import dashscope
from langchain.prompts import ChatPromptTemplate
from langchain.schema.output_parser import StrOutputParser
from langchain.schema.runnable import RunnableMap
from langchain_core.runnables import RunnableLambda
from langchain_openai import AzureChatOpenAI, ChatOpenAI

from taskweaver.plugin import Plugin, register_plugin


@register_plugin
class NlToSql(Plugin):

    def __call__(self, query: str):
        api_type = self.get_config("api_type", "azure")  # self.config.get("api_type", "azure")
        qwen = None
        if api_type == "azure":
            model = AzureChatOpenAI(
                azure_endpoint=self.get_config("api_base"),
                openai_api_key=self.get_config("api_key"),
                openai_api_version=self.get_config("api_version"),
                azure_deployment=self.get_config("deployment_name"),
                temperature=0,
                verbose=True,
            )
        elif api_type == "openai":
            model = ChatOpenAI(
                openai_api_key=self.get_config("api_key"),
                openai_api_base=self.get_config("api_base"),
                model_name=self.get_config("deployment_name"),
                temperature=0,
                verbose=True,
            )
        elif api_type == "qwen":
            qwen = dashscope
            qwen.api_key = self.get_config("api_key")
        else:
            raise ValueError("Invalid API type. Please check your config file.")

        template = """Based on the table schema below, write a SQL query that would answer the user's question:\n
            {schema}

            Question: {question}\n
            Please only write the sql query.\n
            Do not add any comments or extra text.\n
            Do not wrap the query in quotes or ```sql.\n
            SQL结果中的列名使用中文名称（使用AS语句），可参考引用schema中的中文名称。\n
            SQL Query:"""
        prompt = ChatPromptTemplate.from_template(template)

        schema = """
        数据库：PostgreSQL\n
        表名：abi.t_jzg_jbxx（教职工基本信息）\n
        字段列表：\n
            gh：工号\n
            xm：姓名\n
            lb：类别，0=教工，1=职工，2=其他\n
            xb：性别，0=男，1=女，2=其他\n
        """

        def get_schema(_):
            return schema  # todo: return self.db.get_table_info()

        inputs = {
            "schema": RunnableLambda(get_schema),
            "question": itemgetter("question"),
        }

        sql = None
        if qwen is not None:  # 直接用qwen api
            response = qwen.Generation.call(
                model=self.get_config("api_model"),
                prompt=template.format(schema=schema, question=query),
                temperature=0,
            )

            if response.status_code == HTTPStatus.OK:
                sql = response.output.text
            else:
                raise Exception(
                    f"QWen API call failed with code {response.code} and error message {response.message}",
                )

        else:  # default to *opanai
            sql_response = RunnableMap(inputs) | prompt | model.bind(stop=["\nSQLResult:"]) | StrOutputParser()
            sql = sql_response.invoke({"question": query})

        print(f"用户问题：{query}\n结果SQL: {sql}")

        return sql


if __name__ == "__main__":
    from taskweaver.plugin.context import temp_context

    with temp_context() as temp_ctx:
        query = NlToSql(name="nl_to_sql", ctx=temp_ctx, config={
        })
        query(query="统计男女教师分别占全体教职工总人数的百分比")
