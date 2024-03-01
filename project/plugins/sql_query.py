import pandas as pd
from langchain_community.utilities import SQLDatabase

from taskweaver.plugin import Plugin, register_plugin


@register_plugin
class SqlQuery(Plugin):
    db = None

    def __call__(self, query: str):
        if self.db is None:
            self.db = SQLDatabase.from_uri(self.get_config("db_uri"))

        result = self.db._execute(query, fetch="all")

        df = pd.DataFrame(result)

        description = df.head(min(5, len(df))).to_markdown()

        print(description)

        return df, description


if __name__ == "__main__":
    from taskweaver.plugin.context import temp_context

    with temp_context() as temp_ctx:
        query = SqlQuery(name="sql_query", ctx=temp_ctx, config={
            "db_uri": "postgresql://dke:DATAV123@k8s-hw-master:5433/dkedb"
        })
        query(query="select * from abi.t_jzg_jbxx")
