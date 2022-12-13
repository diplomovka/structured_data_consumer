class StructuredData:
    def __init__(self, data_hash, table_name, insert_query, columns_names, table_creation_queries):
        self.data_hash = data_hash
        self.table_name = table_name
        self.insert_query = insert_query
        self.columns_names = columns_names
        self.table_creation_queries = table_creation_queries
