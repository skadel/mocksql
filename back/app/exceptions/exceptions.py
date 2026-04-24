class TableNotFoundException(Exception):
    def __init__(self, table):
        self.table = table
        super().__init__(f"Table '{table}' not found in the list of tables.")
