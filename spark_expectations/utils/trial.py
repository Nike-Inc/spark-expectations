import pandas as pd

class DataProcessor:
    def _init_(self, context):
        self.context = context

    def dataload(self):
        a = self.context.get_detailed_dataframe()
        b = self.context.get_detailed_dataframe1()
        c = a + b  # Assuming the DataFrames can be added directly
        return c