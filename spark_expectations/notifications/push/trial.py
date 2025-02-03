import os
import traceback

import pandas as pd
from jinja2 import Environment, FileSystemLoader
from matplotlib.pyplot import title

# Sample DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Create DataFrame Example") \
    .getOrCreate()

# Define the schema
schema = StructType([
    StructField("column1", StringType(), True),
    StructField("column2", StringType(), True),
    StructField("status", StringType(), True)
])

# Sample data
data = [
    ("value1", "value4", "pass"),
    ("value2", "value5", "fail"),
    ("value3", "value6", "pass")
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Convert DataFrame to list of dictionaries

# Define the template directory and file
template_dir = os.path.join(os.path.dirname(__file__), 'templates')
template_file = 'advanced_email_alert_template.jinja'

# Load the template
env_loader = Environment(loader=FileSystemLoader(template_dir))
template = env_loader.get_template(template_file)
headers =list(df.columns)
rows = [row.asDict().values() for row in df.collect()]


# Define the data dictionary for the template
# template_data = {
#     "title": "Sample Data Report",
#     "headers": df.columns.tolist(),
#     "rows": data_dict
# }

# Render the template
try:
    html_data = template.render(title="hi",headers=headers,rows=rows)
    print("Template rendered successfully")

    print(html_data)
except Exception as e:
    print(f"Error rendering template: {e}")
    traceback.print_exc()