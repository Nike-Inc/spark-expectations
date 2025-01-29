



from spark_expectations.notifications.push.alert import AlertTrial
from spark_expectations.core.context import SparkExpectationsContext
from pyspark.sql import SparkSession

# Define the product_id
product_id = "your_product_id"  # Replace with the appropriate product ID

# Create a Spark session
spark = SparkSession.builder.appName("SparkExpectations").getOrCreate()

# Create an instance of SparkExpectationsContext with the required arguments
_context = SparkExpectationsContext(product_id, spark)

# Create an instance of AlertTrial with the context
instance = AlertTrial(_context)
instance.send_mail("hi", "hi", "sudeepta.pal@nike.com")