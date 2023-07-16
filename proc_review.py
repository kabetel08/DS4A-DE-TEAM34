import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, regexp_extract, regexp_replace, upper, lit

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize SparkContext and GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize Glue job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read the review data from S3 into a Spark DataFrame

reviews_df = spark.read.csv("s3://dataswan/Raw_reviews/review_data.csv", header=True, inferSchema=True)

#Read table which will match doctor names and NPIs
crosswalk_df = spark.read.csv("s3://dataswan/Processed_Payments/Crosswalk_2020/crosswalk2020.csv", header=True, inferSchema=True)

# Define the regular expression pattern that will extract the names from the url
pattern = r'(?<=https://www.vitals.com/doctors/Dr_)([A-Za-z_]+)(?=/reviews)'

# Extract the doctor names from url and create a new column
reviews_df = reviews_df.withColumn("Doctor_Name", regexp_extract(col("sourceurl"), pattern, 1))

# Remove underscores from the values in the 'Doctor_Name' column
reviews_df = reviews_df.withColumn("Doctor_Name2", regexp_replace(col("Doctor_Name"), "_", ""))

# Convert the 'Doctor_Name' column to uppercase
reviews_df = reviews_df.withColumn("Doctor_Name", upper(col("Doctor_Name")))
reviews_df = reviews_df.withColumn("Doctor_Name2", upper(col("Doctor_Name2")))

# Extract website source
reviews_df = reviews_df.withColumn("source", regexp_extract(col("sourceurl"), r"(?:https?://)?(?:www\.)?([^/]+)", 1))

# Convert rating to float if not already
reviews_df = reviews_df.withColumn("rating_value", reviews_df["rating_value"].cast(FloatType()))

# Rename Id to reviewid
reviews_df = reviews_df.withColumnRenamed("id", "reviewid")

crosswalk_df = crosswalk_df.withColumnRenamed("Name_ID_y", "Doctor_Name2")

# Join the DataFrames
merged_df = reviews_df.join(crosswalk_df, "Doctor_Name2", "inner")

# Convert 'date' column to proper date format and filter by date
merged_df = merged_df.withColumn('date', F.to_date('date', 'M/d/y')) \
    .filter((F.col('date') < F.lit('2021-01-01')))

# Function to remove special characters from a column
def remove_special_chars(column):
    cleaned_column = regexp_replace(column, "[^a-zA-Z0-9\s]+", "")
    return cleaned_column

# Apply the function to non-numeric columns
for column in merged_df.columns:
    column_type = merged_df.schema[column].dataType
    if (not isinstance(column_type, FloatType)) and (column != "source" and column != "RATING_VALUE" and column != "date" and column != "Doctor_Name"):
        merged_df = merged_df.withColumn(column, remove_special_chars(col(column)))

# Select columns for review fact and dimension
revfact = ['COVERED_RECIPIENT_NPI', 'REVIEWID', 'RATING_VALUE']
review_fact = merged_df.select(*revfact)

revdim = ['COVERED_RECIPIENT_NPI', 'REVIEWID', 'DOCTOR_NAME', 'DATE', 'SOURCE']
review_dim = merged_df.select(*revdim)

# Write the DataFrames to CSV
review_fact.repartition(1).write.mode("overwrite").csv("s3://dataswan/Processed_Reviews/Review_Fact.csv", header=True)
review_dim.repartition(1).write.mode("overwrite").csv("s3://dataswan/Processed_Reviews/Review_Dim.csv", header=True)

# Commit the Glue job
job.commit()
