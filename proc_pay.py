import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, concat_ws, regexp_replace, date_format, upper, to_date
from pyspark.sql.types import FloatType


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read the Payment data from S3 into a Spark DataFrame (replace with proper year)
payment_df = spark.read.csv("s3://dataswan/Raw_Payments/OP_DTL_GNRL_PGYR2020_P01202023.csv", header=True, inferSchema=True)

needcols=['COVERED_RECIPIENT_NPI','RECORD_ID','Number_of_Payments_Included_in_Total_Amount','Total_Amount_of_Payment_USDollars','RECIPIENT_CITY','RECIPIENT_STATE','RECIPIENT_COUNTRY','RECIPIENT_ZIP_CODE','DATE_OF_PAYMENT','Form_of_Payment_or_Transfer_of_Value','COVERED_RECIPIENT_FIRST_NAME','COVERED_RECIPIENT_MIDDLE_NAME','COVERED_RECIPIENT_LAST_NAME','COVERED_RECIPIENT_TYPE','TEACHING_HOSPITAL_NAME','COVERED_RECIPIENT_PRIMARY_TYPE_1','COVERED_RECIPIENT_SPECIALTY_1','INDICATE_DRUG_OR_BIOLOGICAL_OR_DEVICE_OR_MEDICAL_SUPPLY_1','PRODUCT_CATEGORY_OR_THERAPEUTIC_AREA_1','PROGRAM_YEAR']
payment_df=payment_df.select(*needcols)

def remove_special_chars(column):
    cleaned_column = regexp_replace(column, "[^a-zA-Z0-9\s]+", "")
    return cleaned_column

# Apply the function to non-numeric columns, make names uppercase
for column in payment_df.columns:
    column_type = payment_df.schema[column].dataType
    if (not isinstance(column_type, (FloatType))) and column != "DATE_OF_PAYMENT" and column != "Total_Amount_of_Payment_USDollars" and column != "NUMBER_OF_PAYMENTS_INCLUDED":
        payment_df = payment_df.withColumn(column, remove_special_chars(col(column)))
        payment_df = payment_df.withColumn(column, upper(col(column)))

# Concatenate first and last name in a column named Name_ID
payment_df = payment_df.withColumn("Name_ID", concat_ws("", col("covered_recipient_first_name"), col("covered_recipient_last_name")))

# Shorten Recipient Zip Code to 5 digits
payment_df = payment_df.withColumn("Recipient_Zip_Code", col("Recipient_Zip_Code").cast("string").substr(1, 5).cast("int"))

# Convert Date of Payment to mm/dd/yyyy format
#payment_df = payment_df.withColumn("Date_of_Payment", date_format(col("Date_of_Payment").cast("string"), "MMddyyyy").cast("date"))
#payment_df = payment_df.withColumn("Date_of_Payment", date_format(col("Date_of_Payment"), "MM/dd/yyyy"))

# Remove special characters from Total Amount of Payment and convert it to float
payment_df = payment_df.withColumn("Total_Amount_of_Payment", regexp_replace(col("Total_Amount_of_Payment_USDollars"), "[^\\d.]", "").cast(FloatType()))

# Rename columns
payment_df = payment_df.withColumnRenamed("Record_ID", "Payment_ID") \
    .withColumnRenamed("Number_of_Payments_Included_in_Total_Amount", "Number_of_Payments_Included") \
    .withColumnRenamed("Form_of_Payment_or_Transfer_of_Value","Form_of_Payment")
    
payment_df= payment_df.withColumn("age", col("Payment_ID").cast("integer"))

    

# Group by 'covered_recipient_npi' and select the maximum 'Name_ID'
crosswalk_df = payment_df.groupBy('covered_recipient_npi').agg({'Name_ID': 'max'}).withColumnRenamed('max(Name_ID)', 'Name_ID')

# Convert 'Name_ID' to uppercase
crosswalk_df = crosswalk_df.withColumn('Name_ID', upper(col('Name_ID')))

# Drop duplicates based on 'Name_ID'
crosswalk_df = crosswalk_df.dropDuplicates(['Name_ID'])

crosswalk_df = crosswalk_df.withColumnRenamed('Name_ID','Name_ID_y')

# Merge 'Payment' and 'Crosswalk' based on 'covered_recipient_npi'
merged_df = payment_df.join(crosswalk_df, 'covered_recipient_npi', 'left')

merged_df = merged_df.drop('Name_ID')

# Drop rows with empty 'Name_ID'
merged_df = merged_df.dropna(subset=['Name_ID_y'])

# Drop rows with empty "Total_Amount_of_Payment"
merged_df = merged_df.dropna(subset=["Total_Amount_of_Payment"])

# Drop rows with empty "Date_of_Payment"
merged_df = merged_df.dropna(subset=["Date_of_Payment"])

merged_df = merged_df.withColumnRenamed('Name_ID_y','Name_ID')

#Write crosswalk table to s3 to match with reviews later. We do this so we can append the npi to reviews.
crosswalk_df.repartition(1).write.mode("overwrite").csv("s3://dataswan/Processed_Payments/Crosswalk_2020/crosswalk2020.csv",header=True)

#Get rid of _1 from column names
merged_df = merged_df.toDF(*[col_name.replace("_1", "") for col_name in merged_df.columns])

#Select relevant columns for each table and write to s3
PAYFACTCOLS = ['COVERED_RECIPIENT_NPI','PAYMENT_ID','TOTAL_AMOUNT_OF_PAYMENT','NUMBER_OF_PAYMENTS_INCLUDED']
PAYMENT_FACT=merged_df.select(*PAYFACTCOLS)
PAYMENT_FACT.repartition(1).write.mode("overwrite").csv("s3://dataswan/Processed_Payments/Payment_Fact.csv",header=True)

LOCATIONCOLS = ['COVERED_RECIPIENT_NPI','RECIPIENT_CITY','RECIPIENT_STATE','RECIPIENT_COUNTRY','RECIPIENT_ZIP_CODE']
LOCATION_DIM=merged_df.select(*LOCATIONCOLS)
LOCATION_DIM.repartition(1).write.mode("overwrite").csv("s3://dataswan/Processed_Payments/Location_Dim.csv",header=True)

PAYDIMCOLS=['COVERED_RECIPIENT_NPI','PAYMENT_ID','DATE_OF_PAYMENT','FORM_OF_PAYMENT']
PAY_DIM=merged_df.select(*PAYDIMCOLS)
PAY_DIM.repartition(1).write.mode("overwrite").csv("s3://dataswan/Processed_Payments/Payment_Dim.csv",header=True)

DOCTORDIMCOLS=['COVERED_RECIPIENT_NPI','COVERED_RECIPIENT_FIRST_NAME','COVERED_RECIPIENT_MIDDLE_NAME','COVERED_RECIPIENT_LAST_NAME','COVERED_RECIPIENT_TYPE','TEACHING_HOSPITAL_NAME','COVERED_RECIPIENT_PRIMARY_TYPE','COVERED_RECIPIENT_SPECIALTY']
DOCTOR_DIM=merged_df.select(*DOCTORDIMCOLS)
DOCTOR_DIM.repartition(1).write.mode("overwrite").csv("s3://dataswan/Processed_Payments/Doctor_Dim.csv",header=True)

PROGRAMDIMCOLS=['COVERED_RECIPIENT_NPI','INDICATE_DRUG_OR_BIOLOGICAL_OR_DEVICE_OR_MEDICAL_SUPPLY','PRODUCT_CATEGORY_OR_THERAPEUTIC_AREA','PROGRAM_YEAR']
PROGRAM_DIM=merged_df.select(*PROGRAMDIMCOLS)
PROGRAM_DIM.repartition(1).write.mode("overwrite").csv("s3://dataswan/Processed_Payments/Program_Dim.csv",header=True)

# Write the processed data to an S3 location
#merged_df.repartition(1).write.mode("overwrite").csv("s3://dataswan/Processed_Payments/GeneralPayments2020.csv",header=True)

job.commit()
