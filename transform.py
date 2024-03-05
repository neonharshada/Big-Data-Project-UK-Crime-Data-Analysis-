# Import necessary modules
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions  # Import getResolvedOptions

# Initialize SparkContext
sc = SparkContext()
glueContext = GlueContext(sc)

# Create SparkSession object
spark = glueContext.spark_session

# Define Glue job
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])  # Use getResolvedOptions to get command-line arguments
job.init(args['JOB_NAME'], args)

# Read data from S3

df1 = spark.read.parquet("s3://group56datalake44shoja/street/")


# Perform data processing steps
header_row = df1.first()
if header_row:
    columns = header_row.asDict().keys()
    df1 = df1.toDF(*columns)

df1 = df1.dropDuplicates(["CrimeID", "MONTH", "ReportedBy", "FallsWithin", "Longitude", "Latitude", "Locations", "LSOACODE", "LSOANAME", "CrimeType", "LastOutcomeCategory", "Contexts"])
df1 = df1.drop("Contexts")
df1 = df1.dropna(subset=['Longitude'])
df1 = df1.dropna(subset=['LSOACODE'])
df1 = df1.dropna(subset=['LSOANAME'])
df1 = df1.fillna('Result Not Specified',subset=['Lastoutcomecategory'])
df1 = df1.dropna(subset=['CrimeID'])
df1 = df1.drop("Fallswithin")
from pyspark.sql.functions import to_date
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col
df1 = df1.withColumn("Latitude", col("Latitude").cast(DoubleType())) \
         .withColumn("Longitude", col("Longitude").cast(DoubleType())) \
         .withColumn('MONTH', to_date(col('MONTH'), 'yyyy-MM'))

# Write the DataFrame to Parquet
df1.write.mode("append").parquet("s3://group56datawarehouse34rels/STREET")

# Read data from outcomes for transformation
df2 = spark.read.parquet("s3://group56datalake44shoja/outcomes/")

# Read data from outcomes for transformation
df4 = df2.drop("col13","col14","context","crimetype", "partition_0","lastoutcomecategory")
df2=df4
df3 = df2.dropna(subset=['Longitude','Latitude'])
df2=df3
from pyspark.sql.functions import col, sum
null_counts = df2.select(*(sum(col(c).isNull().cast("int")).alias(c) for c in df2.columns))
df5 = df2.drop("FallsWithin")
df2 = df5
from pyspark.sql.functions import when
df6 = df2.withColumn("Locations", when(df2.Locations == "No location", "Unknown").otherwise(df2.Locations))
df2 = df6
from pyspark.sql.types import DoubleType
df8 = df2.withColumn("Latitude", df2["Latitude"].cast(DoubleType())) \
         .withColumn("Longitude", df2["Longitude"].cast(DoubleType()))
df2 = df8
from pyspark.sql.functions import to_date, date_format
df9 = df2.withColumn('MONTH', to_date(df2.MONTH, 'yyyy-MM'))
df2 = df9
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
num_partitions = 149
df_repartitioned = df2.repartition(num_partitions)
df2 = df_repartitioned

# Write the transformed DataFrame to Parquet

df2.write.mode("append").parquet("s3://group56datawarehouse34rels/OUTCOMES")

# Commit the job
job.commit()
