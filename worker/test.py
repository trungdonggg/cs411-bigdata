from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import StandardScaler, StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType
from pyspark.sql.functions import split, expr
from pyspark.ml.pipeline import PipelineModel


spark = SparkSession.builder \
        .appName('predict') \
        .getOrCreate() 
        # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \

schema = StructType([
    StructField("id", LongType(), nullable=False),
    StructField("Marital status", LongType(), nullable=True),
    StructField("Application mode", LongType(), nullable=True),
    StructField("Application order", LongType(), nullable=True),
    StructField("Course", LongType(), nullable=True),
    StructField("Daytime/evening attendance", LongType(), nullable=True),
    StructField("Previous qualification", LongType(), nullable=True),
    StructField("Previous qualification (grade)", DoubleType(), nullable=True),
    StructField("Nacionality", LongType(), nullable=True),
    StructField("Mother's qualification", LongType(), nullable=True),
    StructField("Father's qualification", LongType(), nullable=True),
    StructField("Mother's occupation", LongType(), nullable=True),
    StructField("Father's occupation", LongType(), nullable=True),
    StructField("Admission grade", DoubleType(), nullable=True),
    StructField("Displaced", LongType(), nullable=True),
    StructField("Educational special needs", LongType(), nullable=True),
    StructField("Debtor", LongType(), nullable=True),
    StructField("Tuition fees up to date", LongType(), nullable=True),
    StructField("Gender", LongType(), nullable=True),
    StructField("Scholarship holder", LongType(), nullable=True),
    StructField("Age at enrollment", LongType(), nullable=True),
    StructField("International", LongType(), nullable=True),
    StructField("Curricular units 1st sem (credited)", LongType(), nullable=True),
    StructField("Curricular units 1st sem (enrolled)", LongType(), nullable=True),
    StructField("Curricular units 1st sem (evaluations)", LongType(), nullable=True),
    StructField("Curricular units 1st sem (approved)", LongType(), nullable=True),
    StructField("Curricular units 1st sem (grade)", DoubleType(), nullable=True),
    StructField("Curricular units 1st sem (without evaluations)", LongType(), nullable=True),
    StructField("Curricular units 2nd sem (credited)", LongType(), nullable=True),
    StructField("Curricular units 2nd sem (enrolled)", LongType(), nullable=True),
    StructField("Curricular units 2nd sem (evaluations)", LongType(), nullable=True),
    StructField("Curricular units 2nd sem (approved)", LongType(), nullable=True),
    StructField("Curricular units 2nd sem (grade)", DoubleType(), nullable=True),
    StructField("Curricular units 2nd sem (without evaluations)", LongType(), nullable=True),
    StructField("Unemployment rate", DoubleType(), nullable=True),
    StructField("Inflation rate", DoubleType(), nullable=True),
    StructField("GDP", DoubleType(), nullable=True),
])


model_path = "hdfs://192.168.80.78:9000/vy/pipeline_model"
pipeline_model = PipelineModel.load(model_path)


df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.80.83:9092") \
    .option("subscribe", "cs411_student_performance") \
    .option("startingOffsets", "latest")\
    .option("failOnDataLoss", False)\
    .load()\
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")\
    .select(from_json("value", schema).alias("json")) \
    .select("json.*")

# df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("checkpointLocation", "/home/acma2k3/delta-data/ckpnt") \
#     .start()

# res = pipeline_model.transform(df).select("probability", "prediction")

res = pipeline_model.transform(df).select([col(field.name) for field in schema] + ["probability", "prediction"])

transformed= res.withColumn(
    'prediction',
    when(col('prediction') == 0, 'graduate')
    .when(col('prediction') == 1, 'dropout')
    .when(col('prediction') == 2, 'enrolled')
)

transformed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "/home/acma2k3/delta-data/ckpnt2") \
    .start()

spark.streams.awaitAnyTermination()
    
spark.stop()

    