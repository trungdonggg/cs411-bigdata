from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StandardScaler, StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.types import StructType,StructField,LongType, StringType,DoubleType


spark = SparkSession \
            .builder \
            .appName('student performance') \
            .getOrCreate()


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
    StructField("Target", StringType(), nullable=True)
])


df = spark.read.format('csv').option('header', True).schema(schema).load('/home/acma2k3/cs411/worker/train.csv')
stringIndexer = StringIndexer(inputCol="Target", outputCol="label")
df = stringIndexer.fit(df).transform(df)


numerical_cols = [
                'Admission grade', 'Age at enrollment', 
                'Previous qualification (grade)',
                'Curricular units 1st sem (credited)',
                'Curricular units 1st sem (enrolled)',
                'Curricular units 1st sem (evaluations)',
                'Curricular units 1st sem (approved)',
                'Curricular units 1st sem (grade)',
                'Curricular units 1st sem (without evaluations)',
                'Curricular units 2nd sem (credited)',
                'Curricular units 2nd sem (enrolled)',
                'Curricular units 2nd sem (evaluations)',
                'Curricular units 2nd sem (approved)',
                'Curricular units 2nd sem (grade)',
                'Curricular units 2nd sem (without evaluations)', 
                'Unemployment rate',
                'Inflation rate', 'GDP']

categorical_cols = [    
                'Application mode',
                'Application order',
                'Course',
                'Daytime/evening attendance',
                'Debtor',
                'Displaced',
                'Educational special needs',
                "Father's occupation",
                "Father's qualification",
                'Gender',
                'International',
                'Marital status',
                "Mother's occupation",
                "Mother's qualification",
                'Nacionality',
                'Previous qualification',
                'Scholarship holder',
                'Tuition fees up to date'
]


rf = RandomForestClassifier(numTrees=50, seed=42)
one = OneHotEncoder(inputCols=categorical_cols, outputCols=[x + "_onehot" for x in categorical_cols])

numerical_assembler = VectorAssembler(inputCols=numerical_cols, outputCol="features_numerical")
scaler = StandardScaler(inputCol="features_numerical", outputCol="scaled_features")

features_assembler = VectorAssembler(inputCols=[x + "_onehot" for x in categorical_cols] + ["scaled_features"], outputCol="features")

stages = [one, numerical_assembler, scaler, features_assembler, rf]

pipeline = Pipeline(stages=stages)

pmodel = pipeline.fit(df)


pmodel.save("hdfs://192.168.80.78:9000/vy/pipeline_model")

spark.stop()

