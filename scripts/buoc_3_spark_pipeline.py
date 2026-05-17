from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler, Imputer
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
import time

print("=" * 60)
print("BUOC 3: PARALLEL PROCESSING VOI APACHE SPARK CLUSTER")
print("=" * 60)

# Khởi tạo SparkSession (cluster mode)
spark = (SparkSession.builder
    .appName("FraudDetection_Cluster")
    .master("spark://spark-master:7077")  # Kết nối Spark Master
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")  # Kết nối HDFS
    .config("spark.executor.memory", "2g")  # Bài báo Section 4.2
    .config("spark.executor.cores", "2")    # Bài báo Section 4.2
    .config("spark.sql.shuffle.partitions", "8")  # Bài báo Section 4.2
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("✅ SparkSession khoi tao!")
print(f"   Master : {spark.sparkContext.master}")
print(f"   App ID : {spark.sparkContext.applicationId}")

# ============================================================
# DOC DATA TU HDFS
# ============================================================
print("\n--- DOC DATA TU HDFS ---")

start_read = time.time()

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("transaction_time", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("transaction_direction", StringType(), True),
    StructField("account_balance", DoubleType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("state", StringType(), True),
    StructField("credit_score", IntegerType(), True),
    StructField("has_loan", IntegerType(), True),
    StructField("loan_type", StringType(), True),
    StructField("emi_amount", DoubleType(), True),
    StructField("transaction_status", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("kyc_status", StringType(), True),
    StructField("is_fraud", IntegerType(), True),
    StructField("transaction_hour", IntegerType(), True),
])

# Doc tu HDFS (cluster mode tu datanode)
df = (spark.read
    .schema(schema)
    .option("header", "true")
    .csv("hdfs://namenode:9000/financial/raw/fraud_dataset.csv")
)

df.cache()

read_time = (time.time() - start_read) * 1000
so_dong = df.count()
so_partition = df.rdd.getNumPartitions()

print(f"✅ Doc xong trong {read_time:.0f}ms")
print(f"   So dong     : {so_dong:,}")
print(f"   So partition: {so_partition}")

# ============================================================
# XU LY DAG - SONG SONG
# ============================================================
print("\n--- DAG PIPELINE (SONG SONG) ---")

df = (df
    .withColumn("transaction_date", F.to_date("transaction_date"))
    .withColumn("day_of_week", F.dayofweek("transaction_date"))
    .withColumn("month", F.month("transaction_date"))
    .withColumn("amount_balance_ratio",
        F.col("transaction_amount") / (F.col("account_balance") + F.lit(1)))
    .withColumn("log_amount", F.log1p(F.col("transaction_amount")))
    .withColumn("is_off_peak",
        F.when((F.col("transaction_hour") <= 5) | (F.col("transaction_hour") >= 23), 1).otherwise(0))
)

print("Fraud rate theo channel (chay song song):")
(df.groupBy("channel")
   .agg(
       F.count("*").alias("tong"),
       (F.mean("is_fraud") * 100).alias("fraud_rate_pct")
   )
   .orderBy(F.desc("fraud_rate_pct"))
   .show(truncate=False)
)

# ============================================================
# CHUAN BI FEATURES VA TRAIN MODELS
# ============================================================
print("\n--- TRAIN MODELS ---")

categorical_cols = ["account_type", "transaction_type", "merchant_category", "state", "channel", "kyc_status"]
numerical_cols = ["transaction_amount", "account_balance", "credit_score", "emi_amount", "transaction_hour",
                  "has_loan", "amount_balance_ratio", "log_amount", "is_off_peak", "day_of_week", "month"]

indexers = [StringIndexer(inputCol=col, outputCol=col + "_idx", handleInvalid="keep") for col in categorical_cols]
imputer = Imputer(inputCols=numerical_cols, outputCols=numerical_cols, strategy="median")
assembler = VectorAssembler(inputCols=numerical_cols + [c + "_idx" for c in categorical_cols],
                            outputCol="features_raw", handleInvalid="keep")
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)

df = df.withColumn("label", F.col("is_fraud").cast(DoubleType()))

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
train_df.cache()
test_df.cache()

print(f"Train: {train_df.count():,} dong")
print(f"Test : {test_df.count():,} dong")

# Random Forest
print("\n  Training Random Forest...")
start_rf = time.time()
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=50, maxDepth=8, seed=42)
pipeline_rf = Pipeline(stages=indexers + [imputer, assembler, scaler, rf])
trained_rf = pipeline_rf.fit(train_df)
time_rf = (time.time() - start_rf) * 1000

pred_rf = trained_rf.transform(test_df)
evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
auc_rf = evaluator.evaluate(pred_rf)

print(f"  ✅ AUC: {auc_rf:.4f} | Time: {time_rf:.0f}ms")

# GBT
print("\n  Training GBT...")
start_gbt = time.time()
gbt = GBTClassifier(featuresCol="features", labelCol="label", maxIter=30, maxDepth=6, seed=42)
pipeline_gbt = Pipeline(stages=indexers + [imputer, assembler, scaler, gbt])
trained_gbt = pipeline_gbt.fit(train_df)
time_gbt = (time.time() - start_gbt) * 1000

pred_gbt = trained_gbt.transform(test_df)
auc_gbt = evaluator.evaluate(pred_gbt)

print(f"  ✅ AUC: {auc_gbt:.4f} | Time: {time_gbt:.0f}ms")

# ============================================================
# LUU KET QUA
# ============================================================
print("\n--- LUU KET QUA ---")

(pred_gbt
    .select("transaction_id", "customer_id", "label", "prediction", "probability")
    .write
    .mode("overwrite")
    .parquet("hdfs://namenode:9000/financial/results/predictions")
)

print("✅ Da luu predictions len HDFS!")

# ============================================================
# BENCHMARK
# ============================================================
print("\n" + "=" * 60)
print("BENCHMARK KET QUA")
print("=" * 60)
print(f"{'Model':<15} {'AUC':>8} {'Time(ms)':>10}")
print("-" * 60)
print(f"{'Random Forest':<15} {auc_rf:>8.4f} {time_rf:>10.0f}")
print(f"{'GBT':<15} {auc_gbt:>8.4f} {time_gbt:>10.0f}")
print("=" * 60)
print(f"\nBai bao: execution time 206-392ms")
print(f"Ket qua: RF={time_rf:.0f}ms, GBT={time_gbt:.0f}ms")

spark.stop()
print("\n✅ BUOC 3 HOAN THANH!")