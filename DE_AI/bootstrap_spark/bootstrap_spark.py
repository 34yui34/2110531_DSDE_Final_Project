from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import re

def clean_thai_text(text):
    if text is None:
        return None

    # Remove URLs
    text = re.sub(r"http\S+|www\.\S+", "", text)

    # Remove emojis and non-text symbols
    emoji_pattern = re.compile(
        "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map
        u"\U0001F1E0-\U0001F1FF"  # flags
        u"\U00002500-\U00002BEF"  # chinese char
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        "]+",
        flags=re.UNICODE
    )
    text = emoji_pattern.sub(r'', text)

    # Remove characters that are not Thai/English/Numbers/Whitespace
    text = re.sub(r"[^ก-๙a-zA-Z0-9\s]", " ", text)

    # Normalize multiple spaces → single space
    text = re.sub(r"\s+", " ", text)

    # Trim
    text = text.strip()

    return text


spark = (
    SparkSession.builder
        .appName("InitialLoad")
        .getOrCreate()
)

clean_thai_udf = F.udf(clean_thai_text, StringType())
df = spark.read.csv(
    "./dataset/traffy_fondue/bangkok_traffy.csv",
    header=True,
    inferSchema=True,
    multiLine=True,
    escape='"',
    quote='"'
)
df = df.withColumn("comment", clean_thai_udf("comment"))

# Clean or cast datetime fields
df = df.withColumn(
    "timestamp",
    F.when(F.col("timestamp").isNull(), None).otherwise(F.to_timestamp("timestamp"))
)

# Save as Parquet (or Delta Lake)
df.write.mode("overwrite").parquet("./DE_AI/parquet_data/traffy/complaints.parquet")

print("Initial dataset written.")

df2 = spark.read.parquet("./DE_AI/parquet_data/traffy/complaints.parquet")

print("=== Schema ===")
df2.printSchema()

print("\n=== Sample Rows ===")
df2.show(5, truncate=False)

print("\n=== Row Count ===")
print(df2.count())

print("\n=== Null Check ===")
df2.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df2.columns]).show()
