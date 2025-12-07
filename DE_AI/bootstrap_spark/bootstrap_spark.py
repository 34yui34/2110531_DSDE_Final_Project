from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import re
import os

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


# ============================================================================
# PATH CONFIGURATION - Auto-detect for both Windows and Docker
# ============================================================================

def find_input_csv():
    """Find the bangkok_traffy.csv file in multiple possible locations."""
    possible_paths = [
        # Docker paths
        # "/DE_AI/dataset/traffy_fondue/bangkok_traffy.csv",
        # "./dataset/traffy_fondue/bangkok_traffy.csv",
        "/dataset/traffy_fondue/bangkok_traffy.csv"
        
        # Local Windows paths (from DE_AI directory)
        "./dataset/traffy_fondue/bangkok_traffy.csv",
        "../dataset/traffy_fondue/bangkok_traffy.csv",
        
        # Absolute Windows path
        r"C:\Users\SorapatPun\Desktop\VScode\DSDE Project\2110531_DSDE_Final_Project\DE_AI\dataset\traffy_fondue\bangkok_traffy.csv",
        r"C:\Users\SorapatPun\Desktop\VScode\DSDE Project\2110531_DSDE_Final_Project\dataset\traffy_fondue\bangkok_traffy.csv",
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            print(f"✅ Found input CSV at: {path}")
            return path
    
    print("❌ Could not find bangkok_traffy.csv in any of these locations:")
    for path in possible_paths:
        print(f"   - {path}")
    return None


def get_output_parquet_path():
    """Get the output parquet path, creating directory if needed."""
    possible_paths = [
        # Docker paths
        "/DE_AI/parquet_data/traffy/complaints.parquet",
        "./parquet_data/traffy/complaints.parquet",
        
        # Local paths
        "./DE_AI/parquet_data/traffy/complaints.parquet",
        "../DE_AI/parquet_data/traffy/complaints.parquet",
    ]
    
    for path in possible_paths:
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(path), exist_ok=True)
            print(f"✅ Will save parquet to: {path}")
            return path
        except Exception as e:
            continue
    
    # Fallback to current directory
    return "./complaints.parquet"


# ============================================================================
# MAIN PROCESSING
# ============================================================================

print("=" * 60)
print("Starting Traffy Data Bootstrap...")
print("=" * 60)

# Find input CSV
input_csv_path = find_input_csv()

if input_csv_path is None:
    print("\n⚠️  WARNING: Input CSV not found!")
    print("Please ensure bangkok_traffy.csv exists in one of the expected locations.")
    print("Skipping bootstrap process.")
    exit(0)  # Exit gracefully instead of failing

# Get output path
output_parquet_path = get_output_parquet_path()

# Initialize Spark
print("\n📊 Initializing Spark session...")
spark = (
    SparkSession.builder
        .appName("InitialLoad")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
)

# Register UDF
clean_thai_udf = F.udf(clean_thai_text, StringType())

# Read CSV
print(f"\n📖 Reading CSV from: {input_csv_path}")
try:
    df = spark.read.csv(
        input_csv_path,
        header=True,
        inferSchema=True,
        multiLine=True,
        escape='"',
        quote='"'
    )
    print(f"✅ Loaded {df.count():,} rows from CSV")
except Exception as e:
    print(f"❌ Error reading CSV: {e}")
    exit(1)

# Clean comment column
print("\n🧹 Cleaning Thai text in 'comment' column...")
df = df.withColumn("comment", clean_thai_udf("comment")) 

# Clean or cast datetime fields
print("📅 Processing timestamp column...")
df = df.withColumn(
    "timestamp",
    F.when(F.col("timestamp").isNull(), None).otherwise(F.to_timestamp("timestamp"))
)

# Save as Parquet
print(f"\n💾 Writing parquet to: {output_parquet_path}")
try:
    df.write.mode("overwrite").parquet(output_parquet_path)
    print("✅ Parquet file written successfully!")
except Exception as e:
    print(f"❌ Error writing parquet: {e}")
    exit(1)

# Verify the written data
print("\n🔍 Verifying written data...")
try:
    df2 = spark.read.parquet(output_parquet_path)
    
    print("\n=== Schema ===")
    df2.printSchema()
    
    print("\n=== Sample Rows ===")
    df2.show(5, truncate=False)
    
    print("\n=== Row Count ===")
    row_count = df2.count()
    print(f"Total rows: {row_count:,}")
    
    print("\n=== Null Check ===")
    df2.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df2.columns]).show()
    
    print("\n" + "=" * 60)
    print("✅ Bootstrap completed successfully!")
    print(f"📊 Processed {row_count:,} records")
    print(f"💾 Output: {output_parquet_path}")
    print("=" * 60)
    
except Exception as e:
    print(f"❌ Error verifying data: {e}")
    exit(1)

# Stop Spark
spark.stop()