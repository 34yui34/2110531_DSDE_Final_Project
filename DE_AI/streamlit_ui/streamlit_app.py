#import DE_AI.streamlit_ui.config as config

import os
import streamlit as st
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime
import pandas as pd
import subprocess
import tempfile
import sys

# -----------------------------------------------------------------------------
# Configs
# -----------------------------------------------------------------------------
# Max rows to materialize into pandas by default (safe)
MAX_ROWS = int(os.environ.get("TRAFFY_UI_MAX_ROWS", 1000))

# Allow user to force a capped load if they want
FORCE_LIMIT_ENV = bool(os.environ.get("TRAFFY_UI_FORCE_LIMIT", False))

# Optional Spark memory settings (tweak if you have more RAM)
SPARK_DRIVER_MEM = os.environ.get("SPARK_DRIVER_MEMORY", "4g")
SPARK_EXECUTOR_MEM = os.environ.get("SPARK_EXECUTOR_MEMORY", "4g")
API_KEY = os.environ.get("GEMINI_API_KEY", None)

DATA_PATH = "./parquet_data/traffy/complaints.parquet"

# -----------------------------------------------------------------------------
# 1. Connect to Spark (with some memory config)
# -----------------------------------------------------------------------------
spark = (
    SparkSession.builder
        .appName("TraffyQuery")
        .config("spark.driver.memory", SPARK_DRIVER_MEM)
        .config("spark.executor.memory", SPARK_EXECUTOR_MEM)
        .getOrCreate()
)

# -----------------------------------------------------------------------------
# Load Spark DF once (for min/max only)
# -----------------------------------------------------------------------------
# Protect against missing path
if not os.path.exists(DATA_PATH):
    st.error(f"Parquet path not found: {DATA_PATH}")
    st.stop()

df_full = spark.read.parquet(DATA_PATH)
# Make sure timestamp column exists and is cast
if "timestamp" in df_full.columns:
    df_full = df_full.withColumn("timestamp", F.to_timestamp("timestamp"))
else:
    st.error("No 'timestamp' column found in parquet. Please ensure dataset has a timestamp column.")
    st.stop()

min_ts = df_full.select(F.min("timestamp")).first()[0]
max_ts = df_full.select(F.max("timestamp")).first()[0]

def normalize_ts(ts):
    """Convert Spark ts → Python datetime safely."""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts   # already a Python datetime
    if hasattr(ts, "to_pydatetime"):
        return ts.to_pydatetime()
    if isinstance(ts, str):
        try:
            return datetime.fromisoformat(ts)
        except:
            return None
    return None

default_start = normalize_ts(min_ts) or datetime(2023, 1, 1)
default_end = normalize_ts(max_ts) or datetime.now()

# -----------------------------------------------------------------------------
# Helper function to save results back to parquet
# -----------------------------------------------------------------------------
def update_parquet_with_labels(labeled_df, data_path):
    """
    Update the parquet file with LLM classification results.
    This merges the new labels into the existing dataset.
    """
    try:
        # Read existing parquet
        existing_df = spark.read.parquet(data_path)
        
        # Convert pandas DataFrame to Spark DataFrame
        # Only keep ticket_id, llm_label, and llm_reasoning columns
        update_cols = ["ticket_id", "llm_label", "llm_reasoning"]
        labeled_spark = spark.createDataFrame(labeled_df[update_cols])
        
        # Join with existing data - left join to keep all original records
        # Drop existing llm_label and llm_reasoning if they exist
        cols_to_drop = [col for col in ["llm_label", "llm_reasoning"] if col in existing_df.columns]
        if cols_to_drop:
            existing_df = existing_df.drop(*cols_to_drop)
        
        # Perform left join to add new labels
        updated_df = existing_df.join(
            labeled_spark,
            on="ticket_id",
            how="left"
        )
        
        # Write back to parquet (overwrite mode)
        updated_df.write.mode("overwrite").parquet(data_path)
        
        return True
    except Exception as e:
        st.error(f"Failed to update parquet file: {str(e)}")
        return False

# -----------------------------------------------------------------------------
# Helper function to save to shared_data for map visualization
# -----------------------------------------------------------------------------
def save_to_shared_data(labeled_df, start_date, end_date):
    """
    Save classified data to shared_data folder for map visualization.
    Tries multiple possible paths (Windows, Docker, relative).
    Filename includes the date range: labeled_output_{start}_{end}.csv
    """
    # Format dates as MMDDYYYY
    start_str = start_date.strftime("%m%d%Y")
    end_str = end_date.strftime("%m%d%Y")
    filename = f"labeled_output_{start_str}_{end_str}.csv"
    
    # Define possible save paths
    save_paths = [
        rf"C:\Users\SorapatPun\Desktop\VScode\DSDE Project\2110531_DSDE_Final_Project\DE_AI\shared_data\{filename}",  # Windows absolute
        f"./shared_data/{filename}",  # Relative (current directory)
        f"../shared_data/{filename}",  # Parent directory
        f"/shared_data/{filename}",  # Docker volume
        f"./DE_AI/shared_data/{filename}",  # From root
    ]
    
    # Try to save to the first valid path
    saved = False
    successful_path = None
    
    for save_path in save_paths:
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            
            # Save the CSV
            labeled_df.to_csv(save_path, index=False)
            saved = True
            successful_path = save_path
            break  # Stop after first successful save
        except Exception as e:
            continue  # Try next path
    
    return saved, successful_path

# -----------------------------------------------------------------------------
# 2. Streamlit UI
# -----------------------------------------------------------------------------
st.title("Traffy Fondue – Gemini Issue Classifier")

st.markdown(
    f"**Dataset range:** {default_start} – {default_end}  \n"
    f"Maximum number of rows selectable is 1000.\r\nThis is configured as such so that the labelling won't take too long :)"
)

st.subheader("Select date & time range")

col1, col2 = st.columns(2)
with col1:
    start_dt = st.datetime_input("Start date & time", value=default_start)
with col2:
    end_dt = st.datetime_input("End date & time", value=default_end)

if start_dt > end_dt:
    st.error("Start datetime must be before end datetime")
    st.stop()

# Store the selected dates in session state for later use
st.session_state["start_dt"] = start_dt
st.session_state["end_dt"] = end_dt

# -----------------------------------------------------------------------------
# 3. Run Spark query
# -----------------------------------------------------------------------------
if st.button("Load Data"):
    st.info("Querying PySpark...")

    # Reload to ensure fresh state
    spark_df = spark.read.parquet(DATA_PATH)
    spark_df = spark_df.withColumn("timestamp", F.to_timestamp("timestamp"))
    available_cols = spark_df.columns

    filtered = (
        spark_df
        .filter(
            (F.col("timestamp") >= F.lit(start_dt)) &
            (F.col("timestamp") <= F.lit(end_dt))
        )
        .select(*available_cols)
        .orderBy("timestamp")
    )

    # Cache to avoid recomputation
    filtered = filtered.cache()

    try:
        row_count = filtered.count()
    except Exception as e:
        st.error("Failed to compute row count. This may indicate corrupted data or insufficient resources.")
        st.exception(e)
        st.stop()

    st.write(f"Rows matched: **{row_count:,}**")

    # If too many rows, ask user to narrow range or allow force-limited load
    if row_count > MAX_ROWS and not FORCE_LIMIT_ENV:
        st.warning(
            f"The selected range returns **{row_count:,}** rows which is larger than the safe limit of {MAX_ROWS:,}.\n\n"
        )
        st.warning(
            f"Please select shorter timeframe between start query date and end query date.\n\n"
        )
        st.stop()

    # If forcing or within limit, collect to pandas (cap to MAX_ROWS if needed)
    if row_count == 0:
        st.warning("No data found for selected date range.")
        st.stop()

    rows_to_collect = row_count
    if row_count > MAX_ROWS:
        rows_to_collect = MAX_ROWS
        st.info(f"Capping collected rows to {rows_to_collect:,} to protect memory.")

    # If we need to cap, use limit() to get a subset on the Spark side (cheap)
    if rows_to_collect < row_count:
        to_collect_df = filtered.limit(rows_to_collect)
    else:
        to_collect_df = filtered

    # Materialize before toPandas
    try:
        to_collect_df = to_collect_df.cache()
        # run an action to populate cache
        to_collect_df.count()
        pandas_df = to_collect_df.toPandas()
    except Exception as e:
        st.error(
            "Failed while collecting data from Spark into pandas. This often happens when the dataset is too large "
            "for the driver JVM / Python process to hold in memory.\n\n"
            "Suggestions:\n"
            " - Narrow the date range\n"
            " - Reduce MAX_ROWS via TRAFFY_UI_MAX_ROWS environment variable\n"
            " - Increase Spark driver memory (set SPARK_DRIVER_MEMORY env var)\n"
        )
        st.exception(e)
        st.stop()

    if pandas_df.empty:
        st.warning("No data after collection.")
        st.stop()

    st.success(f"Loaded {len(pandas_df):,} records")
    
    # Check if records already have classifications
    if "llm_label" in pandas_df.columns and "llm_reasoning" in pandas_df.columns:
        # Count successfully classified (exclude error labels)
        ERROR_LABELS = ["Missing in JSON Response", "Invalid Label from LLM", "API Error"]
        successfully_classified = (pandas_df["llm_label"].notna()) & (~pandas_df["llm_label"].isin(ERROR_LABELS))
        classified_count = successfully_classified.sum()
        unclassified_count = len(pandas_df) - classified_count
        
        # Count errors that will be retried
        error_count = pandas_df["llm_label"].isin(ERROR_LABELS).sum()
        
        st.info(f"📊 Classification Status: **{classified_count:,}** successfully classified, **{unclassified_count:,}** pending")
        
        if classified_count > 0:
            st.success(f"✅ Found {classified_count:,} records with valid classifications (will be reused)")
        
        if error_count > 0:
            st.warning(f"⚠️ Found {error_count:,} records with previous errors (will be retried)")
    
    st.dataframe(pandas_df.head(20))

    st.session_state["filtered_df"] = pandas_df
    # Reset classification complete flag when new data is loaded
    st.session_state["classification_complete"] = False

# -----------------------------------------------------------------------------
# 4. Run Gemini Labeler
# -----------------------------------------------------------------------------
if "filtered_df" in st.session_state:
    # Only show the button if classification is not complete
    if not st.session_state.get("classification_complete", False):
        if st.button("Run Gemini Labeler on filtered data"):
            pdf = st.session_state["filtered_df"]
            
            # Separate already classified and unclassified records
            # Treat error labels (253, 254, 255) as needing re-classification
            ERROR_LABELS = ["Missing in JSON Response", "Invalid Label from LLM", "API Error"]
            
            if "llm_label" in pdf.columns and "llm_reasoning" in pdf.columns:
                # Successfully classified records (not null and not error labels)
                successfully_classified = (pdf["llm_label"].notna()) & (~pdf["llm_label"].isin(ERROR_LABELS))
                already_classified = pdf[successfully_classified].copy()
                needs_classification = pdf[~successfully_classified].copy()
            else:
                already_classified = pd.DataFrame()
                needs_classification = pdf.copy()
            
            classified_count = len(already_classified)
            unclassified_count = len(needs_classification)
            
            # Count how many are errors being retried
            if "llm_label" in needs_classification.columns:
                error_retry_count = needs_classification["llm_label"].isin(ERROR_LABELS).sum()
                truly_new_count = unclassified_count - error_retry_count
                
                if error_retry_count > 0:
                    st.info(f"📊 Records to process: {classified_count:,} cached, {truly_new_count:,} new, {error_retry_count:,} retrying errors")
                else:
                    st.info(f"📊 Records to process: {classified_count:,} cached, {unclassified_count:,} need classification")
            else:
                st.info(f"📊 Records to process: {classified_count:,} cached, {unclassified_count:,} need classification")
            
            # Only run Gemini if there are unclassified records
            if unclassified_count > 0:
                # Save to temporary input CSV
                with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp_in:
                    needs_classification.to_csv(tmp_in.name, index=False)
                    tmp_input_path = tmp_in.name

                tmp_output_path = tmp_input_path.replace(".csv", "_output.csv")

                # Run your existing gemini script using the same Python interpreter
                cmd = [
                    sys.executable,
                    "gemini_labeller/gemini_labeler.py",
                    "--input", tmp_input_path,
                    "--output", tmp_output_path,
                    "--api_key", API_KEY,
                ]

                with st.spinner(f"Running Gemini labeling on {unclassified_count:,} records..."):
                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True
                    )

                    if result.returncode != 0:
                        st.error("Gemini labeler failed:")
                        st.code(result.stderr)
                        # cleanup
                        try:
                            os.remove(tmp_input_path)
                        except Exception:
                            pass
                        st.stop()

                st.success(f"Gemini labeling complete for {unclassified_count:,} new records!")

                # Load newly classified output
                try:
                    newly_labeled_df = pd.read_csv(tmp_output_path)
                except Exception as e:
                    st.error("Failed to read output CSV from gemini_labeler.py")
                    st.exception(e)
                    # cleanup
                    try:
                        os.remove(tmp_input_path)
                        if os.path.exists(tmp_output_path):
                            os.remove(tmp_output_path)
                    except Exception:
                        pass
                    st.stop()

                # Combine cached and newly classified records
                if not already_classified.empty:
                    labeled_df = pd.concat([already_classified, newly_labeled_df], ignore_index=True)
                    st.info(f"✅ Combined {classified_count:,} cached + {unclassified_count:,} new = {len(labeled_df):,} total records")
                else:
                    labeled_df = newly_labeled_df
                
                # cleanup temp files
                try:
                    os.remove(tmp_input_path)
                    if os.path.exists(tmp_output_path):
                        os.remove(tmp_output_path)
                except Exception:
                    pass
            else:
                st.success("All records already have classifications! Using cached results.")
                labeled_df = already_classified

            # Store labeled data and mark classification as complete
            st.session_state["labeled_df"] = labeled_df
            st.session_state["classification_complete"] = True
            
            # Save results back to parquet and shared_data
            with st.spinner("Saving classification results..."):
                # Save to parquet database
                parquet_saved = update_parquet_with_labels(labeled_df, DATA_PATH)
                
                if parquet_saved:
                    st.success("✅ Classification results saved to parquet database!")
                    st.info("Future queries will automatically use these cached results.")
                else:
                    st.warning("⚠️ Could not save results to parquet database.")
                
                # Save to shared_data for map visualization with date-based filename
                # Get the dates from session state
                start_date = st.session_state.get("start_dt", datetime.now())
                end_date = st.session_state.get("end_dt", datetime.now())
                
                shared_saved, shared_path = save_to_shared_data(labeled_df, start_date, end_date)
                
                if shared_saved:
                    st.success(f"💾 Results saved to shared_data for map visualization!")
                    st.info(f"📍 File location: {shared_path}")
                    st.info("🗺️ Go to Map Visualization (http://localhost:8501) and press 'R' to see the updated data!")
                else:
                    st.warning("⚠️ Could not save to shared_data. Map visualization may not update automatically.")
                    st.info("💡 You can still download the CSV manually using the button below.")

            # Force a rerun to update the UI
            st.rerun()
    else:
        st.success("✅ Classification completed for current dataset!")
        st.info("💡 To classify a different date range, use 'Load Data' button above to query new data.")
    
    # Show results and download button if classification is complete
    if st.session_state.get("classification_complete", False) and "labeled_df" in st.session_state:
        labeled_df = st.session_state["labeled_df"]
        
        # Display results
        st.subheader("Classification Results")
        st.dataframe(labeled_df)

        # Generate filename with date range for download
        start_date = st.session_state.get("start_dt", datetime.now())
        end_date = st.session_state.get("end_dt", datetime.now())
        start_str = start_date.strftime("%m%d%Y")
        end_str = end_date.strftime("%m%d%Y")
        download_filename = f"labeled_output_{start_str}_{end_str}.csv"

        # Offer download
        csv_data = labeled_df.to_csv(index=False)
        st.download_button(
            label="📥 Download labeled CSV",
            data=csv_data,
            file_name=download_filename,
            mime="text/csv"
        )
        
        # Display summary statistics
        st.subheader("Classification Summary")
        if "llm_label" in labeled_df.columns:
            label_counts = labeled_df["llm_label"].value_counts()
            st.write("**Label Distribution:**")
            for label, count in label_counts.items():
                percentage = (count / len(labeled_df)) * 100
                st.write(f"- {label}: {count:,} ({percentage:.1f}%)")