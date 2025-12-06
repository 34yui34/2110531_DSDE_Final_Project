import pandas as pd
from sklearn.model_selection import train_test_split

def process_dataset(input_file, output_50k, output_150k):
    print("Loading dataset...")
    # Load your large dataset
    df = pd.read_csv(input_file)

    # ---------------------------------------------------------
    # PREPARATION: Handle NaN values for stratification
    # ---------------------------------------------------------
    # Stratified sampling fails on NaNs, so we fill them with a placeholder
    # solely for the purpose of calculating the split.
    # We create a temporary column 'stratify_col'.
    df['stratify_col'] = df['star'].fillna('MISSING')
    
    # Ensure the placeholder is treated consistently as a string
    df['stratify_col'] = df['stratify_col'].astype(str)

    print(f"Original dataset size: {len(df)}")
    print("Distribution of stars (original):")
    print(df['stratify_col'].value_counts(normalize=True))

    # ---------------------------------------------------------
    # STEP 1: Sample 200,000 rows from the original dataset
    # ---------------------------------------------------------
    # We use train_test_split to get a stratified sample of 200k.
    # train_size=200000 ensures we get exactly 200k rows.
    print("\nSampling 200,000 rows...")
    
    sampled_200k, _ = train_test_split(
        df,
        train_size=200000,
        stratify=df['stratify_col'],
        random_state=42  # Seed for reproducibility
    )

    # ---------------------------------------------------------
    # STEP 2: Split the 200k sample into 50k and 150k
    # ---------------------------------------------------------
    # Now we split the 200k dataset.
    # We want 50,000 for one file (which leaves 150,000 for the other).
    print("Splitting into 50k and 150k files...")
    
    df_50k, df_150k = train_test_split(
        sampled_200k,
        train_size=50000,
        stratify=sampled_200k['stratify_col'],
        random_state=42
    )

    # ---------------------------------------------------------
    # CLEANUP & SAVE
    # ---------------------------------------------------------
    # Remove the temporary stratification column
    df_50k = df_50k.drop(columns=['stratify_col'])
    df_150k = df_150k.drop(columns=['stratify_col'])

    # Save to CSV
    print(f"Saving {output_50k}...")
    df_50k.to_csv(output_50k, index=False)

    print(f"Saving {output_150k}...")
    df_150k.to_csv(output_150k, index=False)
    
    print("Done!")

# ==========================================
# CONFIGURATION
# ==========================================
input_filename = 'bangkok_traffy.csv' 
output_file_1 = 'dataset_50k.csv'
output_file_2 = 'dataset_150k.csv'

# Run the function
if __name__ == "__main__":
    # This try-except block is just to catch file not found errors easily
    try:
        process_dataset(input_filename, output_file_1, output_file_2)
    except FileNotFoundError:
        print(f"Error: The file '{input_filename}' was not found. Please check the filename.")
    except ValueError as e:
        print(f"Error during splitting: {e}")
        print("Tip: This can happen if a specific 'star' category has too few examples to be split effectively.")