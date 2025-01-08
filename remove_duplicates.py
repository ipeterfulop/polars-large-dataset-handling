import polars as pl

if __name__ == "__main__":

    # Load the Parquet file
    file_path = "trip_data_parquet/trip_data_2013_6.parquet"
    df = pl.read_parquet(file_path)

    # Check for duplicates by `trip_id`
    duplicates = df.filter(pl.col("trip_id").is_duplicated())

    # Print duplicate rows (for verification)
    print(f"Duplicate rows based on trip_id:\n{duplicates}")

    # exit(1)

    # Remove duplicates and keep only the first occurrence of each duplicate
    df_deduplicated = df.unique(subset=["trip_id"], keep="first")

    # Save the deduplicated DataFrame back to the original Parquet file
    df_deduplicated.write_parquet(file_path)

    print(f"Deduplicated file saved to {file_path}")

