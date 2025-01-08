import os
import time
import polars as pl
from typing import Callable, List


class DataProcessingUtility:
    YEAR_COLUMN = "trip_year"
    MONTH_COLUMN = "trip_month"

    @staticmethod
    def append_dataframe_to_parquet(df_to_add: pl.DataFrame,
                                    year: int,
                                    month: int):
        output_parquet_file = f"trip_data_parquet/trip_data_{year}_{month}.parquet"
        if not os.path.exists(output_parquet_file):
            df_to_add.write_parquet(output_parquet_file)

        df_target = pl.scan_parquet(output_parquet_file).collect(streaming=True)
        df_combined = pl.concat([df_target, df_to_add], how="diagonal")
        df_combined.write_parquet(output_parquet_file)

    @staticmethod
    def get_column_aligned_dataframe(df_to_align: pl.DataFrame, cols_to_align_with):
        for col in cols_to_align_with:
            if col not in df_to_align.columns:
                df_to_align = df_to_align.with_columns(pl.lit(None).alias(col))

        return df_to_align.select([col for col in cols_to_align_with if col in df_to_align.columns])

    @classmethod
    def append_json_to_parquet(cls,
                               input_json_file: str,
                               transform: Callable[[pl.DataFrame], pl.DataFrame] = None,
                               log_file: str = "json_processing.log.txt"):

        start_time = time.time()

        df = pl.read_json(input_json_file, infer_schema_length=None)
        df = DataProcessingUtility.get_column_aligned_dataframe(df, cls.get_schema_to_enforce())
        df = transform(df) if transform else df

        year_months = cls.get_year_months_from_dataframe(df)
        print(year_months)

        for year_month in year_months:
            year = year_month["year"]
            month = year_month["month"]
            df_filtered = df.filter(
                (pl.col(cls.YEAR_COLUMN) == year) & (pl.col(cls.MONTH_COLUMN) == month)
            )
            cls.append_dataframe_to_parquet(df_filtered, year, month)

        end_time = time.time()
        duration = end_time - start_time

        log_entry = f"{time.strftime('%Y-%m-%d %H:%M:%S')} | Processed file: {input_json_file} | Duration: {duration:.2f} seconds\n"

        with open(log_file, "a") as log:
            log.write(log_entry)

    @classmethod
    def transform_dataframe(cls, df: pl.DataFrame):
        return df.with_columns([
            pl.col("trip_start_timestamp").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f").alias("trip_start_date"),
            pl.col("trip_end_timestamp").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f").alias("trip_end_date"),
            pl.col("trip_start_timestamp").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f").dt.year().alias(
                cls.YEAR_COLUMN),
            pl.col("trip_start_timestamp").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f").dt.month().alias(
                cls.MONTH_COLUMN),
        ])

    @classmethod
    def get_year_months_from_dataframe(cls, df: pl.DataFrame):
        grouped_df = df.group_by([cls.YEAR_COLUMN, cls.MONTH_COLUMN]).agg([])

        return [
            {"year": row[cls.YEAR_COLUMN], "month": row[cls.MONTH_COLUMN]}
            for row in grouped_df.to_dicts()
        ]

    @classmethod
    def get_schema_to_enforce(cls):
        return {
            "trip_id": pl.Utf8,
            "taxi_id": pl.Utf8,
            "trip_start_timestamp": pl.Utf8,  # Can be converted to pl.Datetime if required
            "trip_end_timestamp": pl.Utf8,    # Can be converted to pl.Datetime if required
            "trip_seconds": pl.Int64,
            "trip_miles": pl.Float64,
            cls.YEAR_COLUMN: pl.UInt8,
            cls.MONTH_COLUMN: pl.UInt8,
            "pickup_census_tract": pl.Utf8,
            "dropoff_census_tract": pl.Utf8,
            "pickup_community_area": pl.Int64,
            "dropoff_community_area": pl.Int64,
            "fare": pl.Float64,
            "tips": pl.Float64,
            "tolls": pl.Float64,
            "extras": pl.Float64,
            "trip_total": pl.Float64,
            "payment_type": pl.Utf8,
            "company": pl.Utf8,
            "pickup_centroid_latitude": pl.Float64,
            "pickup_centroid_longitude": pl.Float64,
            "pickup_centroid_location": pl.Struct,  # Complex type, requires flattening if used in DataFrame
            "dropoff_centroid_latitude": pl.Float64,
            "dropoff_centroid_longitude": pl.Float64,
            "dropoff_centroid_location": pl.Struct  # Complex type, requires flattening if used in DataFrame
        }



    @staticmethod
    def split_parquet_file(input_file_with_path: str, number_of_files: int, remove_input_file: bool = False) -> List[str]:
        # Read the input Parquet file
        df = pl.read_parquet(input_file_with_path)

        # Calculate the number of rows per split
        total_rows = df.shape[0]
        rows_per_file = total_rows // number_of_files

        # Generate output file paths
        base_name, ext = os.path.splitext(input_file_with_path)
        output_files = [f"{base_name}_{i + 1}{ext}" for i in range(number_of_files)]

        # Split the data and write to files
        start_idx = 0
        for i, output_file in enumerate(output_files):
            end_idx = start_idx + rows_per_file + (1 if i < (total_rows % number_of_files) else 0)
            df[start_idx:end_idx].write_parquet(output_file)
            start_idx = end_idx

        # Optionally remove the input file
        if remove_input_file:
            os.remove(input_file_with_path)

        return output_files

if __name__ == "__main__":
    input_file = "/Users/peterfulop.me/code/taxi_trips/trip_data_parquet/trip_data_2013_6.parquet"
    number_of_files = 2
    output_files = DataProcessingUtility.split_parquet_file(input_file, number_of_files, remove_input_file=False)
    output_files.append(input_file)
    for file in output_files:
        print(f"File: {file}, number of rows = {pl.read_parquet(file).shape[0]}")