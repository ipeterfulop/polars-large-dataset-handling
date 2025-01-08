import os
import time
import polars as pl
from typing import Callable
from data_processing_utility import DataProcessingUtility


if __name__ == "__main__":

    start_source_files = 12_480
    end_source_file_numbers = 12_554

    for file_number in range(start_source_files, end_source_file_numbers + 1):
        json_file = f"trip_data_2013_2023/trip_data_page_{str(file_number).zfill(5)}.json"
        print(f"\nProcessing {json_file}...")
        DataProcessingUtility.append_json_to_parquet(json_file, DataProcessingUtility.transform_dataframe)
