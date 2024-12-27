import os
import re

def rename_files_with_padding(directory, start_index, end_index, padding=5):
    """
    Rename files by padding the numbers in their filenames.

    :param directory: Directory containing the files.
    :param start_index: Start index of files to process.
    :param end_index: End index of files to process.
    :param padding: Number of digits for padding (default is 5).
    """
    # Compile a regex to match filenames like trip_data_page_123.json
    pattern = re.compile(r"^(trip_data_page_)(\d+)(\.json)$")

    # List all files in the directory
    for filename in os.listdir(directory):
        match = pattern.match(filename)
        if match:
            prefix, number, suffix = match.groups()
            file_number = int(number)

            # Check if the file number is within the specified range
            if start_index <= file_number <= end_index:
                # Pad the number with zeros
                new_number = str(file_number).zfill(padding)
                new_filename = f"{prefix}{new_number}{suffix}"

                # Get full paths for renaming
                old_path = os.path.join(directory, filename)
                new_path = os.path.join(directory, new_filename)

                # Rename the file
                os.rename(old_path, new_path)
                print(f"Renamed: {filename} -> {new_filename}")

if __name__ == "__main__":
    # Configuration
    directory = "trip_data_2013_2023"  # Replace with your directory path
    start_index = 6981 # Replace with your start index
    end_index = 7001  # Replace with your end index
    padding = 5  # Adjust padding as needed

    rename_files_with_padding(directory, start_index, end_index, padding)
