import os
import httpx
import json
import time
import random

class UserAgentProvider:
    @staticmethod
    def get_user_agents():
        return [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
        ]

class DownloadConfiguration:
    def __init__(self, base_url, nr_records_per_page, output_folder):
        self.base_url = base_url
        self.nr_records_per_page = nr_records_per_page
        self.output_folder = output_folder
        self._ensure_output_folder_exists()

    def _ensure_output_folder_exists(self):
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

class DataDownloader:
    def __init__(self,
                 config:DownloadConfiguration,
                 page_index_to_continue_from=0,
                 sleep_time_between_requests=(7, 20),
                 nr_of_retries=3):
        self.config = config
        self.page_index_to_continue_from = page_index_to_continue_from
        self.sleep_time_between_requests = sleep_time_between_requests
        self.nr_of_retries = nr_of_retries
        self.user_agents = UserAgentProvider.get_user_agents()

    def download_data(self, total_pages):
        for page in range(self.page_index_to_continue_from, total_pages):
            offset = page * self.config.nr_records_per_page
            url = f"{self.config.base_url}?$limit={self.config.nr_records_per_page}&$offset={offset}&$order=trip_start_timestamp"
            print(f"Downloading page {page + 1} from {url}")

            success = self._attempt_download(url, page)

            if not success:
                print(f"!!! Failed to download page {page + 1}. Stopping the download.")
                break

        print("Download complete.")

    def _attempt_download(self, url, page):
        for attempt in range(self.nr_of_retries):
            headers = {"User-Agent": random.choice(self.user_agents)}
            try:
                response = httpx.get(url, headers=headers, timeout=10)
                if response.status_code == 200:
                    self._save_data(response.json(), page)
                    self._sleep_random_interval()
                    return True
                else:
                    print(f"Attempt {attempt + 1} failed with status code: {response.status_code}")
            except httpx.RequestError as e:
                print(f"-> Attempt {attempt + 1} failed with error: {e}")

            # Add sleep between attempts
            self._sleep_between_attempts(attempt + 1)

        return False

    def _save_data(self, data, page):
        file_number = str(page + 1).zfill(5)
        file_path = os.path.join(self.config.output_folder, f"trip_data_page_{file_number}.json")
        with open(file_path, "w") as file:
            json.dump(data, file)
        print(f"*** Downloaded and saved page {page + 1}.\n")

    def _sleep_random_interval(self):
        duration = random.uniform(*self.sleep_time_between_requests)
        print(f"Sleeping for {duration:.2f} seconds to avoid rate limits.")
        time.sleep(duration)

    def _sleep_between_attempts(self, attempt):
        duration = random.uniform(5, 10)
        print(f"Sleeping for {duration:.2f} seconds after attempt {attempt} to avoid quick retries.")
        time.sleep(duration)

if __name__ == "__main__":
    # Configuration
    base_url = "https://data.cityofchicago.org/resource/wrvz-psew.json"
    nr_records_per_page = 1000
    output_folder = "trip_data_2013_2023"

    config = DownloadConfiguration(base_url, nr_records_per_page, output_folder)

    # Downloader setup
    page_index_to_continue_from = 9077
    total_pages = 10000
    downloader = DataDownloader(config, page_index_to_continue_from,
                                nr_of_retries=8,
                                sleep_time_between_requests=(20, 32))

    # Start download
    downloader.download_data(total_pages)
