import os
import logging
from dotenv import load_dotenv
import yaml

import requests
import json
from google.cloud import bigquery
import pandas as pd



class APIData:
    def __init__(self) -> None:
        """
        Creates an instance of APIData to handle authentication and data fetching 
        from the IGDB API.

        Attributes:
            TOKEN_URL (str): The URL for obtaining OAuth2 tokens from Twitch.
            api_url (str): The URL for the IGDB API last fetched from.
            data (dict): A dictionary to store fetched data from the IGDB API.
        """
        self.TOKEN_URL = "https://id.twitch.tv/oauth2/token"
        self.client = bigquery.Client()
        self.api_url = ""
        self.data = {}
    def __repr__(self):
        return f"APIData(data={self.data})"
    def __str__(self):
        return json.dumps(self.data, indent=4)
    def authenticate(self, client_id: str, client_secret: str) -> dict | None:
        """
        Tries to authenticate with the IGDB API using the provided client ID 
        and client secret.

        Args:
            client_id (str): The client ID from Twitch Developer for the IGDB API.
            client_secret (str): The client secret from Twitch Developer for the 
                IGDB API.

        Returns:
            Authentication data if successful, otherwise returns None.
        """
        try:
            # Make the authentication request
            response = requests.post(
                url=self.TOKEN_URL,
                params={
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "grant_type": "client_credentials"
                }
            )

            auth_data = response.json()
            return auth_data
        except Exception as e:
            logging.error(f"An error occurd when trying to authenticate: {e}")
            return None
    def api_fetch(self, url: str, client_id: str, access_token: str, 
                  query: str) -> None:
        """
        Fetches data from the IGDB API using the provided URL,
        client ID, access token, and optional data fields and limit.  
        Stores the fetched data in the instance variable `data`.

        Args:
            url (str): The IGDB API endpoint URL. (Should be in the format
                "https://api.igdb.com/v4/{endpoint}")
            client_id (str): The client ID from Twitch Developer for the IGDB API.
            access_token (str): The access token obtained from authentication.
            data_fields (list[str] | None): Optional list of fields to fetch from the
                API.
            data_limit (int | None): Optional limit on the number of records to fetch.
        """
        try:
            # Make the API request
            response = requests.post(
                url=url, 
                **{"headers": {"Client-ID": client_id, 
                               "Authorization": f"Bearer {access_token}"}, 
                               "data": query})
            
            self.api_url = url
            self.data = response.json()
        except Exception as e:
            logging.error(f"An error occurd when trying to fetch data: {e}")

    def save_to_json(self, file_path: str) -> None:
        """
        Saves the fetched data to a JSON file.

        Args:
            file_path (str): The path to the file where data should be saved.
        """
        try:
            with open(file_path, "w") as f:
                json.dump(self.data, f, indent=4)
            logging.info(f"Data successfully saved to {file_path}! Added {len(self.data)} records.")
        except Exception as e:
            logging.error(f"Error saving data to JSON: {e}")
            raise

    def load_from_json(self, file_path: str) -> None:
        """
        Loads data from a JSON file into the instance variable `data`.

        Args:
            file_path (str): The path to the JSON file to load data from.
        """
        try:
            with open(file_path, "r") as f:
                self.data = json.load(f)
            logging.info(f"Data successfully loaded from {file_path}")
        except Exception as e:
            logging.error(f"Error loading data from JSON: {e}")
            raise
    
    def upload_to_bigquery(self, dataset_id: str, table_id: str) -> None:
        """
        Uploads the fetched data to a specified BigQuery table.
        """
        table_ref = self.client.dataset(dataset_id).table(table_id)
        try:
            # Convert to DataFrame
            df = pd.DataFrame(self.data)
            logging.info(f"Prepared {len(df)} records for upload")
            
            # Configure load job
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE_DATA",
                autodetect=False,  # Use predefined schema
            )
            
            # Upload data
            logging.info("Starting BigQuery upload...")
            job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            
            # Wait for completion
            job.result()
            
            # Get updated table info
            table = self.client.get_table(table_ref)
            logging.info(f"Upload complete! Table now has {table.num_rows} total rows")
            
        except Exception as e:
            logging.error(f"Error uploading to BigQuery: {e}")
            raise


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, 
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

    logger = logging.getLogger(__name__)
    logger.info("Hello from %s", __name__)

    load_dotenv()
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")

    with open("listnames.yml", "r") as f:
        config = yaml.safe_load(f)
    urls = config.get("urls", [])
    queries = config.get("queries", [])

    #dataset_id = os.getenv("BQ_DATASET_ID")
    table_ids = os.getenv("BQ_TABLE_IDS", "").split(",")

    my_data = APIData()
    auth = my_data.authenticate(client_id, client_secret)
    if auth:
        logger.info("Authentication successful")

        # Fetch data from the IGDB API.
        # Loopa över URL:erna och fälten i config-filen.
        for url, query, table_id in zip(urls, queries, table_ids):
            my_data.api_fetch(url, client_id, auth["access_token"], query)
            if my_data.data:
                logger.info(f"Data fetch successful from {url}")
                my_data.save_to_json(f"raw_data/{table_id}.json")

    return 0


if __name__ == "__main__":
    exit_code = main()
    if exit_code == 0:
        logging.info("Exited program with exit_code: 0")
    else:
        logging.info(f"Exited program with exit_code: {exit_code}")