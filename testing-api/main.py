import os
import logging
from dotenv import load_dotenv
import yaml

import requests
import json
from google.cloud import bigquery
import pandas as pd



class Data:
    def __init__(self, data: pd.DataFrame | None = None) -> None:
        """
        Creates an instance of Data to handle local data storage.

        Attributes:
            data (dict): A dictionary to store data.
        """
        if data is not None:
            self.data = data
        else:
            self.data = pd.DataFrame()
    def __repr__(self):
        return f"Data(data={self.data})"
    def __str__(self):
        return str(self.data)
    
    @property
    def data(self) -> pd.DataFrame:
        """
        Returns the current data stored in the instance variable `data`.

        Returns:
            dict: The current data.
        """
        return self._data
    @data.setter
    def data(self, value: pd.DataFrame) -> None:
        """
        Sets the instance variable `data` to the provided value.

        Args:
            value (dict): The new data to set.
        """
        if not isinstance(value, pd.DataFrame):
            raise TypeError("Data must be a list")
        self._data = value
    
    @property
    def records(self) -> int:
        """
        Returns the number of records in the data.

        Returns:
            int: The number of records.
        """
        return self.data.shape[0]
    
    def save_to_json(self, file_path: str, append: bool = False, exclude_duplicates: bool = True) -> None:
        """
        Saves `data` to a JSON file.

        Args:
            file_path (str): The path to the file where `data` should be saved.
            append (bool): If True, appends to data in existing file. Defaults to False.
            exclude_duplicates (bool): If True, excludes records with duplicate IDs. Defaults to True.
        """
        try:
            if append:
                with open(file_path, "r") as f:
                    existing_data = pd.DataFrame(json.load(f))
                with open(file_path, "w") as f:
                    if exclude_duplicates:
                        combined_data = pd.concat([existing_data, self.data]).drop_duplicates(subset=['id'])
                        json.dump(combined_data.to_dict(orient='records'), f, indent=4)
                    else:
                        combined_data = pd.concat([existing_data, self.data])
                        json.dump(combined_data.to_dict(orient='records'), f, indent=4)
            else:
                with open(file_path, "w") as f:
                    json.dump(self.data.to_dict(orient='records'), f, indent=4)
            logging.info(f"Data successfully saved to {file_path}. Added {len(self.data)} records.")
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
                self.data = pd.DataFrame(json.load(f))
            logging.info(f"Data successfully loaded from {file_path}")
        except Exception as e:
            logging.error(f"Error loading data from JSON: {e}")
            raise
    def clear_data(self) -> None:
        """
        Clears the current data stored in the instance variable `data`.
        """
        self.data = pd.DataFrame()
        logging.info("Data cleared")

class Pipeline:
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
        self.auth = None

    def _get_client_secret(self) -> None:       # ADD LATER MAYBE
        pass
    
    def authenticate(self, client_id: str, client_secret: str) -> None:
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
            if "access_token" not in auth_data:
                raise ValueError(f"Authentication failed: {auth_data.get('message', 'No access token received')}")
            logging.info("Authentication successful")
            self.auth = auth_data
        except Exception as e:
            logging.error(f"An error occurd when trying to authenticate: {e}")
    def api_fetch(self, url: str, client_id: str, access_token: str, 
                  query: str) -> pd.DataFrame | None:
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
            append (bool): If True, appends new data to existing data. Defaults to 
                False.
            ignore_existing_ids (bool): If True, ignores records with IDs that already 
                are in `data`. Defaults to True.

        Returns:
            The fetched data if successful, otherwise returns None.
        """
        try:
            query_properties = {sub_seg[0].strip(): sub_seg[1].strip() for sub_seg in [seg.strip().split(maxsplit=1) for seg in query.lower().split(";")[:-1]]}
            ROW_INTERVAL = 500      # Max rows per request is 500
            if "limit" in query_properties.keys():
                query_limit = int(query_properties["limit"])
            else:
                query_limit = None

            offset = 0
            all_data = pd.DataFrame()
            paged_query_properties = query_properties.copy()
            reached_end = False
            while not reached_end:
                if query_limit and offset + ROW_INTERVAL >= query_limit:
                    paged_query_properties["limit"] = str(query_limit - offset)
                else:
                    paged_query_properties["limit"] = str(ROW_INTERVAL)
                paged_query_properties["offset"] = str(offset)
                paged_query = " ".join([f"{key} {value};" for key, value in paged_query_properties.items()])
                logging.info(f"Fetching data with query: {paged_query}")
                response = requests.post(url=url, 
                                         headers={"Client-ID": client_id, 
                                                  "Authorization": f"Bearer {access_token}"}, 
                                         data=paged_query).json()
                data_chunk = pd.DataFrame(response)
                all_data = pd.concat([all_data, data_chunk], ignore_index=True)
                offset += ROW_INTERVAL
                if len(data_chunk) < ROW_INTERVAL:
                    reached_end = True
            logging.info(f"Data fetch successful from {url}. Fetched {len(all_data)} records.")
            return all_data
        except Exception as e:
            logging.error(f"An error occurd when trying to fetch data: {e}")
    
    def upload_to_bigquery(self, data: Data, dataset_id: str, table_id: str) -> None:
        """
        Uploads the `data` to a specified BigQuery table.
        """
        table_ref = self.client.dataset(dataset_id).table(table_id)
        try:
            # Configure load job
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE_DATA",
                autodetect=False,  # Use predefined schema
            )
            
            # Upload data
            logging.info("Starting BigQuery upload...")
            job = self.client.load_table_from_dataframe(data.data, table_ref, job_config=job_config)
            
            # Wait for completion
            job.result()
            
            # Get updated table info
            table = self.client.get_table(table_ref)
            logging.info(f"Upload complete! Table now has {table.num_rows} total rows")
        except Exception as e:
            logging.error(f"Error uploading to BigQuery: {e}")
            raise


def save_everything_locally(pipeline: Pipeline, client_id: str) -> None:
    with open("listnames.yml", "r") as f:
        config = yaml.safe_load(f)
    urls = config.get("urls", [])
    queries = config.get("queries", [])

    table_ids = os.getenv("BQ_TABLE_IDS", "").split(",")

    # Fetch data from the IGDB API.
    # Loopa över URL:erna och fälten i config-filen.
    for url, query, table_id in zip(urls, queries, table_ids):
        data = Data(pipeline.api_fetch(url, client_id, pipeline.auth["access_token"], query))
        if data:
            data.save_to_json(f"raw_data/{table_id}.json")

def save_one_table_locally(pipeline: Pipeline, client_id: str, url: str, query: str, table_id: str) -> None:
    data = Data(pipeline.api_fetch(url, client_id, pipeline.auth["access_token"], query))
    if data:
        data.save_to_json(f"raw_data/{table_id}.json")

def upload_local_to_bigquery(pipeline: Pipeline, table_ids: list[str] | None = None) -> None:
    """
    Uploads local JSON files to BigQuery.
    If table_ids is not provided, it will be read from the environment variable.

    Args:
        pipeline (Pipeline): An instance of the Pipeline class.
        table_ids (list[str] | None): Optional list of BigQuery table IDs.
    """
    if not table_ids:
        table_ids = os.getenv("BQ_TABLE_IDS", "").split(",")
    for table_id in table_ids:
        data = Data()
        data.load_from_json(f"raw_data/{table_id}.json")
        if data and data.records > 0:
            pipeline.upload_to_bigquery(data, dataset_id="raw_data", table_id=table_id)

def upload_api_to_bigquery(pipeline: Pipeline, client_id: str, urls: list[str] | None = None, 
                           querys: list[str] | None = None, table_ids: list[str] | None = None) -> None:
    """
    Uploads data fetched from the IGDB API directly to BigQuery.
    If urls, querys, or table_ids are not provided, they will be read from
    the listnames.yml file and environment variables.
    
    Args:
        pipeline (Pipeline): An instance of the Pipeline class.
        client_id (str): The client ID for IGDB API authentication.
        urls (list[str] | None): Optional list of IGDB API endpoint URLs.
        querys (list[str] | None): Optional list of queries for the IGDB API.
        table_ids (list[str] | None): Optional list of BigQuery table IDs.
    """
    if not (urls and querys and table_ids):
        with open("listnames.yml", "r") as f:
            config = yaml.safe_load(f)
        urls = config.get("urls", [])
        queries = config.get("queries", [])

        table_ids = os.getenv("BQ_TABLE_IDS", "").split(",")

        for url, query, table_id in zip(urls, queries, table_ids):
            data = Data(pipeline.api_fetch(url, client_id, pipeline.auth["access_token"], query))
            if data and data.records > 0:
                pipeline.upload_to_bigquery(data, dataset_id="raw_data", table_id=table_id)
    else:
        for url, query, table_id in zip(urls, querys, table_ids):
            data = Data(pipeline.api_fetch(url, client_id, pipeline.auth["access_token"], query))
            if data and data.records > 0:
                pipeline.upload_to_bigquery(data, dataset_id="raw_data", table_id=table_id)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, 
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    logger = logging.getLogger(__name__)
    logger.info("Hello from %s", __name__)
    load_dotenv()

    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    pipeline = Pipeline()
    pipeline.authenticate(client_id, client_secret)

    upload_api_to_bigquery(pipeline=pipeline, client_id=client_id)

    return 0


if __name__ == "__main__":
    exit_code = main()
    if exit_code == 0:
        logging.info("Exited program with exit_code: 0")
    else:
        logging.info(f"Exited program with exit_code: {exit_code}")