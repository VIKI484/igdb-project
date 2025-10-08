from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv
import logging


class MLModel:
    def __init__(self) -> None:
        logging.info("Initializing MLModel")
        try:
            self.client = bigquery.Client()
            self.dataset_id = os.getenv("BQ_DATASET_ID")
            self.view_id = os.getenv("BQ_VIEW_ID")
            if not self.dataset_id or not self.view_id:
                raise ValueError("Environment variables BQ_DATASET_ID and BQ_VIEW_ID must be set")
            self.data = pd.DataFrame()
            logging.info(f"BigQuery Client initialized for dataset: {self.dataset_id}, table: {self.view_id}")
        except Exception as e:
            logging.error(f"Error initializing MLModel: {e}")
            raise

    def fetch_data(self) -> None:
        logging.info("Fetching data from BigQuery")
        try:
            query = f"""
            SELECT *
            FROM `{self.dataset_id}.{self.view_id}`
            """
            query_job = self.client.query(query)
            results = query_job.result()
            df = results.to_dataframe()
            self.data = df
            logging.info("Data fetched successfully")
        except Exception as e:
            logging.error(f"Error fetching data: {e}")
            raise

    def train_model(self):
        logging.info("Training model")
        if self.data.empty:
            logging.warning("No data available to train the model")
            return
        try:
            # Placeholder for model training logic
            logging.info("Model training completed successfully")
        except Exception as e:
            logging.error(f"Error during model training: {e}")
            raise

    def visualize_data(self, df):
        logging.info("Visualizing model data")
        if df.empty:
            logging.warning("No data available for visualization")
            return
        try:
            plt.figure(figsize=(10, 6))
            plt.hist(df.iloc[:, 0], bins=30, alpha=0.7)
            plt.title('Data Distribution')
            plt.xlabel('Value')
            plt.ylabel('Frequency')
            plt.show()
            logging.info("Data visualization completed successfully")
        except Exception as e:
            logging.error(f"Error during data visualization: {e}")
            raise

def main():
    load_dotenv()  # Load environment variables from .env file

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    logging.info("Starting ML Model Training Pipeline")

    model = MLModel()
    model.fetch_data()
    model.visualize_data(model.data)
    model.train_model()


if __name__ == "__main__":
    main()