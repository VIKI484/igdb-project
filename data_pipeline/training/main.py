from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv
import logging
import yaml


class MLModel:
    def __init__(self, dataset_id: str, view_id: str) -> None:
        logging.info("Initializing ML model")
        try:
            self.client = bigquery.Client()
            self.dataset_id = dataset_id
            self.view_id = view_id
            self.data = pd.DataFrame(columns=["genre", "release_year", "release_month", "rating"])
            logging.info(f"BigQuery Client initialized for dataset: {self.dataset_id}, view: {self.view_id}")
        except Exception as e:
            logging.error(f"Error initializing ML model: {e}")
            raise

    def fetch_data(self) -> None:
        logging.info("Fetching data from BigQuery")
        try:
            query = f"""
            SELECT *
            FROM `{self.dataset_id}.{self.view_id}`
            SORT BY genre, release_year, release_month
            """
            query_job = self.client.query(query)
            results = query_job.result()
            df = results.to_dataframe()
            self.data = pd.concat([self.data, df]).drop_duplicates()
            logging.info("Data fetched successfully")
        except Exception as e:
            logging.error(f"Error fetching data: {e}")
            raise

    def visualize_data(self) -> None:
        logging.info("Visualizing model data")
        if self.data.empty:
            logging.warning("No data available for visualization")
            return
        try:
            plt.figure(figsize=(10, 6))
            plt.hist(self.data.iloc[:, 0], bins=30, alpha=0.7)
            plt.title('Data Distribution')
            plt.xlabel('Value')
            plt.ylabel('Frequency')
            plt.show()
            logging.info("Data visualization completed successfully")
        except Exception as e:
            logging.error(f"Error during data visualization: {e}")
            raise

    def train_model(self) -> None:
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

    def predict(self) -> None:
        logging.info("Making prediction")
        try:
            if self.data.empty:
                logging.warning("No data available in the model")
                return
            fig, ax = plt.subplots()
            for genre in self.data["genre"].unique():
                df_filter = self.data["genre"] == genre
                df = self.data[df_filter]
                ax.plot([f"{year}/{month}" for year, month in df[["relese_year", "release_month"]]],
                        df["rating"])
            plt.show()
        except Exception as e:
            logging.error(f"Error during prediction: {e}")
            raise

def main():
    load_dotenv()  # Load environment variables from .env file

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    logging.info("Starting ML Model Training Pipeline")

    with open("value_config.yml", "r") as f:
        config = yaml.safe_load(f)
    dataset_id = config.get("dataset_id")
    view_id = config.get("view_id")

    model = MLModel(dataset_id=dataset_id, view_id=view_id)
    model.fetch_data()
    model.visualize_data(model.data)
    model.train_model()


if __name__ == "__main__":
    main()