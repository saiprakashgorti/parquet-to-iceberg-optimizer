import os
import kaggle
import pandas as pd
from datetime import datetime
import boto3
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_kaggle():
    """Set up Kaggle API credentials"""
    # Create .kaggle directory if it doesn't exist
    os.makedirs(os.path.expanduser("~/.kaggle"), exist_ok=True)

    # Check if kaggle.json exists
    if not os.path.exists(os.path.expanduser("~/.kaggle/kaggle.json")):
        logger.error(
            "Please download your kaggle.json file from https://www.kaggle.com/settings/account and place it in ~/.kaggle/"
        )
        raise FileNotFoundError("kaggle.json not found")


def download_dataset():
    """Download the NYC Airbnb dataset"""
    logger.info("Downloading NYC Airbnb dataset...")
    kaggle.api.dataset_download_files(
        "dgomonov/new-york-city-airbnb-open-data", path="./data/raw", unzip=True
    )
    logger.info("Dataset downloaded successfully")


def prepare_data():
    """Prepare the data for Parquet format"""
    logger.info("Preparing data...")

    # Read the CSV file
    df = pd.read_csv("./data/raw/AB_NYC_2019.csv")

    # Convert last_review to datetime
    df["last_review"] = pd.to_datetime(df["last_review"])

    # Create partition columns
    df["year"] = df["last_review"].dt.year
    df["month"] = df["last_review"].dt.month
    df["day"] = df["last_review"].dt.day

    # Create output directory
    os.makedirs("./data/parquet", exist_ok=True)

    # Save as Parquet
    df.to_parquet(
        "./data/parquet/airbnb_data.parquet",
        partition_cols=["year", "month", "day"],
        index=False,
    )
    logger.info("Data prepared and saved as Parquet")


def upload_to_s3():
    """Upload the Parquet data to S3"""
    logger.info("Uploading data to S3...")

    # Initialize S3 client
    s3 = boto3.client("s3")

    # Upload the entire parquet directory
    for root, dirs, files in os.walk("./data/parquet"):
        for file in files:
            local_path = os.path.join(root, file)
            s3_path = (
                f"airbnb-data/parquet/{os.path.relpath(local_path, './data/parquet')}"
            )

            s3.upload_file(
                local_path, "your-bucket-name", s3_path  # Replace with your bucket name
            )

    logger.info("Data uploaded to S3 successfully")


def main():
    try:
        setup_kaggle()
        download_dataset()
        prepare_data()
        upload_to_s3()
        logger.info("Data preparation completed successfully")
    except Exception as e:
        logger.error(f"Error during data preparation: {str(e)}")
        raise


if __name__ == "__main__":
    main()
