import os


def create_directory_structure():
    """Create the necessary directory structure for the project"""
    directories = [
        "src/data_preparation",
        "src/glue_jobs",
        "src/athena",
        "data/raw",
        "data/parquet",
    ]

    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")


if __name__ == "__main__":
    create_directory_structure()
