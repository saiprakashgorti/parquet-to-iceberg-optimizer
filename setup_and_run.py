import os
import boto3
import logging
import subprocess
import time
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_aws_resources():
    """Set up required AWS resources"""
    try:
        # Initialize AWS clients
        s3 = boto3.client("s3")
        glue = boto3.client("glue")
        iam = boto3.client("iam")

        # Create S3 bucket
        bucket_name = f"airbnb-iceberg-demo-{int(time.time())}"
        logger.info(f"Creating S3 bucket: {bucket_name}")
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )

        # Create Glue database
        database_name = "airbnb_db"
        logger.info(f"Creating Glue database: {database_name}")
        try:
            glue.create_database(
                DatabaseInput={
                    "Name": database_name,
                    "Description": "Database for Airbnb data migration demo",
                }
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "AlreadyExistsException":
                logger.info("Database already exists")
            else:
                raise

        # Create Glue role if it doesn't exist
        role_name = "GlueIcebergDemoRole"
        try:
            iam.get_role(RoleName=role_name)
            logger.info("IAM role already exists")
        except ClientError:
            logger.info(f"Creating IAM role: {role_name}")
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "glue.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }

            iam.create_role(
                RoleName=role_name, AssumeRolePolicyDocument=str(trust_policy)
            )

            # Attach necessary policies
            policies = [
                "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
                "arn:aws:iam::aws:policy/AmazonS3FullAccess",
            ]

            for policy in policies:
                iam.attach_role_policy(RoleName=role_name, PolicyArn=policy)

        return bucket_name, database_name, role_name

    except Exception as e:
        logger.error(f"Error setting up AWS resources: {str(e)}")
        raise


def update_config_files(bucket_name):
    """Update configuration files with the new bucket name"""
    try:
        # Update download_kaggle_dataset.py
        with open("src/data_preparation/download_kaggle_dataset.py", "r") as f:
            content = f.read()

        content = content.replace('"your-bucket-name"', f'"{bucket_name}"')

        with open("src/data_preparation/download_kaggle_dataset.py", "w") as f:
            f.write(content)

        logger.info("Updated configuration files with bucket name")

    except Exception as e:
        logger.error(f"Error updating configuration files: {str(e)}")
        raise


def create_glue_job(bucket_name, database_name, role_name):
    """Create and configure the Glue job"""
    try:
        glue = boto3.client("glue")

        # Upload the Glue job script to S3
        s3 = boto3.client("s3")
        script_path = "src/glue_jobs/airbnb_parquet_to_iceberg.py"
        s3.upload_file(script_path, bucket_name, "scripts/airbnb_parquet_to_iceberg.py")

        # Create the Glue job
        job_name = "AirbnbParquetToIceberg"
        logger.info(f"Creating Glue job: {job_name}")

        glue.create_job(
            Name=job_name,
            Role=role_name,
            Command={
                "Name": "glueetl",
                "ScriptLocation": f"s3://{bucket_name}/scripts/airbnb_parquet_to_iceberg.py",
            },
            DefaultArguments={
                "--job-language": "python",
                "--job-bookmark-option": "job-bookmark-enable",
                "--source_path": f"s3://{bucket_name}/airbnb-data/parquet/",
                "--target_path": f"s3://{bucket_name}/airbnb-data/iceberg/",
                "--database_name": database_name,
                "--table_name": "listings",
                "--additional-python-modules": "apache-iceberg==0.7.0",
            },
            ExecutionProperty={"MaxConcurrentRuns": 1},
            MaxRetries=0,
            Timeout=60,
            NumberOfWorkers=2,
            WorkerType="G.1X",
        )

        return job_name

    except Exception as e:
        logger.error(f"Error creating Glue job: {str(e)}")
        raise


def main():
    try:
        # Step 1: Set up AWS resources
        bucket_name, database_name, role_name = setup_aws_resources()

        # Step 2: Update configuration files
        update_config_files(bucket_name)

        # Step 3: Download and prepare data
        logger.info("Running data preparation script...")
        subprocess.run(
            ["python", "src/data_preparation/download_kaggle_dataset.py"], check=True
        )

        # Step 4: Create and run Glue job
        job_name = create_glue_job(bucket_name, database_name, role_name)

        logger.info("Starting Glue job...")
        glue = boto3.client("glue")
        response = glue.start_job_run(JobName=job_name)
        job_run_id = response["JobRunId"]

        logger.info(f"Glue job started with ID: {job_run_id}")
        logger.info("You can monitor the job in the AWS Glue console")

        # Print instructions for monitoring
        print("\nSetup completed! Here's what to do next:")
        print(
            f"1. Monitor the Glue job in AWS Console: https://console.aws.amazon.com/glue/home?region=us-west-2#etl:tab=jobs"
        )
        print(
            f"2. Check the data in S3: https://s3.console.aws.amazon.com/s3/buckets/{bucket_name}"
        )
        print(f"3. Query the data in Athena using the database: {database_name}")

    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise


if __name__ == "__main__":
    main()
