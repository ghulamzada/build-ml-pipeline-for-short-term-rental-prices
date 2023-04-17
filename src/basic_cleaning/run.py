#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd  # TODO


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):
    """
    This function will open the input artifact and return a cleaned csv file

    Args:
        - input_artifact: The raw csv file
        - min_price: Minimum value to detect/drop outlier
        - max_price: Maximum value to detect/drop outlier
        - output_artifact: Name of csv file to be saved
        - output_type: type of output artifact to be saved
        - output_description: Description of output artifact
    """

    # Creating a W&B run
    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    logger.info(f"downloading raw data")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info("Loading raw data ...")
    df = pd.read_csv(artifact_local_path)

    # Droping outlier depending on minimum and maximum values
    logger.info(f"Droping outliers in dataset ...")
    idx = df["price"].between(args.min_price, args.max_price)
    df = df[idx].copy()

    # Adjusting the boundries of "longitude" column
    idx = df['longitude'].between(-74.25, -
                                  73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    # saving the dataframe as a csv file
    df.to_csv("clean_sample.csv", index=False)

    # Uploading the csv file to W&B
    logger.info("Uploading cleaned file to W&B")
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")

    parser.add_argument(
        "--input_artifact",
        type=str,  # INSERT TYPE HERE: str, float or int,
        help="The input artifact (svaed in W&B)",  # INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,  # INSERT TYPE HERE: str, float or int,
        help="The name for the output artifact",  # INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,  # INSERT TYPE HERE: str, float or int,
        help="The type for the output artifact",  # INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,  # INSERT TYPE HERE: str, float or int,
        help="A description for the output artifact",  # INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--min_price",
        type=float,  # INSERT TYPE HERE: str, float or int,
        help="The minimum price to consider",  # INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=float,  # INSERT TYPE HERE: str, float or int,
        help="The maximum price to consider",  # INSERT DESCRIPTION HERE,
        required=True
    )

    args = parser.parse_args()

    go(args)
