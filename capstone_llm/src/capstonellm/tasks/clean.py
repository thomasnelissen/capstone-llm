import argparse
import logging
import subprocess
import os
from pyspark.sql import SparkSession

from capstonellm.common.spark import ClosableSparkSession

logger = logging.getLogger(__name__)

def clean(spark: SparkSession, environment: str, tag: str):
    # download the ingested data from S3 for the given tag
    print(f"Downloading {tag}/question.json")
    if environment == "local" and not os.path.isfile(f"./data_in/{tag}/questions.json"):
        subprocess.run(["aws", "s3", "cp", f"s3://dataminded-academy-capstone-llm-data-us/input/{tag}/questions.json", f"./data_in/{tag}/"])
    print(f"Downloading {tag}/answers.json")
    if environment == "local" and not os.path.isfile("./data_in/" +tag +"/answers.json"):
        subprocess.run(["aws", "s3", "cp", f"s3://dataminded-academy-capstone-llm-data-us/input/{tag}/answers.json", f"./data_in/{tag}/"])

    # process the data
    questions = spark.read.json(f"./data_in/{tag}/questions.json")
    answers = spark.read.json(f"./data_in/{tag}/answers.json")

    questions_w_answers = (
        questions
            .join(answers, on="question_id", how="inner")
            .select(questions.title, questions.body.alias("question"), answers.body.alias("answer"))
    )

    questions_w_answers.to_json(f"./data_out/{tag}.json")

    

def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=True
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
    )
    logger.info("starting the cleaning job")

    args = parser.parse_args()
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }
    if args.env == "local":
        session = (
            SparkSession.builder.appName("Spark S3 Integration")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
            .getOrCreate()
        )
        clean(session, args.env, args.tag)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()
