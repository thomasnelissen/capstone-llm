import argparse
import logging
import pyspark.sql.functions as psf
from pyspark.sql import SparkSession

from capstonellm.common.spark import ClosableSparkSession

logger = logging.getLogger(__name__)

def clean(spark: SparkSession, environment: str, tag: str):

    # download the ingested data from S3 for the given tag
    # subprocess.run(["aws", "s3", "cp", f"s3://dataminded-academy-capstone-llm-data-us/input/{tag}/questions.json", f"./data_in/{tag}/"])
    # subprocess.run(["aws", "s3", "cp", f"s3://dataminded-academy-capstone-llm-data-us/input/{tag}/answers.json", f"./data_in/{tag}/"])

    # Read JSON files into a DataFrame
    # questions_in = spark.read.json(f"./data_in/{tag}/questions.json")
    answers_in = spark.read.json(f"s3a://dataminded-academy-capstone-llm-data-us/input/{tag}/answers.json")
    questions_in = spark.read.json(f"s3a://dataminded-academy-capstone-llm-data-us/input/{tag}/questions.json")

    # Flatten JSON Files
    questions = ( 
        questions_in
            .select(psf.explode(questions_in.items).alias("items"))
            .filter(psf.col("items.accepted_answer_id").isNotNull())
            .select(psf.col("items.body").alias("question"), "items.title", "items.is_answered", "items.accepted_answer_id")
    )
    answers = ( 
        answers_in
            .select(psf.explode(answers_in.items).alias("items"))
            .select(psf.col("items.body").alias("answer"), "items.answer_id", "items.question_id")
    )

    # Join questions with answers
    output = (
        questions
            .join(answers, on=(questions.accepted_answer_id == answers.answer_id))
            .select("title", "question", "answer")
    )

    # Write output to S3 bucket
    output.repartition(output.count()).write.mode("overwrite").json(f"s3a://dataminded-academy-capstone-llm-data-us/cleaned/{tag}/thomas/")
    print(f"Uploaded files to AWS Bucket /cleaned/{tag}/thomas/")
    

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
