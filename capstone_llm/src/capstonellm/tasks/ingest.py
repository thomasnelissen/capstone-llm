import argparse
import requests
import boto3
import json

import logging
logger = logging.getLogger(__name__)
base_url = "https://api.stackexchange.com/2.3"

def ingest(tag: str):
    # Load questions & answers from stackoverflow API
    questions = get_all_pages("/questions", tag)
    answers = get_all_pages("/answers", tag)

    # upload to s3
    upload_s3(questions, 'questions.json', tag)
    upload_s3(answers, 'answers.json', tag)

def upload_s3(json, filename, tag):
    s3_bucket = f"s3a://dataminded-academy-capstone-llm-data-us/input/{tag}/thomas/"
    s3_client = boto3.resource('s3')
    s3object = s3_client.Object(s3_bucket, filename)
    s3object.put(
        Body=(bytes(json.dumps(json).encode('UTF-8')))
    )


def get_all_pages(path, tag):
    has_next = True
    page = 1
    items = []
    while has_next:
        params = "?site=stackoverflow&pagesize=20&fromdate=2024-01-01&filter=withbodytagged="+tag+"&page="+str(page)
        response = requests.get(base_url+path+params)
        response_json = json.loads(response.text)
        items += response_json.items
        has_next = response_json.has_next
    return items
    

def main():
    parser = argparse.ArgumentParser(description="stackoverflow ingest")
    parser.add_argument(
        "-t", "--tag", dest="tag", help="Tag of the question in stackoverflow to process",
        default="python-polars", required=False
    )
    args = parser.parse_args()
    logger.info("Starting the ingest job")

    ingest(args.tag)


if __name__ == "__main__":
    main()
