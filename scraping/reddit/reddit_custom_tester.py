import asyncio
import datetime as dt
import json
import os
import random

import pymongo
from dotenv import load_dotenv

from common.data import DataEntity, DataLabel, DataSource
from scraping.reddit.reddit_custom_scraper import RedditCustomScraper

load_dotenv()

test_sample_size = 10


def get_mongo_dataset():
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    db_name = os.getenv("MONGO_DB", "reddit_miner_db_in_house")
    collection_name = "reddit_posts"

    try:
        print("Connecting to MongoDB...")
        print(f"Mongo URI: {mongo_uri} - DB Name: {db_name} - Collection Name: {collection_name}")

        client = pymongo.MongoClient(mongo_uri)
        db = client[os.getenv("MONGO_DB", "reddit_miner_db_in_house")]
        collection = db[collection_name]
        data = list(collection.find({}).limit(1000))

    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        data = []
    print(f"Connection established - Posts fetched: {len(data)}")
    return data


async def test_validate():
    global test_sample_size

    scraper = RedditCustomScraper()

    # This test covers a top level comment, a submission, and a nested comment with both the correct parent id and the submission id in order.
    # Previous versions of the custom scraper incorrectly got the submission id as the parent id for nested comments.
    dataset = get_mongo_dataset()

    if len(dataset) == 0:
        print("No data in the database to test. Exiting...")
        return

    if len(dataset) < test_sample_size:
        test_sample_size = len(dataset)

    # choosing a random sample of 10 posts
    dataset = random.sample(dataset, test_sample_size)
    entities = []
    print(f"Expecting Pass. Sample count to be tested: {test_sample_size}")

    for i, post in enumerate(dataset):
        post = post["bittensor_data"]
        content_byte = json.dumps(post["content"], separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        entities.append(
            DataEntity(
                uri=post["uri"],
                datetime=dt.datetime.fromisoformat(post["datetime"]),
                source=DataSource.REDDIT,
                label=DataLabel(value=post["label"]),
                content=content_byte,
                content_size_bytes=len(content_byte),
            )
        )
    results = await scraper.validate(entities=entities)
    successful_results = [r for r in results if r.is_valid]
    print("===================")
    print(f"Success rate: {len(successful_results)} / {len(results)}")
    print("===================")

    print(f"Expecting Pass. Validation results: {results}")


if __name__ == "__main__":
    asyncio.run(test_validate())


# python -m scraping.reddit.reddit_custom_tester
