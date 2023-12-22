import asyncio
import glob
import json
import logging
import os
import pathlib
from dataclasses import dataclass
from typing import Dict, Final, List

import aiofiles
import fire
import openai
from gcloud.aio.storage import Bucket, Storage

from firestore import Firestore
from src.logger import get_or_create_logger
from src.processor.oai import GPTPlanProcessor

mof_limiter = asyncio.Semaphore(1000)  # max open files limiter

PLANS_BUCKET_NAME: Final[str] = "big3-plans"


@dataclass
class Week:
    name: str
    data: Dict

    def to_dict(self) -> Dict:
        return {"name": self.name, "days": self.data}


@dataclass
class Plan:
    name: str
    overview: str
    weeks: List[Week]

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "overview": self.overview,
            "weeks": [w.to_dict() for w in self.weeks],
        }


async def process_week_blob(
    week_blob_name: str, client: Storage, proc: GPTPlanProcessor
):
    week_name = week_blob_name.split("/")[1]
    blob_content = await client.download(
        bucket=PLANS_BUCKET_NAME, object_name=week_blob_name
    )
    summarised_week = await proc.summarise_week(blob_content.decode("utf-8"))
    return Week(name=week_name, data=summarised_week)


async def process_plan(
    plan_name: str, bucket_contents: List[str], processor: GPTPlanProcessor
):
    async with Storage() as client:
        plan_blobs = filter(lambda x: x.startswith(plan_name), bucket_contents)

        async with asyncio.TaskGroup() as tg:
            week_proc_tasks = []
            for blob in plan_blobs:
                if blob.split("/")[1].lower() == "overview.md":
                    overview = await client.download(
                        bucket=PLANS_BUCKET_NAME, object_name=blob
                    )
                else:
                    week_proc_tasks.append(
                        tg.create_task(process_week_blob(blob, client, processor))
                    )

    print(
        Plan(
            name=plan_name,
            overview=overview.decode("utf-8"),
            weeks=[t.result() for t in week_proc_tasks],
        ).to_dict()
    )


async def process_plans_in_bucket():
    async with Storage() as client:
        bucket_contents = await Bucket(client, PLANS_BUCKET_NAME).list_blobs()

    plan_names = list(set(map(lambda x: x.split("/")[1], bucket_contents)))


async def main():
    proc = GPTPlanProcessor(os.environ["OAI_TOKEN"])
    async with Storage() as client:
        bucket_contents = await Bucket(client, PLANS_BUCKET_NAME).list_blobs()
    await process_plan("10k_run_imp", bucket_contents, proc)


if __name__ == "__main__":
    asyncio.run(main())
