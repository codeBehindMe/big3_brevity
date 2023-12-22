import asyncio
import os
from dataclasses import dataclass
from json import JSONDecodeError
from typing import Dict, Final, List

from gcloud.aio.storage import Bucket, Storage

from firestore import Firestore
from logger import get_or_create_logger
from oai import GPTPlanProcessor

mof_limiter = asyncio.Semaphore(1000)  # max open files limiter

PLANS_BUCKET_NAME: Final[str] = "big3-plans"

app_logger = get_or_create_logger()


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
    app_logger.info(f"processing week {week_blob_name}")
    week_name = week_blob_name.split("/")[1]

    app_logger.debug(f"downloading blob for processing {week_blob_name}")
    blob_content = await client.download(
        bucket=PLANS_BUCKET_NAME, object_name=week_blob_name
    )
    app_logger.debug(f"summarising week {week_blob_name}")
    try:
        summarised_week = await proc.summarise_week(blob_content.decode("utf-8"))
    except JSONDecodeError:
        app_logger.error(f"could not summarise week : {week_blob_name}")
        return Week(name=week_name, data={"error": "error"})
    return Week(name=week_name, data=summarised_week)


async def process_plan(
    plan_name: str,
    bucket_contents: List[str],
    processor: GPTPlanProcessor,
    firestore: Firestore,
):
    app_logger.info(f"starting processing plan {plan_name}")
    async with Storage() as client:
        plan_blobs = filter(lambda x: x.startswith(plan_name), bucket_contents)

        async with asyncio.TaskGroup() as tg:
            week_proc_tasks = []
            for blob in plan_blobs:
                if blob.split("/")[1].lower() == "overview.md":
                    app_logger.debug(f"downloading overview for {plan_name}")
                    overview = await client.download(
                        bucket=PLANS_BUCKET_NAME, object_name=blob
                    )
                else:
                    week_proc_tasks.append(
                        tg.create_task(process_week_blob(blob, client, processor))
                    )

    p = Plan(
        name=plan_name,
        overview=overview.decode("utf-8"),
        weeks=[t.result() for t in week_proc_tasks],
    )
    app_logger.info(f"finished creating plan for {p.name}")

    app_logger.info(f"adding plan {p.name} to firestore")
    await firestore.add_document("plans", p.name, p.to_dict())


async def process_plans_in_bucket(oai_token: str, database_name: str):
    app_logger.info(f"getting available plans in {PLANS_BUCKET_NAME}")
    async with Storage() as client:
        bucket_contents = await Bucket(client, PLANS_BUCKET_NAME).list_blobs()
    plan_names = list(set(map(lambda x: x.split("/")[0], bucket_contents)))
    plan_names = ["kb_working_str"]
    app_logger.info(f"found plans: {plan_names}")

    app_logger.info(f"processing plans")

    app_logger.debug(f"creating processor")
    proc = GPTPlanProcessor(oai_token)

    app_logger.debug(f"creating firestore client for database {database_name}")
    f_store = Firestore(database_name)
    async with asyncio.TaskGroup() as tg:
        tasks = [
            tg.create_task(
                process_plan(
                    plan_name=plan,
                    bucket_contents=bucket_contents,
                    processor=proc,
                    firestore=f_store,
                )
            )
            for plan in plan_names
        ]
    [t.result() for t in tasks]


async def main():
    oai_token = os.environ["OAI_TOKEN"]
    database_name = os.environ["TARGET_DATABASE"]

    print(await process_plans_in_bucket(oai_token, database_name))


if __name__ == "__main__":
    asyncio.run(main())
