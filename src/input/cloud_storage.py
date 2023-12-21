import asyncio
from typing import Dict, Final, List, Self, Tuple

from gcloud.aio.storage import Blob, Bucket, Storage

from src.logger import AppLogger, get_or_create_logger

PLANS_BUCKET_NAME: Final[str] = "big3-plans"


class RawPlan:
    _num_weeks: int
    def __init__(self, overview: str, **weeks: str) -> None:
        self.overview = overview
        self.weeks = weeks
        self._num_weeks = len(self.weeks.keys())

    @property
    def num_weeks(self):
        return self._num_weeks

    def get_week(self, week_id: str) -> str:
        return self.weeks[week_id]



class CloudStoragePlanContainer:
    _plan_names: List[str]
    _bucket_contents: List[str]
    _app_logger: AppLogger

    @classmethod
    async def create(cls) -> Self:
        inst = cls()
        inst._bucket_contents = await inst._get_bucket_contents()
        inst._plan_names = await inst._get_available_plan_names()
        inst._app_logger = get_or_create_logger()
        return inst

    @property
    def available_plan_names(self) -> List[str]:
        return self._plan_names

    async def _get_bucket_contents(self) -> List[str]:
        async with Storage() as client:
            return await Bucket(client, PLANS_BUCKET_NAME).list_blobs()

    async def _get_available_plan_names(self) -> List[str]:
        bucket_contents = await self._get_bucket_contents()
        return list(set(map(lambda x: x.split("/")[0], bucket_contents)))

    @staticmethod
    async def _get_plan_week_obj(client: Storage, obj_name: str) -> Tuple[str, str]:
        return obj_name.split("/")[1].lower(), await client.download(
            bucket=PLANS_BUCKET_NAME, object_name=obj_name
        )

    async def get_plan(self, plan_name: str) -> RawPlan:
        weeks: Dict[str, str] = {}
        overview: str = ""

        async with Storage() as client:
            blob_names = filter(
                lambda x: x.startswith(plan_name), self._bucket_contents
            )

            async with asyncio.TaskGroup() as tg:
                week_tasks = []
                for b_name in blob_names:
                    if b_name.split("/")[1].lower() == "overview":
                        overview = await client.download(
                            bucket=PLANS_BUCKET_NAME, object_name=b_name
                        )
                    else:
                        week_tasks.append(
                            tg.create_task(
                                self._get_plan_week_obj(client=client, obj_name=b_name)
                            )
                        )

        for weekname, content in [t.result() for t in week_tasks]:
            weeks[weekname] = content

        return RawPlan(overview, **weeks)