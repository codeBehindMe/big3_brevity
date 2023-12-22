import asyncio
from typing import Dict, Final, List, Self, Tuple

from gcloud.aio.storage import Bucket, Storage

from src.logger import AppLogger, get_or_create_logger

_singleton = None
PLANS_BUCKET_NAME : Final[str] = "big3-plans"

class RawPlan:
    def __init__(self, name: str) -> None:
        self.name = name

    def get_weeks() -> None:
        pass

class CloudStorageSource:
    plans : List[str]
    
    @classmethod
    async def create(cls):
        global _singleton

        if _singleton is None:
            inst = cls()
            inst.plans = await inst.get_blobs()
            _singleton = inst
            return _singleton
        return _singleton

    async def get_blobs(self):
        async with Storage() as client:
            return await Bucket(client, PLANS_BUCKET_NAME).list_blobs()

    async def get_plans(self) -> List[str]:
        blobs = await self.get_blobs()
        return list(set(map(lambda x: x.split("/")[0], blobs)))

    async def get_plan(self, plan_name):
        pass
        

