from typing import Final, List, Self

from gcloud.aio.storage import Blob, Bucket, Storage

PLANS_BUCKET_NAME: Final[str] = "big3-plans"


class RawPlan:
    def __init__(self, overview: str, **weeks: str) -> None:
        self.overview = overview
        self.weeks = weeks


class CloudStoragePlanContainer:
    _plan_names: List[str]
    _bucket_contents: List[str]

    @classmethod
    async def create(cls) -> Self:
        inst = cls()
        inst._bucket_contents = await inst._get_bucket_contents()
        inst._plan_names = await inst._get_available_plan_names()
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

    async def get_plan(self, plan_name: str) -> RawPlan:
        blist = []
        async with Storage() as client:
            bucket = Bucket(client, PLANS_BUCKET_NAME)
            for blob_name in filter(
                lambda x: x.startswith(plan_name), self._bucket_contents
            ):
                blist.append(
                    await client.download(bucket=bucket.name, object_name=blob_name)
                )

            return blist
