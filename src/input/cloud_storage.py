from gcloud.aio.storage import Storage, Bucket, Blob

from typing import Final, List, Self

PLANS_BUCKET_NAME: Final[str] = "big3-plans"

class RawPlan:
    def __init__(self, overview: str, **weeks : str) -> None:
        self.overview = overview
        self.weeks = weeks

class CloudStoragePlans:
    _plan_names: List[str]
    _bucket_contents : List[str]

    @classmethod
    async def create(cls) -> Self:
        inst = cls()
        inst._bucket_contents = await inst._get_bucket_contents() 
        inst._plan_names = await inst._get_available_plan_names()
        return inst
    
    @property
    def available_plan_names(self) -> List[str]:
        if not self._plan_names:
            self._plan_names = self._get_available_plan_names()
        return self._plan_names

    async def _get_bucket_contents(self) -> List[str]:
        if not self._bucket_contents: # Check if contents have been feteched before
            async with Storage() as client:
                return await Bucket(client, PLANS_BUCKET_NAME).list_blobs()
        return self._bucket_contents

    async def _get_available_plan_names(self) -> List[str]:
        async with Storage() as client:
            bucket_contents : self._get_bucket_contents()
            return list(set(map(lambda x: x.split("/")[0], bucket_contents)))

    async def get_plan(self, plan_name: str) -> RawPlan:
        async with Storage() as client:
            bucket= Bucket(client, PLANS_BUCKET_NAME)
            for blob_name in map(lambda x: x.startswith(plan_name), self._get_bucket_contents()):
                await client.download(bucket=bucket,object_name=blob_name)
            
        
        