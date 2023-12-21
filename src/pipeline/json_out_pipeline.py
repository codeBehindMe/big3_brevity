from typing import List
from src.source.cloud_storage import CloudStoragePlanContainer


class AsyncProcessorPipeline:
    
    storage_container : CloudStoragePlanContainer

    @classmethod
    async def create(cls):
      inst = cls()
      inst.storage_container = await CloudStoragePlanContainer.create()
      return inst
      
    
    async def list_plans(self):
      plans : List[str]= self.storage_container.available_plan_names()

      for p in plans:
        self.storage_container.get_plan(p)