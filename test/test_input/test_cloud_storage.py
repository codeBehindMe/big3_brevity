import asyncio

import pytest

from src.input.cloud_storage import CloudStoragePlanContainer


@pytest.mark.asyncio(scope="module")
class TestCloudStorage:
    @pytest.mark.asyncio
    async def test_available_plan_names(self):
        plan_container = await CloudStoragePlanContainer.create()
        assert len(plan_container.available_plan_names) > 0

    @pytest.mark.asyncio
    async def test_bucket_contents(self):
        plan_container = await CloudStoragePlanContainer.create()
        contents = await plan_container._get_bucket_contents()
        assert len(contents) > 0

    @pytest.mark.asyncio
    async def test_get_plan(self):
        container = await CloudStoragePlanContainer.create()

        plan_items = await container.get_plan("10k_run_imp")
        print(plan_items)
