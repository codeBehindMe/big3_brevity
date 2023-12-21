import asyncio

import pytest

from src.plan.cloud_storage import CloudStoragePlanContainer
from src.logger import AppLogger, get_or_create_logger


@pytest.fixture(scope="session")
def get_logger() -> AppLogger:
    return get_or_create_logger("BIG3BREVITYTEST")


@pytest.mark.usefixtures("get_logger")
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

        raw_plan = await container.get_plan("10k_run_imp")
