import os
import openai
import glob
import fire
import json
import logging
import asyncio
import aiofiles
import pathlib


mof_limiter = asyncio.Semaphore(1000)  # max open files limiter

__key_set = False

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


def __set_key():
    global __key_set
    if not __key_set:
        with open(".key", "r") as f:
            openai.api_key = f.readline()


async def _summarize_content(content: str):
    logging.info("requesting summary from gpt-3.5.-turbo")
    resp = await openai.ChatCompletion.acreate(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "summarise workout plan text into a JSON"},
            {"role": "user", "content": content},
        ],
    )

    return json.loads(resp["choices"][0]["message"]["content"])


async def _week(input, output):
    logging.info(f"summarising file {input}")

    async with aiofiles.open(input, "r") as f:
        lines = await f.readlines()
        content = "".join(lines)

    json_summary = await _summarize_content(content=content)

    if os.path.dirname(output) != "":
        os.makedirs(os.path.dirname(output), exist_ok=True)
    logging.info(f"writing file to {output}")
    async with aiofiles.open(output, "w") as f:
        await f.write(json.dumps(json_summary))

    logging.info("finished")


async def _folder(infolder, outfolder):
    # FIXME: Remember to limit the num open files
    files = glob.glob(f"{infolder}/*")
    [logging.info(f"found file: {f}") for f in files]

    async with asyncio.TaskGroup() as tg:
        tasks = [
            tg.create_task(_week(f, f"{outfolder}/{os.path.basename(f)}.json"))
            for f in files
        ]


async def _to_markdown(content: str) -> str:
    if not input.endswith(".json"):
        raise ValueError("expected JSON input")

    logging.info("requesting markdown conversion from gpt-3.5-turbo")
    resp = await openai.ChatCompletion.acreate(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "convert the json string into a markdown"},
            {"role": "user", "content": content},
        ],
    )

    return resp["choices"][0]["message"]["content"]


async def _read_week(path: str) -> str:
    logging.info(f"reading file {path}")
    async with mof_limiter:
        async with aiofiles.open(path, "r") as f:
            lines = await f.read()
            return pathlib.Path(path).stem, "".join(lines)


async def _join(folder, out: str):
    files = glob.glob(f"{folder}/*.json")
    (logging.info(f"found file: {f}") for f in files)

    # FIXME: Extend taskgroup for a nicer syntax to get results
    async with asyncio.TaskGroup() as tg:
        tasks = [tg.create_task(_read_week(f)) for f in files]

    results = [t.result() for t in tasks]

    json_results = []

    for x, y in results:
        json_results.append((x, json.loads(y)))

    combined = {}
    for week, week_plan in json_results:
        combined[week] = week_plan

    async with aiofiles.open(out, "w") as f:
        await f.write(json.dumps(combined))

    logging.info("completed weeks")


class Summarizer:
    def week(self, input, output):
        asyncio.run(_week(input, output))

    def folder(self, infolder, outfolder):
        asyncio.run(_folder(infolder=infolder, outfolder=outfolder))

    def join(self, folder: str, out: str):
        asyncio.run(_join(folder=folder, out=out))


if __name__ == "__main__":
    __set_key()
    fire.Fire(Summarizer)
