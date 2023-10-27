import os
import openai
import glob
import fire
import json
import logging
import asyncio
import aiofiles

__key_set = False

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


def __set_key():
    global __key_set
    if not __key_set:
        with open(".key", "r") as f:
            openai.api_key = f.readline()


async def _summarize_content(content: str):
    __set_key()
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


class Summarizer:
    def week(self, input, output):
        asyncio.run(_week(input, output))

    def folder(self, infolder, outfolder):
        files = glob.glob(f"{infolder}/*")

        tasks = [self.week(f, f"{outfolder}/{os.path.basename(f)}.json") for f in files]
        asyncio.gather(*tasks)


if __name__ == "__main__":
    __set_key()
    fire.Fire(Summarizer)
