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

    with aiofiles.open(input, "w") as f:
        lines = await f.readlines()
        content = "".join(lines)


class Summarizer:
    async def week(self, input, output):
        logging.info(f"summarizing input {input}")

        with open(input, "r") as f:
            content = "".join(f.readlines())

        j = await _summarize_content(content=content)
        if os.path.dirname(output) != "":
            os.makedirs(os.path.dirname(output), exist_ok=True)
        async with aiofiles.open(output, "w") as f:
            await json.dump(j, f)

    def folder(self, infolder, outfolder):
        files = glob.glob(f"{infolder}/*")

        tasks = [self.week(f, f"{outfolder}/{os.path.basename(f)}.json") for f in files]
        asyncio.gather(*tasks)


if __name__ == "__main__":
    __set_key()
    fire.Fire(Summarizer)
