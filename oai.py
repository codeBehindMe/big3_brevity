import json
from typing import Dict, Final

from openai import AsyncOpenAI

from logger import get_or_create_logger

EXAMPLE_FORMAT: Final[
    str
] = """{"Monday": {
    "Session": 7,
    "Objective": "Strength",
    "Warm up": [
      "3 Rounds",
      "Barbell Complex @ 45/65#",
      "6x Hand Release Push Ups",
      "Instep Stretch",
      "5x Shoulder Dislocates"
    ],
    "Training": [
      "8x Back Squat @ 50% 1RM, then \u2026. 6x Back Squat @ 70% 1RM, then \u2026.",
      "5 Rounds, Every 90 Seconds:",
      "4x Back Squat @ 85% 1RM",
      "8x Bench Press @ 50% 1RM, then .... 6x Bench Press @ 70% 1RM, then \u2026.",
      "5 Rounds, Every 90 Seconds:",
      "4x Bench Press @ 85% 1RM",
      "8x Dead Lift @ 50% 1RM, then \u2026 6x Dead Lift @ 70% 1RM, then \u2026",
      "5 Rounds, Every 90 Seconds:",
      "4x Dead Lift @ 85% 1RM",
      "5 Rounds, Every 90 Seconds",
      "35% Max Rep Pull Ups",
      "Foam Roll Legs, Low Back"
    ],
    "Comments": "Use your SESSION 1 Strength Assessment results to calculate today\u2019s loading for the Back Squat, Bench Press and Dead Lift, and the reps for the Pull Ups."}}
    """

app_logger = get_or_create_logger()


class GPTPlanProcessor:
    def __init__(self, api_key: str) -> None:
        self.aclient = AsyncOpenAI(api_key=api_key)
        self.logger = get_or_create_logger()
        self.model = "gpt-3.5-turbo"

    @staticmethod
    def _uppercase_keys(d: Dict) -> Dict:
        ud = {}
        for k, v in d.items():
            if isinstance(v, dict):
                v = GPTPlanProcessor._uppercase_keys(v)
            ud[k.upper()] = v
        return ud

    async def summarise_week(self, content: str) -> Dict:
        self.logger.info("summarising week")
        resp = await self.aclient.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": f"summarise workout plan text into a JSON; here's an example of the required format style {EXAMPLE_FORMAT}",
                },
                {"role": "user", "content": content},
            ],
        )

        try:
            summarised_content = resp.choices[0].message.content
            d = json.loads(summarised_content)
        except json.JSONDecodeError as e:
            app_logger.error(f"could not parse json on content\n{summarised_content}\n")
            raise e
        return GPTPlanProcessor._uppercase_keys(d)
