import json
import os
import time

import praw

from lib.metering import Timer
from lib.monitoring import Monitoring
from lib.redirect import RedirectOutput

DEFAULT_SUBREDDIT_NAME = "synthesizers"

DEFAULT_PROFILE_NAME = "synth_bots"

DEFAULT_SCHEDULE_PATH = "./data/synths_orchestrator_bot/default_orchestrator_schedule.json"
SAVED_SCHEDULE_PATH = "/tmp/synths_orchestrator_schedule.json"


class SynthsBotsOrchestrator:
    def __init__(self, subreddit_name=DEFAULT_SUBREDDIT_NAME, dry_run=False, profile_name=None):
        self.subreddit_name = subreddit_name
        self.dry_run = dry_run

        self.reddit = praw.Reddit('SynthesizersBot')
        self.monitoring = Monitoring(profile_name)

    def orchestrate(self):
        schedule = self.load_schedule()
        self.execute_schedule(schedule)
        self.save_schedule(schedule)

    def execute_schedule(self, schedule):
        for item in schedule:
            interval_mins = item["interval"]
            last_run = item.setdefault("last_run", None)
            now = time.time()

            if last_run is None or (now - last_run) / 60 >= interval_mins:
                bot_class_name = item["name"]
                bot = self.instantiate_bot(bot_class_name)(
                    subreddit_name=self.subreddit_name,
                    dry_run=self.dry_run,
                    reddit=self.reddit
                )
                bot_name = type(bot).__name__

                try:
                    duration, output = self.execute_bot_scan(bot)
                    self.publish_bot_events(bot_name, duration, output)
                    item["last_run"] = now
                except Exception:
                    self.monitoring.publish_metric(bot_name, "Errors", 1, "Count")
                    raise

    def execute_bot_scan(self, bot_instance):
        with RedirectOutput() as output:
            with Timer() as timer:
                bot_instance.scan()

        duration_millis = timer()
        bot_output = output()

        return duration_millis, bot_output

    def publish_bot_events(self, bot_name, duration_millis, bot_output):
        self.monitoring.publish_bot_execution_metrics(bot_name, duration_millis)
        self.monitoring.publish_log_events(bot_name, bot_output)

    def instantiate_bot(self, bot_name):
        module_name, class_name = bot_name.split(".")
        module = __import__(module_name)
        return getattr(module, class_name)

    def load_schedule(self):
        file_name = SAVED_SCHEDULE_PATH if os.path.exists(SAVED_SCHEDULE_PATH) else DEFAULT_SCHEDULE_PATH
        with open(file_name, encoding='utf-8') as file:
            return json.load(file)

    def save_schedule(self, schedule):
        with open(SAVED_SCHEDULE_PATH, "w", encoding='utf-8') as file:
            json.dump(schedule, file)


def main(profile_name=None):
    dry_run = os.environ["dry_run"] == "True" if "dry_run" in os.environ else False
    subreddit_name = os.environ["subreddit_name"] if "subreddit_name" in os.environ else DEFAULT_SUBREDDIT_NAME

    orchestrator = SynthsBotsOrchestrator(
        subreddit_name=subreddit_name, dry_run=dry_run, profile_name=profile_name)
    orchestrator.orchestrate()


def lambda_handler(event=None, context=None):  # pylint: disable=unused-argument
    main()


if __name__ == '__main__':
    main(profile_name=DEFAULT_PROFILE_NAME)
