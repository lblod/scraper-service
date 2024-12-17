import os
from multiprocessing import Process

from flask import jsonify, request
from werkzeug.exceptions import NotFound

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from lblod.spiders.lblod import LBLODSpider
from lblod.job import load_task, update_task_status, TaskNotFoundException
from lblod.harvester import get_harvest_collection_for_task, get_initial_remote_data_object
from helpers import logger
from constants import OPERATIONS, TASK_STATUSES

AUTO_RUN = os.getenv("AUTO_RUN") in ["yes", "on", "true", True, "1", 1]
DEFAULT_GRAPH = os.getenv("DEFAULT_GRAPH", "http://mu.semte.ch/graphs/scraper-graph")
MU_APPLICATION_FILE_STORAGE_PATH = os.getenv("MU_APPLICATION_FILE_STORAGE_PATH", "")
STORE_OVERVIEW_PAGES = os.getenv("STORE_OVERVIEW_PAGES") in ["yes", "on", "true", True, "1", 1]

def run_spider(spider, **kwargs):
    def _run():
        crawler_process = CrawlerProcess(get_project_settings())
        crawler_process.crawl(spider, **kwargs)
        crawler_process.start()

    process = Process(target=_run)
    process.start()
    return process
@app.route("/scrape", methods=["POST"])
def scrape():
    if "url" in request.args:
        start_urls = [request.args["url"]]
        run_spider(LBLODSpider, start_urls=start_urls)
        return jsonify({"message": "Scraping started"})
    else:
        return jsonify({"error": "URL parameter is missing"})

@app.route("/delta", methods=["POST"])
def delta_handler():
    # Only trigger in case of job insertion
    request_data = request.get_json()
    inserts, *_ = [
        changeset["inserts"] for changeset in request_data if "inserts" in changeset
    ]
    scheduled_tasks = [
        insert["subject"]["value"]
        for insert in inserts
        if insert["predicate"]["value"] == "http://www.w3.org/ns/adms#status"
        and insert["object"]["value"] == TASK_STATUSES["SCHEDULED"]
    ]
    if not scheduled_tasks:
        return jsonify({"message": "delta didn't contain download jobs, ignoring"})

    for uri in scheduled_tasks:
        try:
            task = load_task(uri)
            if (task["operation"] == OPERATIONS["COLLECTING"]):
                update_task_status(task["uri"], TASK_STATUSES["BUSY"])
                collection = get_harvest_collection_for_task(task)
                rdo = get_initial_remote_data_object(collection)
                run_spider(LBLODSpider, start_urls=[rdo["url"]], collection = collection, task = uri, store_overview_pages = STORE_OVERVIEW_PAGES)
        except TaskNotFoundException:
          logger.debug(f"no task found for {uri}")
    return jsonify({"message": "thanks for all the fish!"})
