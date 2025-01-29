import datetime
import os
import uuid
from string import Template

from itemadapter import ItemAdapter
from constants import DEFAULT_GRAPH

from escape_helpers import sparql_escape_uri
from sudo_query import update_sudo
from helpers import logger
import gzip

from .file import construct_insert_file_query, STORAGE_PATH
from constants import DEFAULT_GRAPH, RESOURCE_BASE, TASK_STATUSES
from .job import update_task_status
from .harvester import collection_has_collected_files, create_results_container, get_previous_pages, remove_random_10_percent_of_list, copy_files_to_results_container, store_report_metadata
from .extendedjsonencoder import ExtendedJsonEncoder

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TimeoutError, TCPTimedOutError
import json

INCREMENTAL_RETRIEVAL = os.getenv("INCREMENTAL_RETRIEVAL") in ["yes", "on", "true", True, "1", 1]

class Pipeline:
    timestamp = datetime.datetime.now()
    failed_urls = []

    def __init__(self):
        self.storage_path = os.path.join(STORAGE_PATH, self.timestamp.isoformat())
        if not os.path.exists(self.storage_path):
            os.mkdir(self.storage_path)

    def open_spider(self, spider):
        if INCREMENTAL_RETRIEVAL:
            previous_collected_pages = get_previous_pages(spider.task)
            spider.previous_collected_pages = remove_random_10_percent_of_list(previous_collected_pages)
        else:
            spider.previous_collected_pages = []

    def errback_http(self, failure):
        url = failure.request.url
        retries = failure.request.meta.get("retry_times", 0)
        status_code = None

        if failure.check(HttpError):
            response = failure.value.response
            status_code = response.status
            logger.warning(f"HTTP {status_code} error on {url} (Retry {retries})")
        elif failure.check(DNSLookupError):
            logger.warning(f"DNS lookup failed for {url} (Retry {retries})")
        elif failure.check(TimeoutError, TCPTimedOutError):
            logger.warning(f"Timeout error on {url} (Retry {retries})")
        else:
            logger.warning(f"Unknown error on {url}")
        self.failed_urls.append(url)

    def close_spider(self, spider):
        try:
            results_container = create_results_container(spider.task, spider.collection)
            self.store_report(spider, results_container)
            if collection_has_collected_files(spider.collection):
                copy_files_to_results_container(spider.collection, results_container)
                update_task_status(spider.task, TASK_STATUSES["SUCCESS"])
            else:
                logger.error("spider closed without collecting files")
                update_task_status(spider.task, TASK_STATUSES["FAILED"])

        except Exception as e:
            logger.error(e)
            logger.error("failure while closing spider, attempting to set task to failed")
            update_task_status(spider.task, TASK_STATUSES["FAILED"])


    def store_report(self, spider, results_container):
        stats = spider.crawler.stats.get_stats()
        data = {
            "stats": stats,
            "failed_urls": self.failed_urls,
        }
        # assumes storage path is unique to the pipeline
        physical_file_name = "00-scrape-report.json"
        physical_file_path = os.path.join(self.storage_path, physical_file_name)
        with open(physical_file_path, "w") as f:
            json.dump(data, f, indent=2, cls=ExtendedJsonEncoder)
            f.flush()
            size = f.tell()
        store_report_metadata(physical_file_path, physical_file_name, results_container, size)

    def process_spider_exception(self, response, exception, spider):
        # Extract the relevant information from the failed response
        url = response.url
        status = response.status
        error_message = str(exception)
        logger.error(error_message)

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        contents = adapter.get("contents")
        remote_data_object = adapter.get("rdo")
        if isinstance(contents, (bytes, bytearray)):
            write_mode = "wb"
        elif isinstance(contents, str):
            write_mode = "w"
        else:
            # Can't write a file that isn't a (byte)string
            return item

        _uuid = str(uuid.uuid4())
        physical_file_name = f"{_uuid}.html.gz"
        physical_file_path = os.path.join(self.storage_path, physical_file_name)
        with gzip.open(physical_file_path, write_mode) as f:
            f.write(contents.encode())
        size = os.stat(physical_file_path).st_size
        file_created = datetime.datetime.now()
        adapter["uuid"] = _uuid
        adapter["size"] = size
        adapter["file_created"] = file_created
        adapter["extension"] = "html"
        adapter["format"] = "application/gzip"
        adapter["physical_file_name"] = physical_file_name
        adapter["physical_file_path"] = physical_file_path
        try:
            self.push_item_to_triplestore(adapter)
        except Exception as e:
            logger.error(f"Encountered exception while trying to write data to triplestore for item generated by scraping {adapter['url']}")
            update_task_status(spider.task, TASK_STATUSES["FAILED"])
            raise e from None

        return item

    def push_item_to_triplestore(self, item):
        virtual_resource_uuid = str(uuid.uuid4())
        virtual_resource_uri = f"http://data.lblod.info/files/{virtual_resource_uuid}"
        virtual_resource_name = f"{virtual_resource_uuid}.{item['extension']}"
        file = {
            "uri": virtual_resource_uri,
            "uuid": virtual_resource_uuid,
            "name": virtual_resource_name,
            "mimetype": item["format"],
            "created": item["file_created"],
            "modified": item["file_created"], # currently unused
            "size": item["size"],
            "extension": item["extension"],
            "remote_data_object": item["rdo"]["uri"],
            "doc_type": item["doc_type"]
        }
        physical_resource_uri = item["physical_file_path"].replace("/share/", "share://") # TODO: use file lib function to construct share uri
        physical_file = {
            "uuid": item["uuid"],
            "uri": physical_resource_uri,
            "name": item["physical_file_name"]
        }

        ins_file_q_string = construct_insert_file_query(file,
                                                        physical_file,
                                                        DEFAULT_GRAPH)
        update_sudo(ins_file_q_string)
