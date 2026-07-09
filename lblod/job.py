import os
import datetime
from string import Template

from escape_helpers import sparql_escape_uri, sparql_escape_datetime, sparql_escape_string, sparql_escape_int
from helpers import generate_uuid, logger
from sudo_query import update_sudo, query_sudo

from constants import SCRAPE_JOB_TYPE, RESOURCE_BASE, DEFAULT_GRAPH, TASK_STATUSES, OPERATIONS

############################################################
# TODO: keep this generic and extract into packaged module later
############################################################

class TaskNotFoundException(Exception):
    "Raised when task is not found"
    pass


class TaskNotFailedException(Exception):
    "Raised when a task is expected to be in a failed state but isn't"
    pass


class NoCollectedFilesException(Exception):
    "Raised when a task's harvesting collection has no collected files"
    pass


def fail_busy_and_scheduled_tasks():
    logger.info("Startup: failing busy tasks if there are any")
    update_sudo(f"""
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX adms: <http://www.w3.org/ns/adms#>
  PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
  DELETE {{
    GRAPH {sparql_escape_uri(DEFAULT_GRAPH)} {{
        ?task  adms:status ?status
    }}
  }}
  INSERT {{
    GRAPH {sparql_escape_uri(DEFAULT_GRAPH)} {{
      ?task adms:status {sparql_escape_uri(TASK_STATUSES["FAILED"])}
    }}
  }}
  WHERE  {{
    GRAPH {sparql_escape_uri(DEFAULT_GRAPH)} {{
        ?task a task:Task .
        ?task dct:isPartOf ?job;
        task:operation <http://lblod.data.gift/id/jobs/concept/TaskOperation/collecting>;
        adms:status ?status.
    VALUES ?status {{
    {sparql_escape_uri(TASK_STATUSES["BUSY"])}
    {sparql_escape_uri(TASK_STATUSES["SCHEDULED"])}
    }}
    }}
  }}

    """)

def load_task(subject, graph = DEFAULT_GRAPH):
    query_template = Template("""
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX adms: <http://www.w3.org/ns/adms#>
  PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
  SELECT DISTINCT ?id ?job ?jobId ?created ?modified ?status ?index ?operation ?error WHERE {
      GRAPH $graph {
        $subject a task:Task .
        $subject dct:isPartOf ?job;
                      mu:uuid ?id;
                      dct:created ?created;
                      dct:modified ?modified;
                      adms:status ?status;
                      task:index ?index;
                      task:operation ?operation.
        ?job mu:uuid ?jobId.
        OPTIONAL { $subject task:error ?error. }
      }
    }

    """)

    query_string = query_template.substitute(
        graph = sparql_escape_uri(graph),
        subject = sparql_escape_uri(subject)
    )

    results = query_sudo(query_string)
    bindings = results["results"]["bindings"]
    if len(bindings) == 1:
        item = bindings[0]
        id = item['id']['value']
        job = item['job']['value']
        job_id = item['jobId']['value']
        status = item['status']['value']
        index = item['index']['value']
        operation = item['operation']['value']
        error = item.get('error', {}).get('value', None)
        return {
            'id': id,
            'job': job,
            'job_id' : job_id,
            'status': status,
            'operation': operation,
            'index': index,
            'error': error,
            'uri': subject
        }
    elif len(bindings) == 0:
        raise TaskNotFoundException()
    else:
        raise Exception(f"Unexpected result loading task: {results}")

def update_task_status (task, status, graph=DEFAULT_GRAPH):
    query_template = Template("""
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    DELETE {
      GRAPH $graph {
        $subject adms:status ?status .
        $subject dct:modified ?modified.
      }
    }
    INSERT {
      GRAPH $graph {
        $subject adms:status $status.
        $subject dct:modified $modified.
      }
    }
    WHERE {
      GRAPH $graph {
        $subject a task:Task.
        $subject adms:status ?status .
        OPTIONAL { $subject dct:modified ?modified. }
      }
    }
    """)
    time = datetime.datetime.now()
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        subject=sparql_escape_uri(task),
        modified=sparql_escape_datetime(datetime.datetime.now()),
        status=sparql_escape_uri(status)
    )
    update_sudo(query_string)


def prepare_failed_task_conversion(task_uri):
    """Validate that a failed collecting task can be converted to success and
    ensure it has a results container.

    Runs the cheap checks (task exists, is failed, is a collecting task, and its
    harvesting collection has collected files) so the HTTP layer can report
    errors synchronously. Returns a dict describing the task, its harvesting
    collection and its (reused or freshly created) results container. The heavy
    file linking is left to :func:`complete_failed_task_conversion`.
    """
    from .harvester import (
        get_harvest_collection_for_task,
        collection_has_collected_files,
        get_results_container_for_task,
        create_results_container,
    )

    task = load_task(task_uri)  # raises TaskNotFoundException

    if task["status"] != TASK_STATUSES["FAILED"]:
        raise TaskNotFailedException(
            f"task {task_uri} is not in a failed state (status: {task['status']})"
        )
    if task["operation"] != OPERATIONS["COLLECTING"]:
        raise Exception(
            f"task {task_uri} is not a collecting task (operation: {task['operation']})"
        )

    collection = get_harvest_collection_for_task(task)
    if not collection_has_collected_files(collection):
        raise NoCollectedFilesException(
            f"harvesting collection for task {task_uri} has no collected files to convert"
        )

    results_container = get_results_container_for_task(task_uri)
    if results_container is None:
        results_container = create_results_container(task_uri, collection)

    return {
        "task": task_uri,
        "job": task["job"],
        "job_id": task["job_id"],
        "collection": collection,
        "results_container": results_container,
    }


def complete_failed_task_conversion(task_uri, collection, results_container):
    """Link the collection's harvested files to the results container and mark
    the task as success.

    This is the heavy part of the conversion (potentially tens of thousands of
    files, in batched SPARQL updates with retries) and is meant to run in a
    background process, off the request thread. The parent job is intentionally
    left untouched: this service only manages task status and relies on the
    job-controller to flip the job via the task-status delta.

    On failure the task is left in its failed state, so the conversion can be
    safely retried (file linking is idempotent).
    """
    from .harvester import (
        copy_files_to_results_container,
        count_number_of_files_in_collection,
    )

    try:
        copy_files_to_results_container(collection, results_container)
        files_linked = count_number_of_files_in_collection(collection)
        update_task_status(task_uri, TASK_STATUSES["SUCCESS"])
        logger.info(
            f"converted failed task {task_uri} to success ({files_linked} files linked)"
        )
    except Exception as e:
        logger.error(f"failed to convert task {task_uri} to success, leaving it failed")
        logger.error(e)
        raise
