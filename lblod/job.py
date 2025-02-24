import os
import datetime
from string import Template

from escape_helpers import sparql_escape_uri, sparql_escape_datetime, sparql_escape_string, sparql_escape_int
from helpers import generate_uuid, logger
from sudo_query import auth_update_sudo, update_sudo, query_sudo

from constants import SCRAPE_JOB_TYPE, RESOURCE_BASE, DEFAULT_GRAPH, TASK_STATUSES

############################################################
# TODO: keep this generic and extract into packaged module later
############################################################

class TaskNotFoundException(Exception):
    "Raised when task is not found"
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
        ?subject a task:Task .
        ?subject dct:isPartOf ?job;
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
  SELECT DISTINCT ?id ?job ?created ?modified ?status ?index ?operation ?error WHERE {
      GRAPH $graph {
        $subject a task:Task .
        $subject dct:isPartOf ?job;
                      mu:uuid ?id;
                      dct:created ?created;
                      dct:modified ?modified;
                      adms:status ?status;
                      task:index ?index;
                      task:operation ?operation.
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
        status = item['status']['value']
        index = item['index']['value']
        operation = item['operation']['value']
        error = item.get('error', {}).get('value', None)
        return {
            'id': id,
            'job': job,
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
