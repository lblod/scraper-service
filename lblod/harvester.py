from string import Template

from escape_helpers import sparql_escape_uri, sparql_escape_string, sparql_escape_datetime
from helpers import logger, generate_uuid
from sudo_query import auth_update_sudo, update_sudo, query_sudo
import uuid
import datetime
import re
from urllib.parse import urldefrag
import random


from constants import DEFAULT_GRAPH, RESOURCE_BASE, FILE_STATUSES

def ensure_remote_data_object(collection, url):
    rdo = get_remote_data_object(collection, url)
    if rdo:
        return rdo
    else:
        return create_remote_data_object(collection, url)

def clean_url(url):
    """
    Workaround to avoid extracting the same url multiple times because a `jsessionid`
    is set in the url. This is only relevant for urls using a Java backend.
    todo check in the future if that's still the case, otherwise this could be completely removed.
    Not that we also cleanup the hash, e.g http://foo.com#blabla, this shouldn't affect
    extraction. We keep the other query parameters that are necessary for extraction.
    """
    url = urldefrag(url.strip()).url # remove eventual hash
    return re.sub(";jsessionid=[a-zA-Z;0-9]*", "", url)

def create_remote_data_object(collection, url):
    query_template = Template("""
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX    nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    PREFIX    dct: <http://purl.org/dc/terms/>
    PREFIX    nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX    nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>

    INSERT DATA {
      GRAPH $graph {
        $collection dct:hasPart $uri.
        $uri a nfo:RemoteDataObject .
        $uri mu:uuid $uuid;
             nie:url $url;
             dct:created $created;
             dct:creator <http://lblod.data.gift/services/scraper>;
             dct:modified $modified;
             adms:status $status.
      }
    }
""")
    uuid = generate_uuid()
    uri = RESOURCE_BASE.rstrip("/") + f"/remote-data-objects/{uuid}"
    created = datetime.datetime.now()
    q_string = query_template.substitute(
        graph = sparql_escape_uri(DEFAULT_GRAPH),
        uri = sparql_escape_uri(uri),
        uuid = sparql_escape_string(uuid),
        url = sparql_escape_uri(clean_url(url)),
        status = sparql_escape_uri(FILE_STATUSES['READY']),
        created = sparql_escape_datetime(created),
        modified = sparql_escape_datetime(created),
        collection = sparql_escape_uri(collection)

    )
    update_sudo(q_string)
    return {
        'uuid': uuid,
        'url': url,
        'uri': uri,
        'status': FILE_STATUSES['READY']
    }

def get_previous_succesfull_jobs(task_uri, max_age_in_days = 30):
    query_t = Template("""
    PREFIX cogs: <http://vocab.deri.ie/cogs#>
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    dct: <http://purl.org/dc/terms/>
    SELECT ?olderJob WHERE {
      GRAPH $graph {
         $task dct:isPartOf ?job.
         ?job dct:creator ?scheduledJob.
         ?scheduledJob a <http://vocab.deri.ie/cogs#ScheduledJob>.
         ?olderJob dct:creator ?scheduledJob.
         ?olderJob dct:modified ?modified.
         ?olderJob adms:status <http://redpencil.data.gift/id/concept/JobStatus/success>.
         FILTER (?modified > NOW() - $intervalInSeconds)
      }
    }
    """)
    query_s = query_t.substitute(
        graph = sparql_escape_uri(DEFAULT_GRAPH),
        task = sparql_escape_uri(task_uri),
        intervalInSeconds = max_age_in_days * 86400
    )
    results = query_sudo(query_s)
    bindings = results["results"]["bindings"]
    jobs = map(lambda b: b["olderJob"]["value"], bindings)
    return jobs

def count_previous_urls(jobs):
    query_t = Template("""
        PREFIX tasks: <http://redpencil.data.gift/vocabularies/tasks/>
        PREFIX    dct: <http://purl.org/dc/terms/>
        PREFIX    nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
        SELECT (COUNT(DISTINCT(?url)) as ?count) WHERE {
          GRAPH $graph {
            VALUES ?job {
              $jobs
            }
            ?task dct:isPartOf ?job;
                  tasks:operation <http://lblod.data.gift/id/jobs/concept/TaskOperation/collecting>;
                  tasks:resultsContainer/tasks:hasFile ?file.
            ?file nie:url ?url;
                  dct:type ?type.
            FILTER(?type != <http://schema.org/WebPage>)
        }
        }
        """)

    query_s = query_t.substitute(
        graph = sparql_escape_uri(DEFAULT_GRAPH),
        jobs = "\n".join(map(lambda j: sparql_escape_uri(j), jobs)),
    )
    results = query_sudo(query_s)
    bindings = results["results"]["bindings"]
    count = bindings[0]["count"]["value"]
    logger.info(f"found {count} previously harvested urls that should not be harvested again")
    return int(count)

def get_current_pages(collection_uri):
    nb_urls=count_number_of_files_in_collection(collection_uri)
    if nb_urls == 0:
        return []
    offset = 0
    limit = 5000
    urls = []
    while offset < nb_urls:
        query_template = Template("""
            PREFIX    adms: <http://www.w3.org/ns/adms#>
            PREFIX    dct: <http://purl.org/dc/terms/>
            PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
            SELECT ?url WHERE {
                SELECT DISTINCT ?url WHERE {
                    GRAPH $graph {
                        $collection dct:hasPart ?rdo.
                        ?rdo adms:status $status_collected; nie:url ?url.
                    }
                } ORDER BY ?url
            } LIMIT $limit OFFSET $offset
            """)
        query_s = query_template.substitute(
            graph = sparql_escape_uri(DEFAULT_GRAPH),
            status_collected = sparql_escape_uri(FILE_STATUSES['COLLECTED']),
            collection = sparql_escape_uri(collection_uri),
            limit = limit,
            offset = offset
        )
        results = query_sudo(query_s)
        bindings = results["results"]["bindings"]
        for b in bindings:
            urls.append(b["url"]["value"])
        offset = offset + limit

    return urls

def get_previous_pages(task_uri):
    jobs = list(get_previous_succesfull_jobs(task_uri))
    if len(jobs) > 0:
        nb_urls= count_previous_urls(jobs)
        offset = 0
        urls = []
        while offset < nb_urls:
            query_t = Template("""
            PREFIX tasks: <http://redpencil.data.gift/vocabularies/tasks/>
            PREFIX    dct: <http://purl.org/dc/terms/>
            PREFIX    nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
            SELECT ?url WHERE {
              SELECT DISTINCT ?url {
                GRAPH $graph {
                  VALUES ?job {
                    $jobs
                  }
                  ?task dct:isPartOf ?job;
                  tasks:operation <http://lblod.data.gift/id/jobs/concept/TaskOperation/collecting>;
                  tasks:resultsContainer/tasks:hasFile ?file.
                  ?file nie:url ?url;
                  dct:type ?type.
                  FILTER(?type != <http://schema.org/WebPage>)
               }
            } ORDER BY ?url
        } LIMIT 5000 OFFSET $offset
        """)

            query_s = query_t.substitute(
                graph = sparql_escape_uri(DEFAULT_GRAPH),
                jobs = "\n".join(map(lambda j: sparql_escape_uri(j), jobs)),
                offset = offset
            )
            results = query_sudo(query_s)
            bindings = results["results"]["bindings"]
            for b in bindings:
                urls.append(b["url"]["value"])
            offset = offset + 5000
        return urls
    return []

def remove_random_10_percent_of_list(input_list):
    num_elements_to_remove = int(len(input_list) * 0.1)
    elements_to_remove = random.sample(input_list, num_elements_to_remove)
    updated_list = [item for item in input_list if item not in elements_to_remove]
    return updated_list

def count_number_of_files_in_collection(collection_uri):
        query_template = Template("""
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    dct: <http://purl.org/dc/terms/>
    SELECT (COUNT(?rdo) as ?numberOfFile)
    WHERE {
      GRAPH $graph {
        $collection dct:hasPart ?rdo.
        ?rdo adms:status $status_collected.
      }
    }
    """)
        query_string = query_template.substitute(
            collection = sparql_escape_uri(collection_uri),
            graph = sparql_escape_uri(DEFAULT_GRAPH),
            status_collected = sparql_escape_uri(FILE_STATUSES['COLLECTED']),
        )
        results = query_sudo(query_string)
        bindings = results["results"]["bindings"]
        if len(bindings) == 1:
            return int(bindings[0]["numberOfFile"]["value"])
        else:
          return 0

def copy_files_to_results_container(collection_uri, results_container):
    number_of_files = count_number_of_files_in_collection(collection_uri)
    if number_of_files > 0:
        offset = 0
        query_template = Template("""
        PREFIX    adms: <http://www.w3.org/ns/adms#>
        PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
        PREFIX    nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
        PREFIX    dct: <http://purl.org/dc/terms/>
        PREFIX    task: <http://redpencil.data.gift/vocabularies/tasks/>
        PREFIX    nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
        INSERT {
           GRAPH $graph { $result_container task:hasFile ?rdo.  }
        }
       WHERE {
          SELECT ?rdo WHERE {
            GRAPH $graph {
              $collection dct:hasPart ?rdo.
              ?rdo adms:status $status_collected.
            }
         } ORDER BY ?rdo LIMIT 5000 offset $offset
      } 
    """)
        while offset < number_of_files:
            query_s = query_template.substitute(
                graph = sparql_escape_uri(DEFAULT_GRAPH),
                result_container = sparql_escape_uri(results_container),
                status_collected = sparql_escape_uri(FILE_STATUSES['COLLECTED']),
                collection = sparql_escape_uri(collection_uri),
                offset = offset
            )
            update_sudo(query_s)
            offset = offset + 5000

def create_results_container(task_uri, collection_uri):
    create_container_query = Template("""
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX    nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    PREFIX    dct: <http://purl.org/dc/terms/>
    PREFIX    task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX    nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    INSERT DATA {
      GRAPH $graph {
        $task task:resultsContainer $result_container.
        $result_container a nfo:DataContainer;
                          mu:uuid $uuid.
      }
    }
    """)
    uuid = generate_uuid()
    uri = RESOURCE_BASE.rstrip("/") + f"/data-containers/{uuid}"
    query_s = create_container_query.substitute(
        graph = sparql_escape_uri(DEFAULT_GRAPH),
        result_container = sparql_escape_uri(uri),
        uuid = sparql_escape_string(uuid),
        task = sparql_escape_uri(task_uri)
    )
    update_sudo(query_s)
    copy_files_to_results_container(collection_uri, uri)

"""
get remote data object in a harvesting collection that matches remote url. Expects 1 RDO
"""
def get_remote_data_object(collection_uri, remote_url):
    query_template = Template("""
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX    nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    PREFIX    dct: <http://purl.org/dc/terms/>
    PREFIX    nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX    nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>

    SELECT DISTINCT ?dataObject ?uuid ?status
    WHERE {
      GRAPH $graph {
        $collection dct:hasPart ?dataObject.
        ?dataObject a nfo:RemoteDataObject;
             mu:uuid ?uuid;
             nie:url $url.
        OPTIONAL { ?dataObject adms:status ?status.}
      }
    }
""")
    query_string = query_template.substitute(
        graph = sparql_escape_uri(DEFAULT_GRAPH),
        collection = sparql_escape_uri(collection_uri),
        url = sparql_escape_uri(clean_url(remote_url))
    )
    results = query_sudo(query_string)
    bindings = results["results"]["bindings"]
    if len(bindings) == 1:
        item = bindings[0]
        uuid = item['uuid']['value']
        uri = item['dataObject']['value']
        status = item.get('status', {}).get('value', None)
        return {
            'uuid': uuid,
            'url': remote_url,
            'uri': uri,
            'status': status
        }
    elif len(bindings) == 0:
        return None
    else:
        raise Exception(f"Unexpected result {results}")


"""
get remote data object in a harvesting collection. Expects 1 RDO
"""
def get_initial_remote_data_object(collection_uri):
    query_template = Template("""
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX    nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    PREFIX    dct: <http://purl.org/dc/terms/>
    PREFIX    nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX    nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>
    SELECT  ?dataObject ?url ?uuid  WHERE {
        SELECT DISTINCT ?dataObject ?url ?uuid  WHERE {
            GRAPH $graph  {
                $collection  dct:hasPart ?dataObject.
                ?dataObject a nfo:RemoteDataObject;
                    mu:uuid ?uuid; dct:created ?created;
                    nie:url ?url.
            }
        } ORDER BY DESC(?created)
    } LIMIT 1 OFFSET 0
""")
    query_string = query_template.substitute(
        graph = sparql_escape_uri(DEFAULT_GRAPH),
        collection = sparql_escape_uri(collection_uri)
    )
    results = query_sudo(query_string)
    bindings = results["results"]["bindings"]
    if len(bindings) == 1:
        item = bindings[0]
        uuid = item['uuid']['value']
        url = item['url']['value']
        uri = item['dataObject']['value']
        return {
            'uuid': uuid,
            'url': url,
            'uri': uri
        }
    else:
        raise Exception(f"Unexpected result {results}")

def get_harvest_collection_for_task(task, graph = DEFAULT_GRAPH):
    task_uri = task["uri"]
    query_template = Template("""
    PREFIX tasks: <http://redpencil.data.gift/vocabularies/tasks/>
    SELECT ?collection
    WHERE {
      GRAPH $graph  {
        $task tasks:inputContainer ?inputContainer.
        ?inputContainer tasks:hasHarvestingCollection ?collection.
      }
    }
    """)
    query_s = query_template.substitute(
        graph = sparql_escape_uri(graph),
        task = sparql_escape_uri(task_uri)
    )
    results = query_sudo(query_s)
    bindings = results["results"]["bindings"]
    if (len(bindings) == 1):
        return bindings[0]["collection"]["value"]
    else:
        raise Exception(f"Unexpected result {results}")


def collection_has_collected_files(collection):
    query_template = Template("""
    PREFIX    dct: <http://purl.org/dc/terms/>
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    ASK { GRAPH $graph {
      $collection dct:hasPart ?remoteDataObject.
      ?remoteDataObject adms:status $status
    }}
    """)
    query_s = query_template.substitute(
        graph = sparql_escape_uri(DEFAULT_GRAPH),
        collection = sparql_escape_uri(collection),
        status = sparql_escape_uri(FILE_STATUSES["COLLECTED"])
    )
    result = query_sudo(query_s)
    return result["boolean"]
