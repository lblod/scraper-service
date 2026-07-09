from string import Template

from escape_helpers import sparql_escape_uri, sparql_escape_string, sparql_escape_datetime, sparql_escape_int
from helpers import logger, generate_uuid
from sudo_query import update_sudo, query_sudo
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
    url = re.sub(";jsessionid=[a-zA-Z;0-9]*", "", url)
    return re.sub(r'/\(S\([^)]+\)\)', '', url) # e.g : ranst https://ranst.meetingburger.net/(S(qp4fgo00jjm2islntouxtevs))/cbs/5272f4f2-4c69-45b1-8d59-3a314680c30f/besluitenlijs

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
    if not input_list:
        return input_list
    num_elements_to_remove = max(1, int(len(input_list) * 0.1))
    elements_to_remove = random.sample(input_list, num_elements_to_remove)
    return [item for item in input_list if item not in elements_to_remove]

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

def get_collected_data_objects(collection_uri):
    """Return the URIs of all collected remote data objects in a collection.

    Uses keyset pagination (advancing a FILTER boundary) rather than OFFSET, which
    runs into Virtuoso's sorted-top-rows limit once the offset grows large.
    """
    uris = []
    page_size = 5000
    last = ""
    query_template = Template("""
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    dct: <http://purl.org/dc/terms/>
    SELECT DISTINCT ?rdo WHERE {
      GRAPH $graph {
        $collection dct:hasPart ?rdo.
        ?rdo adms:status $status_collected.
        FILTER(STR(?rdo) > $last)
      }
    } ORDER BY ?rdo LIMIT $limit
    """)
    while True:
        query_s = query_template.substitute(
            graph = sparql_escape_uri(DEFAULT_GRAPH),
            status_collected = sparql_escape_uri(FILE_STATUSES['COLLECTED']),
            collection = sparql_escape_uri(collection_uri),
            last = sparql_escape_string(last),
            limit = page_size
        )
        results = query_sudo(query_s)
        bindings = results["results"]["bindings"]
        for b in bindings:
            uris.append(b["rdo"]["value"])
        if len(bindings) < page_size:
            break
        last = uris[-1]
    return uris

def copy_files_to_results_container(collection_uri, results_container):
    # Read the data objects first and link them with plain `INSERT DATA` batches;
    # a combined `INSERT ... WHERE { SELECT ... ORDER BY ... LIMIT ... OFFSET }` runs
    # into Virtuoso's sorted-top-rows limit on large collections.
    rdos = get_collected_data_objects(collection_uri)
    batch_size = 1000
    query_template = Template("""
    PREFIX    task: <http://redpencil.data.gift/vocabularies/tasks/>
    INSERT DATA {
      GRAPH $graph { $result_container task:hasFile $rdos. }
    }
    """)
    for i in range(0, len(rdos), batch_size):
        batch = rdos[i:i + batch_size]
        rdos_str = ", ".join(sparql_escape_uri(rdo) for rdo in batch)
        query_s = query_template.substitute(
            graph = sparql_escape_uri(DEFAULT_GRAPH),
            result_container = sparql_escape_uri(results_container),
            rdos = rdos_str
        )
        update_sudo(query_s)

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
    return uri

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

    SELECT DISTINCT ?dataObject ?url ?uuid ?status
    WHERE {
      GRAPH $graph {
        $collection dct:hasPart ?dataObject.
        ?dataObject a nfo:RemoteDataObject;
             mu:uuid ?uuid;
             nie:url ?url.
      }
    }
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


def get_results_container_for_task(task_uri, graph = DEFAULT_GRAPH):
    query_template = Template("""
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    SELECT ?container
    WHERE {
      GRAPH $graph {
        $task task:resultsContainer ?container.
      }
    }
    """)
    query_s = query_template.substitute(
        graph = sparql_escape_uri(graph),
        task = sparql_escape_uri(task_uri)
    )
    results = query_sudo(query_s)
    bindings = results["results"]["bindings"]
    if len(bindings) == 0:
        return None
    else:
        return bindings[0]["container"]["value"]


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

def store_report_metadata(physical_file_path, physical_file_name, results_container, size):
        physical_resource_uri = physical_file_path.replace("/share/", "share://")
        physical_resource_uuid = str(uuid.uuid4())
        virtual_resource_uuid = str(uuid.uuid4())
        virtual_resource_uri = f"http://data.lblod.info/files/{virtual_resource_uuid}"
        file_created = datetime.datetime.now()
        update_sudo(f"""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dbpedia: <http://dbpedia.org/ontology/>
PREFIX ndo: <http://oscaf.sourceforge.net/ndo.html#>
PREFIX    adms: <http://www.w3.org/ns/adms#>
        INSERT DATA {{
          GRAPH {sparql_escape_uri(DEFAULT_GRAPH)} {{
            {sparql_escape_uri(results_container)} <http://redpencil.data.gift/vocabularies/tasks/hasFile> {sparql_escape_uri(virtual_resource_uri)}.
            {sparql_escape_uri(virtual_resource_uri)} a nfo:FileDataObject ;
            mu:uuid {sparql_escape_string(virtual_resource_uuid)} ;
            nfo:fileName {sparql_escape_string(physical_file_name)}  ;
            dct:format "application/json" ;
            dct:creator <http://lblod.data.gift/services/lblod-scraper>;
            dct:created {sparql_escape_datetime(file_created)} ;
            nfo:fileSize {sparql_escape_int(size)} ;
            dbpedia:fileExtension "json" .

           {sparql_escape_uri(physical_resource_uri)} a nfo:FileDataObject ;
            mu:uuid {sparql_escape_string(physical_resource_uuid)} ;
            nfo:fileName {sparql_escape_string(physical_file_name)}  ;
            dct:format "application/json" ;
            dct:created {sparql_escape_datetime(file_created)} ;
            dct:creator <http://lblod.data.gift/services/lblod-scraper>;
            nfo:fileSize {sparql_escape_int(size)} ;
            dbpedia:fileExtension "json" ;
            nie:dataSource {sparql_escape_uri(virtual_resource_uri)}.
          }}
        }}

        """)
