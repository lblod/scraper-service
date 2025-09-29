import datetime
import os
import time

from SPARQLWrapper import SPARQLWrapper, JSON
from helpers import logger
from constants import SPARQL_TIMEOUT

sparqlQuery = SPARQLWrapper(os.environ.get("MU_SPARQL_ENDPOINT"), returnFormat=JSON)
sparqlQuery.addCustomHttpHeader("mu-auth-sudo", "true")
sparqlQuery.setTimeout(SPARQL_TIMEOUT)

sparqlUpdate = SPARQLWrapper(os.environ.get("MU_SPARQL_UPDATEPOINT"), returnFormat=JSON)
sparqlUpdate.method = "POST"
sparqlUpdate.addCustomHttpHeader("mu-auth-sudo", "true")
sparqlUpdate.setTimeout(SPARQL_TIMEOUT)

authSparqlUpdate = SPARQLWrapper(os.environ.get("MU_AUTH_ENDPOINT"), returnFormat=JSON)
authSparqlUpdate.method = "POST"
authSparqlUpdate.addCustomHttpHeader("mu-auth-sudo", "true")
authSparqlUpdate.setTimeout(SPARQL_TIMEOUT)



def query_sudo(the_query):
    """Execute the given SPARQL query (select/ask/construct)on the triple store and returns the results
    in the given returnFormat (JSON by default)."""
    start = time.time()
    logger.debug(f"started query at {datetime.datetime.now()}")
    logger.debug("execute query: \n" + the_query)
    sparqlQuery.setQuery(the_query)
    logger.debug(f"query took {time.time() - start} seconds")
    return sparqlQuery.query().convert()


def update_sudo(the_query, attempt=0, max_retries=5):
    """Execute the given update SPARQL query on the triple store,
    if the given query is no update query, nothing happens."""
    sparqlUpdate.setQuery(the_query)
    if sparqlUpdate.isSparqlUpdateRequest():
        try:
            start = time.time()
            logger.debug(f"started query at {datetime.datetime.now()}")
            logger.debug("execute query: \n" + the_query)

            sparqlUpdate.query()

            logger.debug(f"query took {time.time() - start} seconds")
        except Exception as e:
            logger.warn(f"Executing query failed unexpectedly. Stacktrace:", e)
            if attempt <= max_retries:
                wait_time = 0.6 * attempt + 30
                logger.warn(f"Retrying after {wait_time} seconds [{attempt}/{max_retries}]")
                time.sleep(wait_time)

                update_sudo(the_query, attempt + 1, max_retries)
            else:
                logger.warn(f"Max attempts reached for query. Skipping.")
                raise
