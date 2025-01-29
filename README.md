# LBLOD Scraper service

Service to scrape LBLOD publication from a provided url. The URL can either be an index (listing) page or an actual LBLOD publication.
The service will start downloading at the provided url and follow any links that have the following annotiations:
 - lblodBesluit:linkToPublication
 - besluit:heeftAgenda
 - besluit:heeftBesluitenlijst
 - besluit:heeftUittreksel
 - besluit:heeftNotulen

 For more information see https://lblod.github.io/pages-vendors/#/docs/publication-annotations


## setup

Add the following to your docker-compose.yml
```yaml
services:
  scraper:
    image: lblod/scraper
    links:
      - database:database
    volumes:
      - ./data/files:/share
    environment:
      DEFAULT_GRAPH: "http://mu.semte.ch/graphs/public"
```

And add a delta rule in rules.js
```json
  {
    match: {
      predicate: {
        type: 'uri',
        value: 'http://www.w3.org/ns/adms#status',
      },
      object: {
        type: 'uri',
        value: 'http://redpencil.data.gift/id/concept/JobStatus/scheduled',
      },
    },
    callback: {
      method: 'POST',
      url: 'http://scraper/delta',
    },
    options: {
      resourceFormat: 'v0.0.1',
      gracePeriod: 1000,
      ignoreFromSelf: true,
      optOutMuScopeIds: ['http://redpencil.data.gift/id/concept/muScope/deltas/initialSync'],
    },
  },


```
NOTE: This service can replace the download-url-service && harvest-collector-service and as such has similar configuration. It currently doesn't support authenticated resources.


## Reference
### Configuration
The following environment variales can be configured:
* `DEFAULT_GRAPH`: graph to write the download event and file to
* `INCREMENTAL_RETRIEVAL`: (default: `false`) for scheduled jobs check result of previous succesfull executions and don't refetch all documents on each execution. 
* `STORE_ALL_PAGES`: (default: `true`) when disabled (`false`) will only store pages containing Notulen, Agenda, Besluitenlijst, Uittreksel, Besluit or BehandelingVanAgendapunt. (using the same heuristic as incremantal retrieval).

### Model
The service is triggered by updates of resources of type `nfo:RemoteDataObject` of which the status is updated to `http://lblod.data.gift/file-download-statuses/ready-to-be-cached`. It will download the associated URL (`nie:url`) as file.

The download service will create a download event (`ndo:DownloadEvent`) and a local file data object (`nfo:LocalFileDataObject`) on succesfull download of the remote resource. The properties of these resources are specified below.

The model of this service is compliant with the model of the [file-service](http://github.com/mu-semtech/file-service). Hence, the cached files can be downloaded using this service.

## Testing
the service exposes an endpoint `/scrape` that you can `POST` to. the provided URL (query param `url`) is used as the start_url to scrape from.

## Things worth mentioning

### RDFa support
The scraper doesn't actually parse rdfa but uses heuristics to find relevant links. This is a lot faster than parsing the RDFa
Testing with about 100 "gemeenten" indicates this works well enough for now, but we might want to parse RDFa if required.

### Incremental retrieval
This relies on making a distinction between overview pages and actual documents and currently heavily relies on the specified document types via the "typeof" attribute. If a matching document type is not found the page will be refetched every time
In addition 10% of previously fetched pages are refetched each run to avoid revisiting all pages if executions move outside of our interval (currently +- 30 days)
Currently we only check previous executions of a scheduled job, so a manually triggered job will always index everything. If you remove and recreate a scheduled job this will also trigger a reindex of everyting.

### Maximum amount of items
The scraper is configured (in settings.py) to stop scraping after 50.000 pages (actual amount will be slightly larger). This should suffice for most use cases, if not set up incremental scraping so data can be fetched in several runs
