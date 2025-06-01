import os
from scrapy import Spider
from scrapy.loader import ItemLoader
from scrapy.http.response.text import TextResponse
from scrapy.exceptions import IgnoreRequest
from rdflib import Graph, Namespace
from helpers import logger

from lblod.items import Page
from lblod.harvester import ensure_remote_data_object, clean_url

BESLUIT = Namespace("http://data.vlaanderen.be/ns/besluit#")
LBBESLUIT = Namespace("http://lblod.data.gift/vocabularies/besluit/")
GENERAL_PAGE_TYPE = 'http://schema.org/WebPage'
INTERESTING_PROPERTIES = [
    'heeftNotulen',
    'heeftAgenda',
    'heeftBesluitenlijst',
    'heeftUittreksel',
    'linkToPublication'
]
STORE_ALL_PAGES = os.getenv("STORE_ALL_PAGES") in ["yes", "on", "true", True, "1", 1]

def doc_type_from_type_ofs(type_ofs):
    # notulen, agenda, besluitenlijst uittreksel
    for type_of in type_ofs:
        if '8e791b27-7600-4577-b24e-c7c29e0eb773' in type_of:
            return 'https://data.vlaanderen.be/id/concept/BesluitDocumentType/8e791b27-7600-4577-b24e-c7c29e0eb773'
        elif '13fefad6-a9d6-4025-83b5-e4cbee3a8965' in type_of:
            return 'https://data.vlaanderen.be/id/concept/BesluitDocumentType/13fefad6-a9d6-4025-83b5-e4cbee3a8965'
        elif '3fa67785-ffdc-4b30-8880-2b99d97b4dee' in type_of:
            return 'https://data.vlaanderen.be/id/concept/BesluitDocumentType/3fa67785-ffdc-4b30-8880-2b99d97b4dee'
        elif '9d5bfaca-bbf2-49dd-a830-769f91a6377b' in type_of:
            return 'https://data.vlaanderen.be/id/concept/BesluitDocumentType/9d5bfaca-bbf2-49dd-a830-769f91a6377b'

    # If none of the UUID conditions are met, check for "Besluit" or "BehandelingOfAgendapunt" to detect non overview pages
    for type_of in type_ofs:
        if 'Besluit' in type_of or 'BehandelingOfAgendapunt' in type_of:
            return 'https://schema.org/ItemPage'
    # Else return general webpage type
    return GENERAL_PAGE_TYPE

class LBLODSpider(Spider):
    name = "LBLODSpider"
    def parse(self, response):
        if not isinstance(response, TextResponse):
            raise IgnoreRequest("ignoring non text response")

        # store page itself
        type_ofs = response.xpath('//@typeof').getall()
        doc_type = doc_type_from_type_ofs(type_ofs)
        if doc_type != GENERAL_PAGE_TYPE or STORE_ALL_PAGES:
            rdo = ensure_remote_data_object(self.collection, response.url)
            page = ItemLoader(item=Page(), response=response)
            page.add_value("url", response.url)
            page.add_value("contents", response.text)
            page.add_value("rdo", rdo)
            page.add_value("doc_type", doc_type)
            yield page.load_item()

        for element in response.xpath('//a[@href and @property]'):
            href = element.xpath('@href').get()
            property_value = element.xpath('@property').get()
            if any(value in property_value for value in INTERESTING_PROPERTIES):
                if not href.endswith('.pdf'):
                    url = clean_url(response.urljoin(href))
                    if not url in self.previous_collected_pages:
                        yield response.follow(url)
                    else:
                        logger.info(f"ignoring previously harvested url {url}")
                else:
                    logger.info(f'ignoring pdf link {href}')
