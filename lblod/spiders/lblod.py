from scrapy import Spider
from scrapy.loader import ItemLoader
from scrapy.http.response.text import TextResponse
from scrapy.exceptions import IgnoreRequest
from rdflib import Graph, Namespace
from helpers import logger

from lblod.items import Page
from lblod.harvester import ensure_remote_data_object

BESLUIT = Namespace("http://data.vlaanderen.be/ns/besluit#")
LBBESLUIT = Namespace("http://lblod.data.gift/vocabularies/besluit/")

def doc_type_from_type_ofs(type_ofs):
    # notulen, agenda, besluitenlijst uittreksel
    for type_of in type_ofs:
        type_of = type_of.get()
        if '8e791b27-7600-4577-b24e-c7c29e0eb773' in type_of:
            return 'https://data.vlaanderen.be/id/concept/BesluitDocumentType/8e791b27-7600-4577-b24e-c7c29e0eb773'
        elif '13fefad6-a9d6-4025-83b5-e4cbee3a8965' in type_of:
            return 'https://data.vlaanderen.be/id/concept/BesluitDocumentType/13fefad6-a9d6-4025-83b5-e4cbee3a8965'
        elif '3fa67785-ffdc-4b30-8880-2b99d97b4dee' in type_of:
            return 'https://data.vlaanderen.be/id/concept/BesluitDocumentType/3fa67785-ffdc-4b30-8880-2b99d97b4dee'
        elif '9d5bfaca-bbf2-49dd-a830-769f91a6377b' in type_of:
            return 'https://data.vlaanderen.be/id/concept/BesluitDocumentType/9d5bfaca-bbf2-49dd-a830-769f91a6377b'
        else:
            return None

class LBLODSpider(Spider):
    name = "LBLODSpider"
    def parse(self, response):
        if not isinstance(response, TextResponse):
            raise IgnoreRequest("ignoring non text response")

        # store page itself
        rdo = ensure_remote_data_object(self.collection, response.url)
        type_ofs = response.xpath('//@typeof')
        doc_type = doc_type_from_type_ofs(type_ofs)
        page = ItemLoader(item=Page(), response=response)
        page.add_value("url", response.url)
        page.add_value("contents", response.text)
        page.add_value("rdo", rdo)
        page.add_value("doc_type", doc_type)
        yield page.load_item()
        interesting_properties = [
            'heeftNotulen',
            'heeftAgenda',
            'heeftBesluitenlijst',
            'heeftUittreksel',
            'linkToPublication'
        ]
        for element in response.xpath('//a[@href and @property]'):
            href = element.xpath('@href').get()
            property_value = element.xpath('@property').get()
            if any(value in property_value for value in interesting_properties):
                if not href.endswith('.pdf'):
                    url = response.urljoin(href)
                    ensure_remote_data_object(self.collection, url)
                    yield response.follow(url)
                else:
                    logger.info(f'ignoring pdf link {href}')
