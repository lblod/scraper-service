# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
- fixed tracking of failed_urls for scrape report
- moved visited urls to a set for better performance
- only follow links within the same domain
- fixed test endpoint /scrape

## [1.5.0] - 2025-09-29
### Fixed
- typo in cleanup query, hanging busy jobs should now be set to failed on startup
- properly recognize pages with only a behandeling

### Changed
- added timeout of 300s to SPARQL queries to prevent unexpected hangups

### Added
- made `INTERESTING_PROPERTIES` configurable 

## [1.4.1] - 2025-09-04
### Fixed
- fixed scrape report storage to use job-specific subfolder instead of root storage path

## [1.4.0] - 2025-09-01
- fixed a cycle in crawling (related to relative urls and sessionids used in paths)
- changed storage path to use job id instead of timestamp

## [1.3.1] - 2025-02-20
- make sure to revisit at least one page

## [1.3.0] - 2025-01-29
- added a scrape report to the scraper output for better visibility
- added a flag to only store pages containing Notulen, Agenda, Besluitenlijst, Uittreksel, Besluit or BehandelingVanAgendapunt
- updated clean url function to filter out dynamic segment of meetingburger URLs
- fail busy and scheduled tasks on startup

## [1.2.0] - 2024-12-17
### Changed
- set up close spider and configure max items to 50.000 to stop scraping in a reasonable time

## [1.1.1] - 2023-12-15
### Changed
- improve detection of publications vs overview pages

## [1.1.0] - 2023-12-13

### Added
- support for incremental harvests
