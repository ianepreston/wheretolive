"""Daily runner to get MLS data and ingest it."""
from wheretolive.mls.ingest import update_mls_postgis
from wheretolive.mls.parse import parse_scrapes
from wheretolive.mls.scrape import scrape_all

scrape_all()
parse_scrapes()
update_mls_postgis()
