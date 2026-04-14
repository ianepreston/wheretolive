"""Download daily rental.

Just saves them to the data folder. Separate logic will need to be used to provision
to a database or cloud data store.

"""
from wheretolive.rfaster.ingest import update_rfaster_gis
from wheretolive.rfaster.parse import parse_scrapes
from wheretolive.rfaster.scrape import scrape_all
from wheretolive.rfaster.upload_candidates import upload_candidate

scrape_all()
parse_scrapes()
update_rfaster_gis()
upload_candidate("ian")
