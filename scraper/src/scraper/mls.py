import json

import geocoder
import requests

MAP_URL = 'https://www.realtor.ca/Residential/Map.aspx'
API_URL = 'https://api.realtor.ca'

PROPERTY_SEARCH_TYPE = {
    0: "No Preference",
    1: "Residential",
    2: "Recreational",
    3: "Condo/Strata",
    4: "Agriculture",
    5: "Parking",
    6: "Vacant Land",
    8: "Multi Family",
}


def get_city(city_name: str = "Calgary, Canada") -> geocoder.api.OsmQuery:
    """Geocoded location for search.
    
    Parameters
    ----------
    city_name: str, default "Calgary, Canada"
        The city to query
    
    Returns
    -------
    geocoder.api.OsmQuery
    """
    return geocoder.osm(city_name)

g = get_city()
max_results=10
payload = {
    "CultureId": "1",
    "ApplicationId": "37",
    "RecordsPerPage": max_results,
    "MaximumResults": max_results,
    "PropertySearchTypeId": 1,
    "PriceMin": 0,
    "PriceMax": 1_000_000,
    "LandSizeRange": "0-0",
    "TransactionTypeId": "2",
    "StoreyRange": "0-0",
    "BedRange": "0-0",
    "BathRange": "0-0",
    "LongitudeMin": g.west,
    "LongitudeMax": g.east,
    "LatitudeMin": g.south,
    "LatitudeMax": g.north,
    "SortOrder": "A",
    "SortBy": "1",
    "viewState": "m",
    "Longitude": g.lng,
    "Latitude": g.lat,
    "ZoomLevel": "8",
}

if __name__ == "__main__":
    uri = API_URL + "/Listing.svc/PropertySearch_Post"
    r = requests.post(uri, data=payload)
    result = r.json()
    print("hurray!")
