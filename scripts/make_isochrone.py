"""Generate isochrones of travel time for key locations."""
import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

maps_key = os.environ["MAPS_KEY"]

isochrone_dir = Path("data/isochrone")

with open(isochrone_dir / "coordinates.json", "r") as f:
    coords = json.load(f)


test_coord = list(coords.values())[0]


base_url = "https://atlas.microsoft.com/route/range/json?"
# https://docs.microsoft.com/en-us/rest/api/maps/route/get-route-range#travelmode
travelmode = "pedestrian"
query = f"{test_coord[0]},{test_coord[1]}"
time_budget = f"{10 * 60}"

full_url = f"{base_url}subscription-key={maps_key}&api-version=1.0&query={query}&timeBudgetInSec={time_budget}&TravelMode={travelmode}"  # noqa: B950

bounds = requests.get(full_url).json()

print(bounds)
with open(isochrone_dir / "test_bounds.json", "w") as f:
    json.dump(bounds, f)
