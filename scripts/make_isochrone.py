"""Generate isochrones of travel time for key locations."""
import datetime as dt
import json
import os
from pathlib import Path
from typing import Tuple
from urllib.parse import urlencode

import pytz
import requests
from dotenv import load_dotenv

load_dotenv()

maps_key = os.environ["MAPS_KEY"]


def azure_isochrone(
    coord: Tuple[int, int], minutes_budget: int, depart_at: str, weekday: bool
):
    """Get isochrone for drive times from Azure."""
    base_url = "https://atlas.microsoft.com/route/range/json?"
    # https://docs.microsoft.com/en-us/rest/api/maps/route/get-route-range#travelmode
    query = f"{coord[0]},{coord[1]}"
    time_budget = f"{minutes_budget * 60}"
    # We need a date for travel times, and it has to be slightly in the future
    # set it to next week and then adjust back if we need to make it a weekend or not
    today = dt.date.today() + dt.timedelta(days=7)
    today_is_weekend = today.isoweekday() in (6, 7)
    if today_is_weekend & weekday:
        today += dt.timedelta(days=8 - today.isoweekday())
    elif (not today_is_weekend) & (not weekday):
        today += dt.timedelta(days=6 - today.isoweekday())
    depart_time = dt.datetime.strptime(depart_at, "%I:%M%p").time()
    depart_datetime = dt.datetime.combine(today, depart_time)
    tz_info = pytz.timezone("America/Edmonton")
    tz_aware_dt = depart_datetime.astimezone(tz_info)
    base_utc = f"{tz_aware_dt:%z}"
    clean_utc = f"{base_utc[:3]}:{base_utc[-2:]}"
    depart_strtime = f"{depart_datetime:%Y-%m-%dT%H:%M}:00{clean_utc}"
    parameters = {
        "subscription-key": maps_key,
        "api-version": "1.0",
        "query": query,
        "timeBudgetInSec": time_budget,
        "TravelMode": "car",
        "departAt": f"{depart_strtime}",
    }
    query_string = urlencode(parameters)

    full_url = f"{base_url}{query_string}"

    bounds = requests.get(full_url).json()["reachableRange"]["boundary"]
    return bounds


# s TRANSIT, WALK, BICYCLE, CAR, BUS, RAIL,
def otp_isochrone(
    coord: Tuple[int, int],
    minutes_budget: int,
    arrive_time: str = "7:30am",
    arrive_by: bool = True,
    weekday: bool = True,
    travel_mode: str = "WALK, TRANSIT",
):
    """Make an isochrone using open trip planner running locally."""
    base_url = "http://localhost:8080/otp/routers/calgary/isochrone?"
    # We need a date for travel times, and it has to be slightly in the future
    # set it to next week and then adjust back if we need to make it a weekend or not
    today = dt.date.today() + dt.timedelta(days=7)
    today_is_weekend = today.isoweekday() in (6, 7)
    if today_is_weekend & weekday:
        today += dt.timedelta(days=8 - today.isoweekday())
    elif (not today_is_weekend) & (not weekday):
        today += dt.timedelta(days=6 - today.isoweekday())
    params = {
        # Have to have both of these so I can swap in arriveBy
        "toPlace": f"{coord[0]},{coord[1]}",
        "fromPlace": f"{coord[0]},{coord[1]}",
        "time": arrive_time,
        "date": f"{today:%m-%d-%Y}",
        "arriveBy": f"{arrive_by}".lower(),
        "walkReluctance": 5,
        "waitReluctance": 10,
        "cutoffSec": minutes_budget * 60,
        "mode": travel_mode,
    }
    query_string = urlencode(params)
    url = f"{base_url}{query_string}"
    rq = requests.get(url)
    bounds = rq.json()["features"][0]["geometry"]
    return bounds


def get_all_isochrones():
    """Let's just try a big one."""
    isochrone_dir = Path("data/isochrone")
    with open(isochrone_dir / "places.json", "r") as f:
        coords = json.load(f)
    modes = ["WALK, TRANSIT", "WALK", "CAR", "BICYCLE", "WALK, BICYCLE, TRANSIT"]
    times = [i for i in range(10, 65, 5)]
    isochrones = list()
    for place_name, details in coords.items():
        for mode in modes:
            for cutoff_time in times:
                isochrone = {
                    "place_name": place_name,
                    "mode": mode,
                    "cutoff_time": cutoff_time,
                    "shape": otp_isochrone(
                        coord=details["coordinate"],
                        minutes_budget=cutoff_time,
                        travel_mode=mode,
                        arrive_time=details["time"],
                        weekday=details["weekday"],
                    ),
                }
                isochrones.append(isochrone)
    return isochrones


def save_all_isochrones():
    """Dump them all to a file."""
    isochrones = get_all_isochrones()
    out_file = Path("data/isochrone/isochrones.json")
    with open(out_file, "w") as f:
        json.dump(isochrones, f)


def test_azure_isochrone():
    """Compare driving from Azure vs OSM."""
    isochrone_dir = Path("data/isochrone")
    with open(isochrone_dir / "places.json", "r") as f:
        coords = json.load(f)
    place_name, details = list(coords.items())[0]
    cutoff_time = 20
    isochrone = {
        "place_name": place_name,
        "mode": "azure_car",
        "cutoff_time": cutoff_time,
        "shape": azure_isochrone(
            coord=details["coordinate"],
            minutes_budget=cutoff_time,
            depart_at=details["time"],
            weekday=details["weekday"],
        ),
    }
    print(isochrone)


def get_azure_maps_isochrones():
    """Compare driving from Azure vs OSM."""
    isochrone_dir = Path("data/isochrone")
    with open(isochrone_dir / "places.json", "r") as f:
        coords = json.load(f)
    times = [i for i in range(10, 65, 5)]
    isochrones = list()
    for place_name, details in coords.items():
        for cutoff_time in times:
            isochrone = {
                "place_name": place_name,
                "mode": "azure_car",
                "cutoff_time": cutoff_time,
                "shape": azure_isochrone(
                    coord=details["coordinate"],
                    minutes_budget=cutoff_time,
                    depart_at=details["time"],
                    weekday=details["weekday"],
                ),
            }
            isochrones.append(isochrone)
    return isochrones


def save_azure_isochrones():
    """Dump them all to a file."""
    isochrones = get_azure_maps_isochrones()
    out_file = Path("data/isochrone/azure_isochrones.json")
    with open(out_file, "w") as f:
        json.dump(isochrones, f)


if __name__ == "__main__":
    save_all_isochrones()
    # save_azure_isochrones()
    # test_azure_isochrone()
