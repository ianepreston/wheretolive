"""Download open streetmap data for Calgary."""
import subprocess  # noqa: S404
from pathlib import Path

import requests


def download_alberta() -> Path:
    """Get the Alberta OSM file."""
    url = "https://download.geofabrik.de/north-america/canada/alberta-latest.osm.pbf"
    out_dir = Path("data/isochrone/commute/calgary")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "osm_alberta.osm.pbf"
    response = requests.get(url, stream=True, headers={"user-agent": None})
    with open(out_file, "wb") as handle:
        for chunk in response.iter_content(chunk_size=512):
            if chunk:  # filter out keep-alive new chunks
                handle.write(chunk)
    return out_file


def trim_calgary(alberta_osm: Path):
    """Trim down the Alberta dataset to just Calgary."""
    bounds = "-114.455184,50.838713,-113.713607,51.268603"
    calgary_osm = alberta_osm.parent / "calgary.osm.pbf"
    trim_args = [
        "osmconvert",
        f"{alberta_osm}",
        f"-b={bounds}",
        "--complete-ways",
        f"-o={calgary_osm}",
    ]
    subprocess.run(trim_args)  # noqa: S404,S603
    alberta_osm.unlink()


if __name__ == "__main__":
    ab_osm = download_alberta()
    trim_calgary(ab_osm)
