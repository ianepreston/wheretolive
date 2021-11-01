#!/usr/bin/env bash

echo "Be sure to run download_osm_data.py and download_transit_data.py first"

# If we don't have the graph generated create it
if [[ ! -f $PWD/../data/isochrone/commute/calgary/Graph.obj ]]; then
  echo "No graph built, building that first"
  docker run \
    -v $PWD/../data/isochrone/commute:/var/otp/graphs \
    -e JAVA_OPTIONS=-Xmx4G \
    urbica/otp --build /var/otp/graphs/calgary
fi

echo "running the actual planner"
docker run \
  -p 8080:8080 \
  -v $PWD/../data/isochrone/commute:/var/otp/graphs \
  -e JAVA_OPTIONS=-Xmx4G \
  urbica/otp --server  --analyst --autoScan --verbose
