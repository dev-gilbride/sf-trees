import aiohttp
import asyncio
import argparse
import geopandas as gpd
from geopy.adapters import AioHTTPAdapter
import geopy.geocoders
from geopy.geocoders import Nominatim
import logging
import math
import pandas as pd
from shapely.geometry import Point
import urllib.request


async def get_address_coords(address: str) -> dict:
    """Retrieves latitude, longitude coordinates from Open Street Map (Nominatim)."""

    geopy.geocoders.options.default_timeout = 3
    async with Nominatim(
        user_agent="tree_radius",
        adapter_factory=AioHTTPAdapter,
    ) as geolocator:
        attempts = 0
        while attempts < 5:
            try:
                location = await geolocator.geocode(address)
                return {
                    "Latitude": location.latitude,
                    "Longitude": location.longitude
                }
            except (geopy.exc.GeocoderTimedOut,
                    geopy.exc.GeocoderUnavailable,
                    geopy.exc.GeocoderServiceError,
                    geopy.exc.GeocoderQuotaExceeded) as err:
                logging.warning(f'Attempt {attempts + 1} failed with error {err}')
                attempts += 1
        raise ConnectionError("Failed to retrieve coordinates for the address.")


async def get_sf_tree_data(page_size: int, offset: int) -> dict:
    """Queries the sf-trees datasette for a page of entries with offset."""

    SF_TREES_URL = "https://san-francisco.datasettes.com/sf-trees"

    query = ("select rowid, TreeID, qLegalStatus, qSpecies, qAddress, SiteOrder, qSiteInfo, PlantType, qCaretaker, qCareAssistant, PlantDate, "
             "DBH, PlotSize, PermitNotes, XCoord, YCoord, Latitude, Longitude, Location from Street_Tree_List order by rowid limit "
             f'{page_size}'
             )
    if offset:
        query = query + f' offset {offset}'
    query = urllib.parse.quote_plus(query)
    url = SF_TREES_URL + '.json?sql=' + query
    logging.debug(f'Request Offset {offset}: Obtaining query: {url}')
    attempts = 0
    while attempts < 10:
        try:
            timeout = aiohttp.ClientTimeout(total=3)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        raise aiohttp.ClientConnectorError(f'Query returned a bad reponse status: {resp.status}')
                    data = await resp.json()
                    logging.debug(f'Data obtained for offset: {offset}')
                    return data
        except (asyncio.exceptions.TimeoutError,
                aiohttp.ClientConnectorError) as err:
            logging.warning(f'Request Offset {offset}: Attempt {attempts + 1} failed with error {err}')
            attempts += 1
    raise ConnectionError(f'Failed to retrieve url: {url}')


async def filter_by_proximity(center: dict, radius: float, data: dict, query_offset: int) -> gpd.GeoDataFrame:
    """Converts data into a GeoDataFrame and returns entries within radius distance of the center."""

    df = pd.DataFrame(data["rows"])
    df = df.rename(columns={0: "rowid", 1: "TreeID", 2: "qLegalStatus", 3: "qSpecies", 4: "qAddress", 5: "SiteOrder", 6: "qSiteInfo",
                            7: "PlantType", 8: "qCaretaker", 9: "qCareAssistant", 10: "PlantDate", 11: "DBH", 12: "PlotSize", 13: "PermitNotes",
                            14: "XCoord", 15: "YCoord", 16: "Latitude", 17: "Longitude", 18: "Location"})
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(
            df.Longitude, df.Latitude, crs="EPSG:4326"))
    logging.debug(f'Request Offset {query_offset}: GeoDataFrame entry count: {len(gdf.index)}')
    center_point = Point(center['Longitude'], center['Latitude'])
    center_point_geodetic = gpd.GeoSeries([center_point], crs="EPSG:4326")
    center_point_cartesian = center_point_geodetic.to_crs(crs="EPSG:3857").iloc[0]
    gdf_cartesian = gdf.to_crs(crs="EPSG:3857")
    gdf["proximity"] = gdf_cartesian.distance(center_point_cartesian)
    return (gdf[df["proximity"] <= radius])


async def consumer(name: int, q: asyncio.Queue, center: dict, radius: float, page_size: int) -> list:
    """Consumer for query requests in queue based off an offset value. Returns list of GeoDataFrames accrued upon reaching an empty query result."""

    trees_to_add = []
    while True:
        query_offset = await q.get()
        data = await get_sf_tree_data(page_size, query_offset)
        if len(data["rows"]) <= 0:
            logging.debug(f'Consumer ID {name} Request Offset {query_offset}: Reached empty result. Returning values and exiting.')
            return trees_to_add
        logging.debug(f'Consumer ID {name} Request Offset {query_offset}: Data collected from URL count: {len(data["rows"])}')
        trees = await filter_by_proximity(center, radius, data, query_offset)
        logging.debug(f'Consumer ID {name} Request Offset {query_offset}: Filtered tree count: {len(trees.index)}')
        trees_to_add.append(trees)
        q.task_done()


async def producer(q: asyncio.Queue, page_size: int) -> None:
    """Producer for query requests in queue based off an offset value. Waits on queue size to implement more."""

    query_offset = 0
    while True:
        logging.debug(f'Request offset {query_offset} added to queue.')
        await q.put(query_offset)
        query_offset += page_size


async def main(address: str, blocks: int, block_length: float, page_size: int, logging: str, runners: int):
    """Producer for query requests in queue based off an offset value. Waits on queue size to implement more."""
    queue_size = math.ceil(runners * 1.5)
    q = asyncio.Queue(maxsize=queue_size)

    center_coordinates = await get_address_coords(address)
    radius = block_length * blocks

    producer_task = asyncio.create_task(producer(q, page_size))
    consumers = [asyncio.create_task(consumer(name, q, center_coordinates, radius, page_size)) for name in range(runners)]
    returned_trees = await asyncio.gather(*consumers)
    producer_task.cancel()

    trees_to_add = [df for sublist in returned_trees for df in sublist]
    trees_in_range = pd.concat(trees_to_add)
    trees_in_range.reset_index(drop=True, inplace=True)

    print(f'There are {len(trees_in_range.index)} trees within a {radius}m radius.')
    print(f'Where the radius consists of {blocks} blocks of length {block_length}m.')
    print(f'Centered around address: {address}')
    if len(trees_in_range.index) > 0:
        print(trees_in_range)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find trees with a block radius of given address.")
    parser.add_argument("--address", type=str, help="Address to center the search around.")
    parser.add_argument("--blocks", type=int, help="Number of blocks to extend search radius.")
    parser.add_argument("--block-length", required=False, default=182.88, type=float,
                        help="Length in meters to measure a block. Defaults to US average of 182.88m.")
    parser.add_argument("--page-size", required=False, choices=range(100, 1000), default=1000, type=int, metavar="[100-1000]",
                        help="Number of database entries per request. Defaults to 1000.")
    parser.add_argument("--logging", required=False, default='info', type=str, help="Logging mode. Defaults to INFO.")
    parser.add_argument("--runners", required=False, default=20, type=int, help="Number of runners to consume from queue.")
    args = parser.parse_args()

    logging_level = getattr(logging, args.logging.upper(), None)
    logging.basicConfig(level=logging_level)

    asyncio.run(main(**args.__dict__))
