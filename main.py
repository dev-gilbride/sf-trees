import asyncio
import argparse
import geopandas as gpd
from geopy.adapters import AioHTTPAdapter
from geopy.geocoders import Nominatim
import json
import pandas as pd
from shapely.geometry import Point
import urllib.request


async def get_address_coords(address: str) -> dict:
    async with Nominatim(
        user_agent="tree_radius",
        adapter_factory=AioHTTPAdapter,
    ) as geolocator:
        location = await geolocator.geocode(address)
        return {
            "Latitude": location.latitude,
            "Longitude": location.longitude
        }


def get_sf_tree_data(page_size: int, offset: int = 0):
    query = ("select rowid, TreeID, qLegalStatus, qSpecies, qAddress, SiteOrder, qSiteInfo, PlantType, qCaretaker, qCareAssistant, PlantDate, "
             "DBH, PlotSize, PermitNotes, XCoord, YCoord, Latitude, Longitude, Location from Street_Tree_List order by rowid limit "
             f'{page_size}'
             )
    if offset:
        query = query + f' offset {offset}'
    query = urllib.parse.quote_plus(query)
    url = f'https://san-francisco.datasettes.com/sf-trees.json?sql={query}'
    print(f'Obtaining: {url}')
    req = urllib.request.Request(
        url,
        data=None,
        headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
        }
    )
    with urllib.request.urlopen(req) as response:
        data = json.load(response)
        return data


def filter_by_proximity(center: dict, radius: float, data: dict):
    df = pd.DataFrame(data["rows"])
    df = df.rename(columns={0: "rowid", 1: "TreeID", 2: "qLegalStatus", 3: "qSpecies", 4: "qAddress", 5: "SiteOrder", 6: "qSiteInfo",
                            7: "PlantType", 8: "qCaretaker", 9: "qCareAssistant", 10: "PlantDate", 11: "DBH", 12: "PlotSize", 13: "PermitNotes",
                            14: "XCoord", 15: "YCoord", 16: "Latitude", 17: "Longitude", 18: "Location"})
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(
            df.Longitude, df.Latitude, crs="EPSG:4326"))
    center_point = Point(center['Longitude'], center['Latitude'])
    center_point_geodetic = gpd.GeoSeries([center_point], crs="EPSG:4326")
    center_point_cartesian = center_point_geodetic.to_crs(crs="EPSG:3857").iloc[0]
    gdf_cartesian = gdf.to_crs(crs="EPSG:3857")
    gdf["proximity"] = gdf_cartesian.distance(center_point_cartesian)
    return (gdf[df["proximity"] <= radius])


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Find trees with a block radius of given address.")
    parser.add_argument("--address", type=str, help="Address to center the search around.")
    parser.add_argument("--blocks", type=int, help="Number of blocks to extend search radius.")
    parser.add_argument("--block-length", required=False, default=182.88, type=float,
                        help="Length in meters to measure a block. Defaults to US average of 182.88m.")
    parser.add_argument("--page-size", required=False, default=10000, type=int, help="Number of database entries per request. Defaults to 10000.")
    args = parser.parse_args()

    center_coordinates = asyncio.run(get_address_coords(args.address))

    radius = args.block_length * args.blocks
    offset = 0
    trees_to_add = []
    while True:  # Do While Loop
        data = get_sf_tree_data(args.page_size, offset)
        if len(data["rows"]) > 0:
            offset += args.page_size
            trees = filter_by_proximity(center_coordinates, radius, data)
            trees_to_add.append(trees)
            print(f'Trees appended: {len(trees_to_add)}')
        else:
            break
    trees_in_range = pd.concat(trees_to_add)
    trees_in_range.reset_index(drop=True, inplace=True)

    print(f'There are {len(trees_in_range.index)} trees within a {radius}m radius.')
    print(f'Where the radius consists of {args.blocks} blocks of length {args.block_length}m.')
    print(f'Centered around address: {args.address}')
    if len(trees_in_range.index) > 0:
        print(trees_in_range)
