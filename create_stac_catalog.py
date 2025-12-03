import os
from datetime import datetime, timezone
from pathlib import Path
import json
from typing import Dict, List
import rasterio
from rasterio.warp import transform_bounds
from shapely.geometry import box, mapping
import pystac


def get_raster_metadata(file_path: str) -> Dict:
    """Extract metadata from a GeoTIFF file."""
    with rasterio.open(file_path) as src:
        bounds_4326 = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
        bbox = list(bounds_4326)
        geometry = mapping(box(*bounds_4326))

        metadata = {
            "bbox": bbox,
            "geometry": geometry,
            "crs": src.crs.to_string() if src.crs else None,
            "shape": src.shape,
            "transform": list(src.transform),
            "dtype": src.dtypes[0],
            "nodata": src.nodata,
            "count": src.count,
        }
        return metadata


def parse_date_from_filename(filename: str) -> datetime:
    """Parse date from filename format: SIF_YYYYMMDD.tif"""
    date_str = filename.replace("SIF_", "").replace(".tif", "")
    date_obj = datetime.strptime(date_str, "%Y%m%d")
    return date_obj.replace(tzinfo=timezone.utc)


def create_cdse_compliant_collection(
    data_dir: str,
    output_dir: str,
    github_repo_url: str,
    collection_title: str = "SIF Collection",
    collection_description: str = "Daily Solar-Induced Fluorescence measurements",
):
    """
    Create a CDSE-compliant STAC collection matching their exact format.
    """

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Construct URLs
    base_stac_url = (
        github_repo_url.replace(
            "https://github.com/", "https://raw.githubusercontent.com/"
        ).rstrip("/")
        + "/main/stac"
    )

    github_raw_url = (
        github_repo_url.replace(
            "https://github.com/", "https://raw.githubusercontent.com/"
        ).rstrip("/")
        + "/main/data"
    )

    # Find all GeoTIFF files
    tif_files = sorted(Path(data_dir).glob("SIF_*.tif"))
    print(f"Found {len(tif_files)} GeoTIFF files")

    # Extract metadata from first file to get spatial extent
    first_metadata = get_raster_metadata(str(tif_files[0]))

    # Get EPSG code
    epsg = 4326  # Default
    if first_metadata["crs"]:
        crs_string = first_metadata["crs"]
        if ":" in crs_string:
            epsg = int(crs_string.split(":")[1])

    # Calculate spatial resolution (approximate)
    transform = first_metadata["transform"]
    spatial_resolution = abs(transform[0])  # pixel size in degrees

    # Collect all dates for temporal extent
    dates = []
    all_bboxes = []

    for tif_file in tif_files:
        date_time = parse_date_from_filename(tif_file.name)
        dates.append(date_time)
        metadata = get_raster_metadata(str(tif_file))
        all_bboxes.append(metadata["bbox"])

    # Calculate spatial extent
    min_lon = min(bbox[0] for bbox in all_bboxes)
    min_lat = min(bbox[1] for bbox in all_bboxes)
    max_lon = max(bbox[2] for bbox in all_bboxes)
    max_lat = max(bbox[3] for bbox in all_bboxes)

    start_date = min(dates)
    end_date = max(dates)

    # Create collection in CDSE format
    collection = {
        "type": "Collection",
        "id": "SIF_COLLECTION",
        "stac_version": "0.9.0",  # Match CDSE's version
        "stac_extensions": [
            "https://stac-extensions.github.io/datacube/v2.2.0/schema.json",
            "https://stac-extensions.github.io/eo/v1.1.0/schema.json",
        ],
        "title": collection_title,
        "description": collection_description,
        "license": "proprietary",
        "cube:dimensions": {
            "x": {
                "type": "spatial",
                "axis": "x",
                "extent": [min_lon, max_lon],
                "step": spatial_resolution,
                "reference_system": {
                    "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
                    "type": "GeodeticCRS",
                    "name": f"EPSG:{epsg}",
                    "id": {"authority": "EPSG", "code": epsg},
                },
            },
            "y": {
                "type": "spatial",
                "axis": "y",
                "extent": [min_lat, max_lat],
                "step": spatial_resolution,
                "reference_system": {
                    "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
                    "type": "GeodeticCRS",
                    "name": f"EPSG:{epsg}",
                    "id": {"authority": "EPSG", "code": epsg},
                },
            },
            "t": {
                "type": "temporal",
                "extent": [start_date.isoformat(), end_date.isoformat()],
            },
            "bands": {"type": "bands", "values": ["SIF"]},
        },
        "extent": {
            "spatial": {"bbox": [[min_lon, min_lat, max_lon, max_lat]]},
            "temporal": {"interval": [[start_date.isoformat(), end_date.isoformat()]]},
        },
        "summaries": {
            "eo:bands": [
                {
                    "name": "SIF",
                    "center_wavelength": 0.740,
                    "full_width_half_max": 0.040,
                    "common_name": None,
                    "gsd": spatial_resolution
                    * 111000,  # Convert degrees to meters (approximate)
                    "offset": 0,
                    "scale": 1.0,
                    "type": "float32",
                    "unit": "W/m²/sr/μm",
                }
            ],
            "bands": [
                {"name": "SIF", "eo:center_wavelength": 0.740, "eo:common_name": None}
            ],
            "gsd": [spatial_resolution * 111000],
        },
        "links": [
            {
                "rel": "self",
                "href": f"{base_stac_url}/sif-collection/collection.json",
                "type": "application/json",
            },
            {
                "rel": "root",
                "href": f"{base_stac_url}/catalog.json",
                "type": "application/json",
            },
            {
                "rel": "parent",
                "href": f"{base_stac_url}/catalog.json",
                "type": "application/json",
            },
        ],
    }

    # Add item links
    for tif_file in tif_files:
        item_id = tif_file.stem
        collection["links"].append(
            {
                "rel": "item",
                "href": f"{base_stac_url}/sif-collection/{item_id}/{item_id}.json",
                "type": "application/geo+json",
            }
        )

    # Save collection
    collection_dir = output_path / "sif-collection"
    collection_dir.mkdir(parents=True, exist_ok=True)

    collection_file = collection_dir / "collection.json"
    with open(collection_file, "w") as f:
        json.dump(collection, f, indent=2)

    print(f"✓ Created collection: {collection_file}")

    # Create items
    print("\nCreating items...")
    for tif_file in tif_files:
        filename = tif_file.name
        item_id = tif_file.stem

        date_time = parse_date_from_filename(filename)
        metadata = get_raster_metadata(str(tif_file))

        # Create item
        item = {
            "type": "Feature",
            "stac_version": "0.9.0",
            "stac_extensions": [
                "https://stac-extensions.github.io/eo/v1.1.0/schema.json",
                "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
            ],
            "id": item_id,
            "geometry": metadata["geometry"],
            "bbox": metadata["bbox"],
            "properties": {
                "datetime": date_time.isoformat(),
                "eo:bands": [
                    {
                        "name": "SIF",
                        "center_wavelength": 0.740,
                        "full_width_half_max": 0.040,
                    }
                ],
            },
            "assets": {
                "data": {
                    "href": f"{github_raw_url}/{filename}",
                    "type": "image/tiff; application=geotiff",
                    "roles": ["data"],
                    "title": "SIF GeoTIFF",
                    "eo:bands": [0],  # Reference to band index
                }
            },
            "links": [
                {
                    "rel": "self",
                    "href": f"{base_stac_url}/sif-collection/{item_id}/{item_id}.json",
                    "type": "application/geo+json",
                },
                {
                    "rel": "collection",
                    "href": f"{base_stac_url}/sif-collection/collection.json",
                    "type": "application/json",
                },
                {
                    "rel": "parent",
                    "href": f"{base_stac_url}/sif-collection/collection.json",
                    "type": "application/json",
                },
                {
                    "rel": "root",
                    "href": f"{base_stac_url}/catalog.json",
                    "type": "application/json",
                },
            ],
        }

        # Add projection info if available
        if metadata["crs"]:
            crs_string = metadata["crs"]
            if ":" in crs_string:
                item["properties"]["proj:epsg"] = int(crs_string.split(":")[1])
        item["properties"]["proj:shape"] = metadata["shape"]
        item["properties"]["proj:transform"] = metadata["transform"]

        # Save item
        item_dir = collection_dir / item_id
        item_dir.mkdir(parents=True, exist_ok=True)

        item_file = item_dir / f"{item_id}.json"
        with open(item_file, "w") as f:
            json.dump(item, f, indent=2)

        print(f"  Created: {item_id}")

    # Create root catalog
    catalog = {
        "type": "Catalog",
        "id": "sif-catalog",
        "stac_version": "0.9.0",
        "title": "SIF Data Catalog",
        "description": "STAC Catalog for Solar-Induced Fluorescence (SIF) data",
        "links": [
            {
                "rel": "self",
                "href": f"{base_stac_url}/catalog.json",
                "type": "application/json",
            },
            {
                "rel": "root",
                "href": f"{base_stac_url}/catalog.json",
                "type": "application/json",
            },
            {
                "rel": "child",
                "href": f"{base_stac_url}/sif-collection/collection.json",
                "type": "application/json",
                "title": collection_title,
            },
        ],
    }

    catalog_file = output_path / "catalog.json"
    with open(catalog_file, "w") as f:
        json.dump(catalog, f, indent=2)

    print(f"\n✓ Created catalog: {catalog_file}")

    print(f"\n{'=' * 70}")
    print("CDSE-Compatible STAC Catalog Created!")
    print(f"{'=' * 70}")
    print(f"\nCollection URL (use this with CDSE):")
    print(f"{base_stac_url}/sif-collection/collection.json")
    print(f"\nTotal items: {len(tif_files)}")
    print(f"Temporal extent: {start_date.date()} to {end_date.date()}")
    print(
        f"Spatial extent: [{min_lon:.2f}, {min_lat:.2f}, {max_lon:.2f}, {max_lat:.2f}]"
    )
    print(f"{'=' * 70}")


# Configuration
DATA_DIR = "data"  # Directory with your GeoTIFF files
OUTPUT_DIR = "stac"  # Where STAC JSON files will be created
GITHUB_REPO_URL = "https://github.com/dpabon/sif_dong_li_2023_sample"

create_cdse_compliant_collection(
    data_dir=DATA_DIR,
    output_dir=OUTPUT_DIR,
    github_repo_url=GITHUB_REPO_URL,
    collection_title="SIF July 2023",
    collection_description="Daily Solar-Induced Fluorescence measurements for July 2023",
)
