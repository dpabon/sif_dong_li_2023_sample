import os
from datetime import datetime, timezone
from pathlib import Path
import json
from typing import Dict, List, Optional
import rasterio
from rasterio.warp import calculate_default_transform, transform_bounds
from shapely.geometry import box, mapping
import pystac
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.raster import RasterExtension, RasterBand, DataType
from pystac.extensions.eo import EOExtension, Band as EOBand


def get_raster_metadata(file_path: str) -> Dict:
    """Extract metadata from a GeoTIFF file."""
    with rasterio.open(file_path) as src:
        # Get bounds in EPSG:4326
        bounds_4326 = transform_bounds(src.crs, "EPSG:4326", *src.bounds)

        # Create bbox and geometry
        bbox = list(bounds_4326)
        geometry = mapping(box(*bounds_4326))

        # Get additional metadata
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
    """
    Parse date from filename format: SIF_YYYYMMDD.tif
    Returns datetime object in UTC.
    """
    # Extract date string from filename (e.g., "20230701" from "SIF_20230701.tif")
    date_str = filename.replace("SIF_", "").replace(".tif", "")
    date_obj = datetime.strptime(date_str, "%Y%m%d")
    # Make timezone-aware (UTC)
    return date_obj.replace(tzinfo=timezone.utc)


def create_stac_item(
    file_path: str, github_raw_url: str, collection_id: str = "sif-collection"
) -> pystac.Item:
    """
    Create an OpenEO and CDSE-compliant STAC Item for a single SIF GeoTIFF file.

    Args:
        file_path: Path to the GeoTIFF file
        github_raw_url: Base URL for raw GitHub content
        collection_id: ID of the parent collection
    """
    filename = os.path.basename(file_path)
    item_id = filename.replace(".tif", "")

    # Parse date from filename
    date_time = parse_date_from_filename(filename)

    # Get raster metadata
    metadata = get_raster_metadata(file_path)

    # Create STAC Item with required extensions
    item = pystac.Item(
        id=item_id,
        geometry=metadata["geometry"],
        bbox=metadata["bbox"],
        datetime=date_time,
        properties={
            "description": f"Solar-Induced Fluorescence (SIF) data for {date_time.strftime('%Y-%m-%d')}",
        },
        stac_extensions=[
            "https://stac-extensions.github.io/eo/v1.1.0/schema.json",
            "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
            "https://stac-extensions.github.io/raster/v1.1.0/schema.json",
        ],
    )

    # Add EO extension with CDSE-compatible band information (with wavelengths)
    eo_ext = EOExtension.ext(item, add_if_missing=True)
    eo_ext.bands = [
        EOBand.create(
            name="SIF",
            description="Solar-Induced Fluorescence",
            common_name=None,  # SIF is not a standard common name
            center_wavelength=0.740,  # 740 nm (typical SIF observation wavelength)
            full_width_half_max=0.040,  # ~40 nm bandwidth (approximate)
        )
    ]

    # Add the GeoTIFF asset
    asset_url = f"{github_raw_url}/{filename}"
    asset = pystac.Asset(
        href=asset_url,
        media_type=pystac.MediaType.GEOTIFF,
        roles=["data"],
        title="SIF GeoTIFF",
    )
    item.add_asset("data", asset)

    # Add projection extension
    proj_ext = ProjectionExtension.ext(item, add_if_missing=True)
    if metadata["crs"]:
        crs_string = metadata["crs"]
        # Extract EPSG code if present
        if ":" in crs_string and crs_string.split(":")[0].upper() == "EPSG":
            proj_ext.epsg = int(crs_string.split(":")[1])
        proj_ext.transform = metadata["transform"]
        proj_ext.shape = metadata["shape"]

    # Add raster extension with detailed band metadata
    raster_ext = RasterExtension.ext(asset, add_if_missing=True)

    # Map numpy dtypes to STAC raster DataType enum
    dtype_map = {
        "uint8": DataType.UINT8,
        "uint16": DataType.UINT16,
        "uint32": DataType.UINT32,
        "int8": DataType.INT8,
        "int16": DataType.INT16,
        "int32": DataType.INT32,
        "float32": DataType.FLOAT32,
        "float64": DataType.FLOAT64,
    }

    data_type = dtype_map.get(metadata["dtype"], DataType.FLOAT32)

    raster_ext.bands = [
        RasterBand.create(
            nodata=metadata["nodata"],
            data_type=data_type,
            unit="W/m²/sr/μm",  # Standard SIF units
            spatial_resolution=None,  # Will be calculated from transform if needed
        )
    ]

    # CRITICAL for CDSE: Add eo:bands reference to asset
    # This tells CDSE which band in the file corresponds to which band definition
    asset.extra_fields["eo:bands"] = [0]  # Reference to the first (and only) band

    return item


def create_stac_catalog(
    data_dir: str,
    output_dir: str,
    github_repo_url: str,
    catalog_title: str = "SIF Data Catalog",
    catalog_description: str = "STAC Catalog for Solar-Induced Fluorescence (SIF) data",
    collection_title: str = "SIF Collection",
    collection_description: str = "Daily Solar-Induced Fluorescence measurements",
):
    """
    Create a complete OpenEO and CDSE-compliant STAC catalog structure.
    Uses ABSOLUTE URLs for CDSE compatibility.

    Args:
        data_dir: Directory containing the GeoTIFF files
        output_dir: Directory where STAC JSON files will be written
        github_repo_url: GitHub repository URL (e.g., 'https://github.com/username/repo')
        catalog_title: Title for the STAC catalog
        catalog_description: Description for the STAC catalog
        collection_title: Title for the collection
        collection_description: Description for the collection
    """
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Construct GitHub raw content URL
    # Format: https://raw.githubusercontent.com/username/repo/main/
    github_raw_url = (
        github_repo_url.replace(
            "https://github.com/", "https://raw.githubusercontent.com/"
        ).rstrip("/")
        + "/main"
    )

    # Create root catalog
    catalog = pystac.Catalog(
        id="sif-catalog", description=catalog_description, title=catalog_title
    )

    # Create collection with OpenEO-required extensions
    collection = pystac.Collection(
        id="sif-collection",
        description=collection_description,
        title=collection_title,
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),  # Placeholder
            temporal=pystac.TemporalExtent([[None, None]]),  # Will be updated
        ),
        license="proprietary",  # Change as needed
        stac_extensions=[
            "https://stac-extensions.github.io/eo/v1.1.0/schema.json",
            "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
            "https://stac-extensions.github.io/raster/v1.1.0/schema.json",
        ],
    )

    # Add OpenEO-specific collection properties
    collection.extra_fields["cube:dimensions"] = {
        "x": {
            "type": "spatial",
            "axis": "x",
            "reference_system": "AUTO:42001",  # Will be updated with actual EPSG
        },
        "y": {
            "type": "spatial",
            "axis": "y",
            "reference_system": "AUTO:42001",  # Will be updated with actual EPSG
        },
        "t": {
            "type": "temporal",
            "extent": [None, None],  # Will be updated
        },
        "bands": {"type": "bands", "values": ["SIF"]},
    }

    # CRITICAL for CDSE: Add detailed band summaries with wavelength information
    collection.summaries = pystac.Summaries(
        {
            "eo:bands": [
                {
                    "name": "SIF",
                    "description": "Solar-Induced Fluorescence",
                    "center_wavelength": 0.740,  # 740 nm
                    "full_width_half_max": 0.040,  # ~40 nm bandwidth
                }
            ],
            "proj:epsg": [],  # Will be populated with actual EPSG codes
        }
    )

    # Add EO extension at collection level with CDSE-compatible band info
    eo_ext = EOExtension.summaries(collection, add_if_missing=True)
    eo_ext.bands = [
        EOBand.create(
            name="SIF",
            description="Solar-Induced Fluorescence",
            common_name=None,
            center_wavelength=0.740,
            full_width_half_max=0.040,
        )
    ]

    # Find all SIF GeoTIFF files
    tif_files = sorted(Path(data_dir).glob("SIF_*.tif"))

    print(f"Found {len(tif_files)} GeoTIFF files")

    # Track temporal extent and collect metadata
    dates = []
    all_bboxes = []
    epsg_codes = set()

    # Create items
    for tif_file in tif_files:
        print(f"Processing {tif_file.name}...")

        item = create_stac_item(str(tif_file), github_raw_url, collection.id)

        collection.add_item(item)
        dates.append(item.datetime)
        all_bboxes.append(item.bbox)

        # Collect EPSG codes
        proj_ext = ProjectionExtension.ext(item)
        if proj_ext.epsg:
            epsg_codes.add(proj_ext.epsg)

    # Update collection extent
    if dates:
        start_date = min(dates)
        end_date = max(dates)
        collection.extent.temporal.intervals = [[start_date, end_date]]

        # Update cube:dimensions temporal extent
        collection.extra_fields["cube:dimensions"]["t"]["extent"] = [
            start_date.isoformat(),
            end_date.isoformat(),
        ]

    if all_bboxes:
        # Calculate union of all bboxes
        min_lon = min(bbox[0] for bbox in all_bboxes)
        min_lat = min(bbox[1] for bbox in all_bboxes)
        max_lon = max(bbox[2] for bbox in all_bboxes)
        max_lat = max(bbox[3] for bbox in all_bboxes)
        collection.extent.spatial.bboxes = [[min_lon, min_lat, max_lon, max_lat]]

        # Update cube:dimensions spatial extent
        collection.extra_fields["cube:dimensions"]["x"]["extent"] = [min_lon, max_lon]
        collection.extra_fields["cube:dimensions"]["y"]["extent"] = [min_lat, max_lat]

    # Update reference system if we have a consistent EPSG
    if len(epsg_codes) == 1:
        epsg = list(epsg_codes)[0]
        collection.extra_fields["cube:dimensions"]["x"]["reference_system"] = epsg
        collection.extra_fields["cube:dimensions"]["y"]["reference_system"] = epsg
        # Update summaries with actual EPSG
        collection.summaries.add("proj:epsg", list(epsg_codes))
    elif epsg_codes:
        collection.summaries.add("proj:epsg", list(epsg_codes))

    # Add collection to catalog
    catalog.add_child(collection)

    # CRITICAL FOR CDSE: Use absolute URLs instead of relative paths
    # CDSE cannot resolve relative paths in STAC collections
    base_stac_url = (
        github_repo_url.replace(
            "https://github.com/", "https://raw.githubusercontent.com/"
        ).rstrip("/")
        + "/main/stac"
    )

    # First save with relative paths
    catalog.normalize_hrefs(str(output_path))
    catalog.save(
        catalog_type=pystac.CatalogType.SELF_CONTAINED, dest_href=str(output_path)
    )

    # Now update all links to absolute URLs for CDSE
    print("\nConverting links to absolute URLs for CDSE compatibility...")

    # Update catalog links
    catalog_file = output_path / "catalog.json"
    if catalog_file.exists():
        import json

        with open(catalog_file) as f:
            catalog_data = json.load(f)

        # Update self link
        for link in catalog_data.get("links", []):
            if link["rel"] == "self":
                link["href"] = f"{base_stac_url}/catalog.json"
            elif link["rel"] == "child":
                link["href"] = f"{base_stac_url}/sif-collection/collection.json"

        with open(catalog_file, "w") as f:
            json.dump(catalog_data, f, indent=2)

    # Update collection links
    collection_file = output_path / "sif-collection" / "collection.json"
    if collection_file.exists():
        import json

        with open(collection_file) as f:
            collection_data = json.load(f)

        # Update links to absolute URLs
        for link in collection_data.get("links", []):
            if link["rel"] == "self":
                link["href"] = f"{base_stac_url}/sif-collection/collection.json"
            elif link["rel"] == "root":
                link["href"] = f"{base_stac_url}/catalog.json"
            elif link["rel"] == "parent":
                link["href"] = f"{base_stac_url}/catalog.json"
            elif link["rel"] == "item":
                # Convert relative item path to absolute
                # e.g., ./SIF_20230701/SIF_20230701.json -> https://.../stac/sif-collection/SIF_20230701/SIF_20230701.json
                href = link["href"]
                if href.startswith("./"):
                    href = href[2:]
                link["href"] = f"{base_stac_url}/sif-collection/{href}"

        with open(collection_file, "w") as f:
            json.dump(collection_data, f, indent=2)

    # Update all item links
    for item_dir in (output_path / "sif-collection").glob("SIF_*"):
        if item_dir.is_dir():
            item_file = item_dir / f"{item_dir.name}.json"
            if item_file.exists():
                import json

                with open(item_file) as f:
                    item_data = json.load(f)

                # Update links
                for link in item_data.get("links", []):
                    if link["rel"] == "self":
                        link["href"] = (
                            f"{base_stac_url}/sif-collection/{item_dir.name}/{item_dir.name}.json"
                        )
                    elif link["rel"] == "collection":
                        link["href"] = f"{base_stac_url}/sif-collection/collection.json"
                    elif link["rel"] == "parent":
                        link["href"] = f"{base_stac_url}/sif-collection/collection.json"
                    elif link["rel"] == "root":
                        link["href"] = f"{base_stac_url}/catalog.json"

                with open(item_file, "w") as f:
                    json.dump(item_data, f, indent=2)

    print(f"✓ All links converted to absolute URLs")

    print(f"\nOpenEO-compliant STAC catalog created successfully in {output_dir}")
    print(f"Root catalog: {output_path / 'catalog.json'}")
    print(f"Collection: {output_path / collection.id / 'collection.json'}")
    print(f"Total items: {len(tif_files)}")
    print(f"Temporal extent: {start_date.date()} to {end_date.date()}")
    print(f"Spatial extent: {all_bboxes[0] if all_bboxes else 'N/A'}")

    return catalog


def update_urls_for_github(stac_dir: str, github_pages_url: str):
    """
    Update all self links in STAC files to use GitHub Pages URLs.
    This is optional but helps with absolute URLs.

    Args:
        stac_dir: Directory containing STAC JSON files
        github_pages_url: GitHub Pages URL (e.g., 'https://username.github.io/repo')
    """
    stac_path = Path(stac_dir)

    # Update all JSON files
    for json_file in stac_path.rglob("*.json"):
        with open(json_file, "r") as f:
            data = json.load(f)

        # Update self link if present
        if "links" in data:
            for link in data["links"]:
                if link.get("rel") == "self":
                    # Convert relative path to absolute GitHub Pages URL
                    rel_path = json_file.relative_to(stac_path)
                    link["href"] = f"{github_pages_url.rstrip('/')}/{rel_path}"

        with open(json_file, "w") as f:
            json.dump(data, f, indent=2)

    print(f"Updated URLs to use GitHub Pages: {github_pages_url}")


# Configuration
DATA_DIR = "data"  # Directory with your GeoTIFF files
OUTPUT_DIR = "stac"  # Where STAC JSON files will be created
GITHUB_REPO_URL = "https://github.com/dpabon/sif_dong_li_2023_sample"

# Create catalog
catalog = create_stac_catalog(
    data_dir=DATA_DIR,
    output_dir=OUTPUT_DIR,
    github_repo_url=GITHUB_REPO_URL,
    catalog_title="SIF Data Catalog - July 2023",
    catalog_description="Daily Solar-Induced Fluorescence measurements for July 2023",
    collection_title="SIF July 2023",
    collection_description="Solar-Induced Fluorescence daily data covering July 2023 provided by Dong Li",
)
