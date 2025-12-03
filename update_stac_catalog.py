import os
import json
from pathlib import Path
from datetime import datetime
from create_stac_catalog import (
    create_stac_item,
    get_raster_metadata,
    parse_date_from_filename,
)
import pystac


def get_existing_items(collection_dir: Path) -> set:
    """Get IDs of existing items in the collection."""
    existing = set()

    for item_file in collection_dir.rglob("SIF_*.json"):
        # Skip the collection.json itself
        if item_file.name == "collection.json":
            continue

        try:
            with open(item_file) as f:
                item_data = json.load(f)
                existing.add(item_data["id"])
        except Exception as e:
            print(f"Warning: Could not read {item_file}: {e}")

    return existing


def update_collection_extent(collection_path: Path):
    """Update the collection's temporal and spatial extent based on all items."""

    with open(collection_path) as f:
        collection_data = json.load(f)

    # Find all item files
    collection_dir = collection_path.parent
    item_files = list(collection_dir.rglob("SIF_*.json"))
    item_files = [f for f in item_files if f.name != "collection.json"]

    if not item_files:
        print("Warning: No items found in collection")
        return

    # Collect all datetimes and bboxes
    datetimes = []
    bboxes = []

    for item_file in item_files:
        with open(item_file) as f:
            item_data = json.load(f)

            if "properties" in item_data and "datetime" in item_data["properties"]:
                dt_str = item_data["properties"]["datetime"]
                datetimes.append(datetime.fromisoformat(dt_str.replace("Z", "+00:00")))

            if "bbox" in item_data:
                bboxes.append(item_data["bbox"])

    # Update temporal extent
    if datetimes:
        start_date = min(datetimes)
        end_date = max(datetimes)

        collection_data["extent"]["temporal"]["interval"] = [
            [start_date.isoformat(), end_date.isoformat()]
        ]

        # Update cube:dimensions temporal extent if present
        if (
            "cube:dimensions" in collection_data
            and "t" in collection_data["cube:dimensions"]
        ):
            collection_data["cube:dimensions"]["t"]["extent"] = [
                start_date.isoformat(),
                end_date.isoformat(),
            ]

    # Update spatial extent
    if bboxes:
        min_lon = min(bbox[0] for bbox in bboxes)
        min_lat = min(bbox[1] for bbox in bboxes)
        max_lon = max(bbox[2] for bbox in bboxes)
        max_lat = max(bbox[3] for bbox in bboxes)

        collection_data["extent"]["spatial"]["bbox"] = [
            [min_lon, min_lat, max_lon, max_lat]
        ]

        # Update cube:dimensions spatial extent if present
        if "cube:dimensions" in collection_data:
            if "x" in collection_data["cube:dimensions"]:
                collection_data["cube:dimensions"]["x"]["extent"] = [min_lon, max_lon]
            if "y" in collection_data["cube:dimensions"]:
                collection_data["cube:dimensions"]["y"]["extent"] = [min_lat, max_lat]

    # Write updated collection
    with open(collection_path, "w") as f:
        json.dump(collection_data, f, indent=2)

    print(f"✓ Updated collection extent: {len(item_files)} items")
    print(f"  Temporal: {start_date.date()} to {end_date.date()}")
    print(f"  Spatial: [{min_lon:.2f}, {min_lat:.2f}, {max_lon:.2f}, {max_lat:.2f}]")


def add_new_items(
    data_dir: str, stac_dir: str, github_repo_url: str, force: bool = False
):
    """
    Add new items to existing STAC catalog.

    Args:
        data_dir: Directory containing GeoTIFF files
        stac_dir: Directory containing existing STAC catalog
        github_repo_url: GitHub repository URL
        force: If True, regenerate all items (even existing ones)
    """

    data_path = Path(data_dir)
    stac_path = Path(stac_dir)

    # Check if catalog exists
    catalog_file = stac_path / "catalog.json"
    if not catalog_file.exists():
        print(f"Error: Catalog not found at {catalog_file}")
        print("Run create_stac_catalog.py first to create the initial catalog.")
        return

    # Load collection
    collection_dir = stac_path / "sif-collection"
    collection_file = collection_dir / "collection.json"

    if not collection_file.exists():
        print(f"Error: Collection not found at {collection_file}")
        return

    collection = pystac.Collection.from_file(str(collection_file))

    # Get existing items
    if not force:
        existing_items = get_existing_items(collection_dir)
        print(f"Found {len(existing_items)} existing items in catalog")
    else:
        existing_items = set()
        print("Force mode: regenerating all items")

    # Construct GitHub raw URL
    github_raw_url = (
        github_repo_url.replace(
            "https://github.com/", "https://raw.githubusercontent.com/"
        ).rstrip("/")
        + "/main/data"
    )

    # Find all GeoTIFF files
    tif_files = sorted(data_path.glob("SIF_*.tif"))
    print(f"Found {len(tif_files)} GeoTIFF files in {data_dir}")

    # Track new items
    new_items = []
    skipped_items = []

    # Process each file
    for tif_file in tif_files:
        item_id = tif_file.stem  # filename without extension

        # Skip if already exists (unless force mode)
        if not force and item_id in existing_items:
            skipped_items.append(item_id)
            continue

        print(f"Creating item: {item_id}...")

        try:
            # Create STAC item
            item = create_stac_item(str(tif_file), github_raw_url, collection.id)

            # Add to collection
            collection.add_item(item)
            new_items.append(item_id)

        except Exception as e:
            print(f"  Error creating item for {tif_file.name}: {e}")
            continue

    # Save updated catalog
    if new_items or force:
        print(f"\nSaving catalog with {len(new_items)} new items...")

        # Normalize hrefs
        collection.normalize_hrefs(str(stac_path / "sif-collection"))

        # Save collection
        collection.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)

        # Update collection extent
        update_collection_extent(collection_file)

        print(f"\n✓ Catalog updated successfully!")
        print(f"  New items added: {len(new_items)}")
        print(f"  Existing items: {len(skipped_items)}")
        print(f"  Total items: {len(new_items) + len(skipped_items)}")

        if new_items:
            print(f"\nNew items:")
            for item_id in new_items[:10]:  # Show first 10
                print(f"  - {item_id}")
            if len(new_items) > 10:
                print(f"  ... and {len(new_items) - 10} more")

    else:
        print("\n✓ No new items to add. Catalog is up to date.")

    return len(new_items)


def main():
    """Main function."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Update STAC catalog with new SIF GeoTIFF files"
    )
    parser.add_argument(
        "--data-dir",
        default="./data",
        help="Directory containing GeoTIFF files (default: ./data)",
    )
    parser.add_argument(
        "--stac-dir",
        default="./stac",
        help="Directory containing STAC catalog (default: ./stac)",
    )
    parser.add_argument(
        "--github-url",
        required=True,
        help="GitHub repository URL (e.g., https://github.com/user/repo)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate all items, including existing ones",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("STAC Catalog Updater")
    print("=" * 60)
    print()

    num_added = add_new_items(
        data_dir=args.data_dir,
        stac_dir=args.stac_dir,
        github_repo_url=args.github_url,
        force=args.force,
    )

    if num_added > 0:
        print("\n" + "=" * 60)
        print("Next steps:")
        print("1. Validate the updated catalog:")
        print("   python validate_openeo_stac.py")
        print("2. Commit and push changes:")
        print("   git add stac/")
        print("   git commit -m 'Update STAC catalog with new items'")
        print("   git push")
        print("=" * 60)


if __name__ == "__main__":
    main()
