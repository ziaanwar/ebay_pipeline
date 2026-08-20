from dagster import Definitions, load_assets_from_modules
from assets import ingestion

all_assets = load_assets_from_modules([ingestion])

defs = Definitions(
    assets=all_assets
)
