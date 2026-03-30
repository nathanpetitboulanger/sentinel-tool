import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple
import pystac
import pystac_client

logger = logging.getLogger(__name__)


class StacFetcher:
    """
    Network expert responsible for STAC API communications.
    Handles pagination, error retries, and temporal splitting.
    """

    def __init__(self, stac_url: str, collection: str):
        self.stac_url = stac_url
        self.collection = collection
        self.catalog = pystac_client.Client.open(self.stac_url)

    def _split_date_range(self, start_str: str, end_str: str) -> List[Tuple[str, str]]:
        """Split a time period into chunks of at most 1 year to avoid timeouts."""
        start = datetime.strptime(start_str, "%Y-%m-%d")
        end = datetime.strptime(end_str, "%Y-%m-%d")

        chunks = []
        current_start = start
        while current_start <= end:
            next_end = min(current_start + timedelta(days=365), end)
            chunks.append(
                (current_start.strftime("%Y-%m-%d"), next_end.strftime("%Y-%m-%d"))
            )
            current_start = next_end + timedelta(days=1)
        return chunks

    def _search_with_retry(
        self, search_params: Dict[str, Any], max_retries: int = 3
    ) -> List[pystac.Item]:
        """Execute a STAC search with exponential backoff."""
        for attempt in range(max_retries):
            try:
                search = self.catalog.search(**search_params)
                return list(search.items())
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    logger.warning(
                        f"STAC API error (attempt {attempt + 1}/{max_retries}). Retrying in {wait_time}s. Error: {e}"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(
                        f"STAC request failed after {max_retries} attempts."
                    )
                    raise e

    def fetch_items_for_batch(
        self,
        bbox: List[float],
        start_date: str,
        end_date: str,
        max_cloud_cover: int = 80,
    ) -> List[pystac.Item]:
        """Robustly fetch all STAC items for a given area and time period."""
        date_chunks = self._split_date_range(start_date, end_date)
        all_stac_items = []

        for sub_start, sub_end in date_chunks:
            search_params = {
                "collections": [self.collection],
                "bbox": bbox,
                "datetime": f"{sub_start}/{sub_end}",
                "query": {"eo:cloud_cover": {"lt": max_cloud_cover}},
            }
            chunk_items = self._search_with_retry(search_params)
            all_stac_items.extend(chunk_items)

        return all_stac_items
