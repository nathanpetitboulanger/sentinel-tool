"""Unit tests for StacFetcher._split_date_range()"""

from unittest.mock import MagicMock, patch
import pytest
from src.stac_fetcher import StacFetcher


@pytest.fixture
def fetcher():
    """Instantiate StacFetcher without a real network call."""
    with patch("src.stac_fetcher.pystac_client.Client.open", return_value=MagicMock()):
        return StacFetcher("https://fake.stac/v1", "sentinel-2-c1-l2a")


# --- _split_date_range ---


def test_short_period_single_chunk(fetcher):
    chunks = fetcher._split_date_range("2023-01-01", "2023-06-30")
    assert len(chunks) == 1
    assert chunks[0] == ("2023-01-01", "2023-06-30")


def test_exactly_365_days_single_chunk(fetcher):
    chunks = fetcher._split_date_range("2023-01-01", "2023-12-31")
    assert len(chunks) == 1
    assert chunks[0] == ("2023-01-01", "2023-12-31")


def test_366_days_two_chunks(fetcher):
    # 2023-01-01 + timedelta(365) = 2024-01-01, so a period ending
    # on 2024-01-02 produces exactly 2 chunks.
    chunks = fetcher._split_date_range("2023-01-01", "2024-01-02")
    assert len(chunks) == 2
    assert chunks[0] == ("2023-01-01", "2024-01-01")
    assert chunks[1] == ("2024-01-02", "2024-01-02")


def test_two_full_years(fetcher):
    # 2022-01-01 + 365 = 2023-01-01 (2022 is not a leap year)
    # → Chunk 1: 2022-01-01 / 2023-01-01 ; Chunk 2: 2023-01-02 / 2023-12-31
    chunks = fetcher._split_date_range("2022-01-01", "2023-12-31")
    assert len(chunks) == 2
    assert chunks[0] == ("2022-01-01", "2023-01-01")
    assert chunks[1] == ("2023-01-02", "2023-12-31")


def test_same_start_and_end(fetcher):
    chunks = fetcher._split_date_range("2023-06-15", "2023-06-15")
    assert len(chunks) == 1
    assert chunks[0] == ("2023-06-15", "2023-06-15")


def test_chunks_are_contiguous_no_gaps(fetcher):
    """Verify there are no missing days between chunks."""
    from datetime import datetime, timedelta

    chunks = fetcher._split_date_range("2021-01-01", "2023-06-30")
    for i in range(len(chunks) - 1):
        end_of_chunk = datetime.strptime(chunks[i][1], "%Y-%m-%d")
        start_of_next = datetime.strptime(chunks[i + 1][0], "%Y-%m-%d")
        assert start_of_next == end_of_chunk + timedelta(days=1)


def test_chunks_cover_full_range(fetcher):
    """The first and last chunks cover exactly the requested period."""
    chunks = fetcher._split_date_range("2020-03-15", "2022-09-10")
    assert chunks[0][0] == "2020-03-15"
    assert chunks[-1][1] == "2022-09-10"


def test_three_year_period(fetcher):
    chunks = fetcher._split_date_range("2020-01-01", "2022-12-31")
    assert len(chunks) == 3
