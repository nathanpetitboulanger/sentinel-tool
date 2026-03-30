"""Unit tests for src/config.py :: _validate_config()"""

import pytest
from src.config import _validate_config


def _base_cfg() -> dict:
    """Minimal valid config."""
    return {
        "stac_api_url": "https://example.com/stac",
        "collection": "sentinel-2-c1-l2a",
        "indices": ["NDVI"],
        "dask": {"n_workers": 4, "threads_per_worker": 10},
    }


# --- Valid cases ---


def test_valid_minimal_config():
    _validate_config(_base_cfg())


def test_defaults_are_set():
    cfg = _base_cfg()
    _validate_config(cfg)
    assert cfg["save_zarr"] is False
    assert cfg["debug_cloud"] is False
    assert cfg["batch_size_deg"] == 0.5


def test_valid_with_both_indices():
    cfg = _base_cfg()
    cfg["indices"] = ["NDVI", "NDWI"]
    _validate_config(cfg)


def test_valid_explicit_save_zarr_true():
    cfg = _base_cfg()
    cfg["save_zarr"] = True
    _validate_config(cfg)


def test_valid_custom_batch_size():
    cfg = _base_cfg()
    cfg["batch_size_deg"] = 1.0
    _validate_config(cfg)


# --- Missing keys ---


@pytest.mark.parametrize(
    "missing_key", ["stac_api_url", "collection", "indices", "dask"]
)
def test_missing_required_key(missing_key):
    cfg = _base_cfg()
    del cfg[missing_key]
    with pytest.raises(ValueError, match=missing_key):
        _validate_config(cfg)


@pytest.mark.parametrize("sub_key", ["n_workers", "threads_per_worker"])
def test_missing_dask_sub_key(sub_key):
    cfg = _base_cfg()
    del cfg["dask"][sub_key]
    with pytest.raises(ValueError, match=sub_key):
        _validate_config(cfg)


# --- Wrong types ---


def test_stac_api_url_not_string():
    cfg = _base_cfg()
    cfg["stac_api_url"] = 42
    with pytest.raises(ValueError, match="stac_api_url"):
        _validate_config(cfg)


def test_collection_not_string():
    cfg = _base_cfg()
    cfg["collection"] = ["sentinel-2"]
    with pytest.raises(ValueError, match="collection"):
        _validate_config(cfg)


def test_indices_not_list():
    cfg = _base_cfg()
    cfg["indices"] = "NDVI"
    with pytest.raises(ValueError, match="indices"):
        _validate_config(cfg)


def test_save_zarr_not_bool():
    cfg = _base_cfg()
    cfg["save_zarr"] = "true"
    with pytest.raises(ValueError, match="save_zarr"):
        _validate_config(cfg)


def test_debug_cloud_not_bool():
    cfg = _base_cfg()
    cfg["debug_cloud"] = 1
    with pytest.raises(ValueError, match="debug_cloud"):
        _validate_config(cfg)


def test_dask_n_workers_not_int():
    cfg = _base_cfg()
    cfg["dask"]["n_workers"] = 2.5
    with pytest.raises(ValueError, match="n_workers"):
        _validate_config(cfg)


def test_dask_threads_not_int():
    cfg = _base_cfg()
    cfg["dask"]["threads_per_worker"] = "10"
    with pytest.raises(ValueError, match="threads_per_worker"):
        _validate_config(cfg)


# --- Invalid values ---


def test_empty_indices_list():
    cfg = _base_cfg()
    cfg["indices"] = []
    with pytest.raises(ValueError, match="empty"):
        _validate_config(cfg)


def test_unknown_index():
    cfg = _base_cfg()
    cfg["indices"] = ["NDVI", "EVI"]
    with pytest.raises(ValueError, match="EVI"):
        _validate_config(cfg)


def test_batch_size_zero():
    cfg = _base_cfg()
    cfg["batch_size_deg"] = 0
    with pytest.raises(ValueError, match="batch_size_deg"):
        _validate_config(cfg)


def test_batch_size_negative():
    cfg = _base_cfg()
    cfg["batch_size_deg"] = -0.5
    with pytest.raises(ValueError, match="batch_size_deg"):
        _validate_config(cfg)


def test_dask_n_workers_zero():
    cfg = _base_cfg()
    cfg["dask"]["n_workers"] = 0
    with pytest.raises(ValueError, match="n_workers"):
        _validate_config(cfg)


# --- Error accumulation ---


def test_multiple_errors_reported_together():
    """All errors are reported in a single exception."""
    cfg = {
        "stac_api_url": 123,  # wrong type
        "collection": "ok",
        "indices": [],  # empty
        "dask": {"n_workers": 0, "threads_per_worker": 4},  # zero
    }
    with pytest.raises(ValueError) as exc_info:
        _validate_config(cfg)
    msg = str(exc_info.value)
    assert "stac_api_url" in msg
    assert "empty" in msg
    assert "n_workers" in msg
