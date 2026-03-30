"""Unit tests for MathEngine._calculate_indices() and _scale_dataset()"""

from unittest.mock import MagicMock
import numpy as np
import pytest
import xarray as xr
from src.math_engine import MathEngine


def _make_dataset(**bands) -> xr.Dataset:
    """Create a minimal xr.Dataset from 2D numpy arrays."""
    return xr.Dataset({name: xr.DataArray(data) for name, data in bands.items()})


# --- _calculate_indices ---


class TestCalculateIndices:
    def test_ndvi_simple_value(self):
        engine = MathEngine(indices=["NDVI"])
        # nir=0.8, red=0.2 → NDVI = (0.8-0.2)/(0.8+0.2) = 0.6
        ds = _make_dataset(nir=np.array([[0.8]]), red=np.array([[0.2]]))
        result = engine._calculate_indices(ds)
        assert "NDVI" in result
        np.testing.assert_allclose(result["NDVI"].values, [[0.6]])

    def test_ndwi_simple_value(self):
        engine = MathEngine(indices=["NDWI"])
        # green=0.3, nir=0.5 → NDWI = (0.3-0.5)/(0.3+0.5) = -0.25
        ds = _make_dataset(green=np.array([[0.3]]), nir=np.array([[0.5]]))
        result = engine._calculate_indices(ds)
        assert "NDWI" in result
        np.testing.assert_allclose(result["NDWI"].values, [[-0.25]])

    def test_ndvi_range_all_valid(self):
        """NDVI must be in [-1, 1] for typical reflectance values."""
        engine = MathEngine(indices=["NDVI"])
        rng = np.random.default_rng(42)
        nir = rng.uniform(0.0, 1.0, (10, 10))
        red = rng.uniform(0.0, 1.0, (10, 10))
        ds = _make_dataset(nir=nir, red=red)
        result = engine._calculate_indices(ds)
        ndvi = result["NDVI"].values
        # Values outside [-1, 1] indicate an incorrect formula
        valid = ~np.isnan(ndvi)
        assert np.all(ndvi[valid] >= -1.0)
        assert np.all(ndvi[valid] <= 1.0)

    def test_both_indices_computed(self):
        engine = MathEngine(indices=["NDVI", "NDWI"])
        ds = _make_dataset(
            nir=np.array([[0.6]]),
            red=np.array([[0.1]]),
            green=np.array([[0.2]]),
        )
        result = engine._calculate_indices(ds)
        assert "NDVI" in result
        assert "NDWI" in result

    def test_ndvi_not_computed_when_not_in_indices(self):
        engine = MathEngine(indices=["NDWI"])
        ds = _make_dataset(
            nir=np.array([[0.6]]),
            red=np.array([[0.1]]),
            green=np.array([[0.2]]),
        )
        result = engine._calculate_indices(ds)
        assert "NDVI" not in result
        assert "NDWI" in result

    def test_ndwi_not_computed_when_not_in_indices(self):
        engine = MathEngine(indices=["NDVI"])
        ds = _make_dataset(
            nir=np.array([[0.6]]),
            red=np.array([[0.1]]),
            green=np.array([[0.2]]),
        )
        result = engine._calculate_indices(ds)
        assert "NDWI" not in result
        assert "NDVI" in result

    def test_ndvi_missing_band_not_computed(self):
        """If the 'nir' band is missing, NDVI should not be computed."""
        engine = MathEngine(indices=["NDVI"])
        ds = _make_dataset(red=np.array([[0.2]]))
        result = engine._calculate_indices(ds)
        assert "NDVI" not in result

    def test_ndvi_bare_soil_near_zero(self):
        """Bare soil: nir ≈ red → NDVI close to 0."""
        engine = MathEngine(indices=["NDVI"])
        ds = _make_dataset(nir=np.array([[0.3]]), red=np.array([[0.3]]))
        result = engine._calculate_indices(ds)
        np.testing.assert_allclose(result["NDVI"].values, [[0.0]], atol=1e-6)

    def test_ndvi_dense_vegetation_positive(self):
        """Dense vegetation: nir >> red → NDVI strongly positive."""
        engine = MathEngine(indices=["NDVI"])
        ds = _make_dataset(nir=np.array([[0.9]]), red=np.array([[0.05]]))
        result = engine._calculate_indices(ds)
        assert float(result["NDVI"].values) > 0.8


# --- _scale_dataset ---


class TestScaleDataset:
    def _make_item(self, scale: float = 0.0001, offset: float = 0.0) -> MagicMock:
        item = MagicMock()
        item.assets["red"].extra_fields = {
            "raster:bands": [{"scale": scale, "offset": offset}]
        }
        return item

    def test_scale_applied_to_spectral_bands(self):
        engine = MathEngine(indices=["NDVI"])
        raw_values = np.array([[10000.0]])
        ds = _make_dataset(red=raw_values, nir=raw_values, scl=np.array([[4]]))
        items = [self._make_item(scale=0.0001, offset=0.0)]
        result = engine._scale_dataset(ds, items)
        np.testing.assert_allclose(result["red"].values, [[1.0]])
        np.testing.assert_allclose(result["nir"].values, [[1.0]])

    def test_offset_applied(self):
        engine = MathEngine(indices=["NDVI"])
        raw_values = np.array([[10000.0]])
        ds = _make_dataset(red=raw_values, nir=raw_values, scl=np.array([[4]]))
        items = [self._make_item(scale=0.0001, offset=-0.1)]
        result = engine._scale_dataset(ds, items)
        np.testing.assert_allclose(result["red"].values, [[0.9]], rtol=1e-5)

    def test_scl_preserved_unchanged(self):
        """The SCL band must not be scaled."""
        engine = MathEngine(indices=["NDVI"])
        scl_values = np.array([[4, 5], [0, 4]])
        ds = _make_dataset(
            red=np.array([[100.0, 200.0], [300.0, 400.0]]), scl=scl_values
        )
        items = [self._make_item(scale=0.0001, offset=0.0)]
        result = engine._scale_dataset(ds, items)
        np.testing.assert_array_equal(result["scl"].values, scl_values)

    def test_default_scale_used_when_metadata_absent(self):
        """If raster:bands metadata is absent, scale defaults to 0.0001."""
        engine = MathEngine(indices=["NDVI"])
        item = MagicMock()
        item.assets["red"].extra_fields = {"raster:bands": [{}]}  # no scale/offset
        raw_values = np.array([[10000.0]])
        ds = _make_dataset(red=raw_values, scl=np.array([[4]]))
        result = engine._scale_dataset(ds, [item])
        np.testing.assert_allclose(result["red"].values, [[1.0]])
