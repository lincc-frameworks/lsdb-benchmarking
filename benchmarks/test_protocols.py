"""
Benchmarks for catalog access across protocols and storage locations.

Protocols
---------
- http:    Public HTTPS endpoint (UW). No credentials required. Uses standard
           HTTP range-requests to fetch Parquet row-groups.

- aws:     Public S3 bucket (AWS). No credentials required. Uses anonymous S3
           access. Requires the ``s3fs`` Python package.

- xrootd:  On USDF. Uses the XRootD protocol for efficient streaming. Requires
           the ``xrootd`` and ``fsspec-xrootd`` Python packages and valid SLAC
           credentials.

- webdav:  On USDF. Uses plain HTTP range-requests via the WebDAV endpoint.
           Requires no credentials.
"""

import pytest
import lsdb

GAIA_URLS = {
    "http": "https://data.lsdb.io/hats/gaia_dr3",
    "aws": "s3://stpubdata/gaia/gaia_dr3/public/hats",
    "xrootd": "root://sdfdtn001.slac.stanford.edu:1094//lsdb/gaia_dr3",
    "webdav": "http://sdfdtn001.slac.stanford.edu:1094/lsdb/gaia_dr3",
}
all_protocols = list(GAIA_URLS.keys())


# Test 1: Open catalog


@pytest.mark.parametrize("protocol", all_protocols)
def test_open_catalog(lbench, protocol):
    """Open GAIA DR3 (reads catalog metadata only)."""
    lbench(lambda: lsdb.open_catalog(GAIA_URLS[protocol]))


@pytest.mark.parametrize("protocol", all_protocols)
def test_open_catalog_radec(lbench, protocol):
    """Open GAIA DR3 selecting only ra/dec columns."""
    lbench(lambda: lsdb.open_catalog(GAIA_URLS[protocol], columns=["ra", "dec"]))


# Test 2: Open catalog and compute second partition


@pytest.mark.parametrize("protocol", all_protocols)
def test_open_catalog_compute_partition(lbench, protocol):
    """Open the catalog and compute the 2nd partition of GAIA."""

    def open_and_compute():
        cat = lsdb.open_catalog(GAIA_URLS[protocol])
        partition = cat.partitions[1]
        partition.compute()

    lbench(open_and_compute)


@pytest.mark.parametrize("protocol", all_protocols)
def test_open_catalog_compute_partition_radec(lbench, protocol):
    """Open the catalog and compute the 2nd partition of GAIA, using only ra/dec."""

    def open_and_compute():
        cat = lsdb.open_catalog(GAIA_URLS[protocol], columns=["ra", "dec"])
        partition = cat.partitions[1]
        partition.compute()

    lbench(open_and_compute)


# Test 3/4: Crossmatch GAIA at USDF with external GAIA


@pytest.mark.parametrize("external_protocol", ["http", "aws"])
@pytest.mark.parametrize("internal_protocol", ["xrootd", "webdav"])
def test_crossmatch_usdf(lbench_dask, external_protocol, internal_protocol):
    """Crossmatch GAIA at USDF against an external GAIA."""

    def crossmatch():
        cone = lsdb.ConeSearch(ra=0.0, dec=0.0, radius_arcsec=3600)
        gaia_int = lsdb.open_catalog(GAIA_URLS[internal_protocol], search_filter=cone)
        gaia_ext = lsdb.open_catalog(GAIA_URLS[external_protocol], search_filter=cone)
        xmatch = gaia_int.crossmatch(gaia_ext, radius_arcsec=1, suffixes=("_1", "_2"))
        xmatch.compute()

    lbench_dask(crossmatch)


@pytest.mark.parametrize("external_protocol", ["http", "aws"])
@pytest.mark.parametrize("internal_protocol", ["xrootd", "webdav"])
def test_crossmatch_usdf_ra_dec(lbench_dask, external_protocol, internal_protocol):
    """Crossmatch GAIA at USDF against an external GAIA, with only ra/dec."""

    def crossmatch():
        cone = lsdb.ConeSearch(ra=0.0, dec=0.0, radius_arcsec=3600)
        gaia_int = lsdb.open_catalog(
            GAIA_URLS[internal_protocol], columns=["ra", "dec"], search_filter=cone
        )
        gaia_ext = lsdb.open_catalog(
            GAIA_URLS[external_protocol], columns=["ra", "dec"], search_filter=cone
        )
        xmatch = gaia_int.crossmatch(gaia_ext, radius_arcsec=1, suffixes=("_1", "_2"))
        xmatch.compute()

    lbench_dask(crossmatch)


# Test 5: Cone search with magnitude cut


@pytest.mark.parametrize("protocol", all_protocols)
def test_cone_search_magnitude_filter(lbench_dask, protocol):
    """One degree cone around (ra=0, dec=0) filtered to phot_g_mean_mag < 16."""

    def search_and_filter():
        gaia = lsdb.open_catalog(GAIA_URLS[protocol])
        cone = gaia.cone_search(ra=0.0, dec=0.0, radius_arcsec=3600)
        query = cone.query("phot_g_mean_mag < 16")
        query.compute()

    lbench_dask(search_and_filter)
