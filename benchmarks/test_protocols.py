"""Benchmarks for GAIA DR3 catalog access across protocols and storage locations.

Protocols
---------
- http:    Public HTTPS endpoint (UW). No credentials required. Uses standard
           HTTP range-requests to fetch Parquet row-groups; latency per request
           is the main cost.

- aws:     Public S3 bucket (AWS). No credentials required. Uses anonymous
           S3 access via ``s3fs``; performance depends on egress and region.

- xrootd:  USDF internal. Requires the ``xrootd`` Python package and a valid
           SLAC grid certificate or token. Uses the XRootD protocol for
           efficient streaming over the SLAC network.

- webdav:  USDF internal. Requires no special credentials. Uses plain HTTP
           range-requests via the USDF WebDAV endpoint; expect performance
           comparable to xrootd on the internal network.
"""

import pytest
import lsdb

GAIA_URLS = {
    "http": "https://data.lsdb.io/hats/gaia_dr3",
    "aws": "s3://stpubdata/gaia/gaia_dr3/public/hats",
    "xrootd": "root://sdfdtn005.slac.stanford.edu:1094//lsdb/gaia_dr3",
    "webdav": "http://sdfdtn005.slac.stanford.edu:1094/lsdb/gaia_dr3",
}
all_protocols = list(GAIA_URLS.keys())


# ── Test 1: Open catalog ──────────────────────────────────────────────────────


@pytest.mark.parametrize("protocol", all_protocols)
def test_open_catalog(lbench, protocol):
    """Open GAIA DR3 (reads catalog metadata only)."""
    lbench(lambda: lsdb.open_catalog(GAIA_URLS[protocol]))


@pytest.mark.parametrize("protocol", all_protocols)
def test_open_catalog_radec(lbench, protocol):
    """Open GAIA DR3 selecting only ra/dec columns."""
    lbench(lambda: lsdb.open_catalog(GAIA_URLS[protocol], columns=["ra", "dec"]))


# ── Test 2: Open catalog and compute second partition ─────────────────────────


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


# ── Test 3/4: Crossmatch GAIA at USDF with external GAIA ──────────────────────


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
        gaia_int = lsdb.open_catalog(GAIA_URLS[internal_protocol], columns=["ra", "dec"], search_filter=cone)
        gaia_ext = lsdb.open_catalog(GAIA_URLS[external_protocol], columns=["ra", "dec"], search_filter=cone)
        xmatch = gaia_int.crossmatch(gaia_ext, radius_arcsec=1, suffixes=("_1", "_2"))
        xmatch.compute()

    lbench_dask(crossmatch)


# ── Test 5: Cone search with magnitude cut ────────────────────────────────────


@pytest.mark.parametrize("protocol", all_protocols)
def test_cone_search_magnitude_filter(lbench_dask, protocol):
    """One degree cone around (ra=0, dec=0) filtered to phot_g_mean_mag < 16."""

    def search_and_filter():
        gaia = lsdb.open_catalog(GAIA_URLS[protocol])
        cone = gaia.cone_search(ra=0.0, dec=0.0, radius_arcsec=3600)
        query = cone.query("phot_g_mean_mag < 16")
        query.compute()

    lbench_dask(search_and_filter)
