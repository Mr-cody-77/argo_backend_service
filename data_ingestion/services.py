import os
import re
import time
import random
import requests
import numpy as np
import xarray as xr
from urllib.parse import urljoin
from datetime import datetime, timedelta
from django.db import transaction
from .models import ArgoProfile, ArgoMeasurement # Assuming models are in the same app
import logging
import io
import pandas as pd

logger = logging.getLogger(__name__)

# --- UTILITY FUNCTIONS ---

def list_links(url, pattern=None, retries=3, backoff=2):
    """Fetches links from a directory URL with retry logic."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            links = re.findall(r'href="([^"]+)"', resp.text)
            if pattern:
                links = [l for l in links if re.search(pattern, l)]
            return links
        except requests.exceptions.RequestException as e:
            wait = backoff * (2 ** attempt) + random.random()
            logger.warning(f"⚠️ Error fetching {url} (attempt {attempt+1}/{retries}): {e}. Retrying in {wait:.1f}s...")
            time.sleep(wait)
    logger.error(f"❌ Failed to fetch {url} after {retries} retries.")
    return []

def recursive_nc_files(base_url, limit=None):
    """Recursively crawls ARGO float directories for .nc files."""
    count = 0
    # Use a minimal list_links here to find float directories (e.g., '1901820/')
    float_dirs = list_links(base_url, r'^[0-9]+/$') 
    
    for fdir in float_dirs:
        fdir_url = urljoin(base_url, fdir)
        
        # Check the main float directory for profile files (e.g., 1901820_prof.nc)
        for fname in list_links(fdir_url, r'\_prof\.nc$'):
            full_url = urljoin(fdir_url, fname)
            yield full_url
            count += 1
            if limit is not None and count >= limit: return

        # Check the 'profiles/' subdirectory common in some GDAC structures
        if "profiles/" in list_links(fdir_url):
            prof_url = urljoin(fdir_url, "profiles/")
            for fname in list_links(prof_url, r'\.nc$'):
                full_url = urljoin(prof_url, fname)
                yield full_url
                count += 1
                if limit is not None and count >= limit: return

def decode_bytes(x):
    """Decodes NetCDF byte strings to standard Python strings."""
    if isinstance(x, (bytes, np.bytes_)):
        try:
            return x.decode("utf-8").strip()
        except UnicodeDecodeError:
            return x.decode("latin1").strip()
    return str(x).strip()

def safe_index(var, i=0):
    """Safely extracts a scalar value from an xarray variable, handling 0D arrays."""
    val = var.values
    if np.ndim(val) == 0:
        return val
    else:
        # Check if index i is valid
        if i < len(val):
            return val[i]
        else:
            # Handle out-of-bounds access gracefully
            return np.nan 
def julian_to_datetime(juld_val):
    """Converts Argo Julian Day (days since 1950-01-01) to a datetime object."""
    if isinstance(juld_val, np.ndarray):
        juld_val = juld_val.item()

    if juld_val is None or (isinstance(juld_val, float) and np.isnan(juld_val)):
        return None

    # Case 1: Already numpy.datetime64
    if isinstance(juld_val, np.datetime64):
        return pd.to_datetime(juld_val).to_pydatetime()

    # Case 2: Normal float (days since 1950-01-01)
    reference_date = datetime(1950, 1, 1)
    return reference_date + timedelta(days=float(juld_val))


# --- CORE INGESTION FUNCTIONS ---

def process_single_netcdf_file(file_url):
    """
    Downloads a single .nc file, extracts data, and saves it to the Django DB.
    Returns the number of measurements created.
    """
    logger.info(f"📂 Processing: {file_url}")
    try:
        response = requests.get(file_url, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Failed to download {file_url}: {e}")
        return 0

    measurements_to_create = []
    
    with xr.open_dataset(io.BytesIO(response.content), decode_timedelta=False) as ds:
        if "N_PROF" not in ds.sizes:
            logger.warning(f"⚠️ Skipping non-profile file: {file_url}")
            return 0
        
        n_profiles = ds.sizes.get("N_PROF", 1)
        profiles_created = 0
        
        for i in range(n_profiles):
            try:
                # 1. EXTRACT PROFILE METADATA
                platform_number = decode_bytes(safe_index(ds["PLATFORM_NUMBER"], i))
                cycle_number = int(safe_index(ds["CYCLE_NUMBER"], i))
                
                # ✅ FIX: Use DATA_CENTRE as identified in the log error
                data_centre_name = decode_bytes(safe_index(ds.get("DATA_CENTRE", [b'UNKNOWN']), i)) 
                
                # Argo files use a unique REF field composed of platform+cycle, but since 
                # DATA_CENTRE_REF is missing, we use a custom composite key for the check:
                composite_key = f"{platform_number}-{cycle_number}"

                # Check if profile already exists (skip if it does)
                if ArgoProfile.objects.filter(platform_number=platform_number, cycle_number=cycle_number).exists():
                    logger.info(f"➡️ Profile {composite_key} already exists. Skipping.")
                    continue
                
                # Get core data arrays (using .get() to handle missing adjusted fields gracefully)
                pres_arr = ds["PRES"].isel(N_PROF=i).values.flatten()
                temp_arr = ds.get("TEMP", np.full_like(pres_arr, np.nan)).isel(N_PROF=i).values.flatten()
                temp_adj_arr = ds.get("TEMP_ADJUSTED", np.full_like(pres_arr, np.nan)).isel(N_PROF=i).values.flatten()
                sal_arr = ds.get("PSAL", np.full_like(pres_arr, np.nan)).isel(N_PROF=i).values.flatten()
                sal_adj_arr = ds.get("PSAL_ADJUSTED", np.full_like(pres_arr, np.nan)).isel(N_PROF=i).values.flatten()

                # Get QC flags (using .get() to handle missing QC fields, default to '9')
                pres_qc_arr = ds.get("PRES_QC", np.full_like(pres_arr, b'9')).isel(N_PROF=i).values.flatten()
                temp_qc_arr = ds.get("TEMP_QC", np.full_like(pres_arr, b'9')).isel(N_PROF=i).values.flatten()
                psal_qc_arr = ds.get("PSAL_QC", np.full_like(pres_arr, b'9')).isel(N_PROF=i).values.flatten()
                
                # 2. CREATE ARGO PROFILE
                # NOTE: Since DATA_CENTRE_REF is missing, we use the custom key for the unique field 'data_centre'
                profile_obj = ArgoProfile.objects.create(
                    platform_number=platform_number,
                    cycle_number=cycle_number,
                    juld_date=julian_to_datetime(safe_index(ds["JULD"], i)),
                    latitude=float(safe_index(ds["LATITUDE"], i)),
                    longitude=float(safe_index(ds["LONGITUDE"], i)),
                    data_mode=decode_bytes(safe_index(ds["DATA_MODE"], i)),
                    data_centre_ref=composite_key
                )
                profiles_created += 1
                
                # 3. EXTRACT AND FLATTEN MEASUREMENTS
                for level in range(len(pres_arr)):
                    # Only store data if the pressure value is valid
                    if not np.isnan(pres_arr[level]):
                        
                        measurements_to_create.append(
                            ArgoMeasurement(
                                profile=profile_obj,
                                pressure=float(pres_arr[level]),
                                
                                temperature=float(temp_arr[level]) if not np.isnan(temp_arr[level]) else None,
                                temperature_adjusted=float(temp_adj_arr[level]) if not np.isnan(temp_adj_arr[level]) else None,
                                salinity=float(sal_arr[level]) if not np.isnan(sal_arr[level]) else None,
                                salinity_adjusted=float(sal_adj_arr[level]) if not np.isnan(sal_adj_arr[level]) else None,
                                
                                # Convert QC flag bytes/arrays to string
                                pres_qc=decode_bytes(pres_qc_arr[level]),
                                temp_qc=decode_bytes(temp_qc_arr[level]),
                                psal_qc=decode_bytes(psal_qc_arr[level]),
                            )
                        )
            except Exception as e:
                logger.error(f"❌ Error processing profile {i+1} in {file_url}: {e}")
                continue

    # 4. BULK INSERT MEASUREMENTS
    if measurements_to_create:
        ArgoMeasurement.objects.bulk_create(measurements_to_create)
        logger.info(f"✅ Created {profiles_created} new profiles and {len(measurements_to_create)} measurements from {file_url}.")
        return len(measurements_to_create)
    return 0

def coordinate_argo_ingestion(base_url):
    """
    Coordinates the ingestion, checking if the URL is a single file or a directory.
    This is the function called directly by the Django view (or Celery task).
    """
    ingested_measurements = 0
    nc_urls_to_process = []

    # 1. Determine the type of URL and get list of NC files
    if base_url.endswith(".nc"):
        # Case 1: Single file URL
        nc_urls_to_process.append(base_url)
    elif base_url.endswith("/"):
        # Case 2: Directory URL (crawl for all .nc files within it)
        try:
            for url in recursive_nc_files(base_url, limit=None):
                 nc_urls_to_process.append(url)
        except Exception as e:
            logger.error(f"Failed to crawl directory {base_url}: {e}")
            return 0
    else:
        logger.error(f"Invalid or unsupported URL format: {base_url}")
        return 0

    # 2. Process each found NetCDF URL
    for url in nc_urls_to_process:
        if "_prof.nc" in url or "/profiles/" in url:
            # Use transaction for atomicity (either the entire file saves, or nothing)
            with transaction.atomic():
                count = process_single_netcdf_file(url)
                ingested_measurements += count
            
    return ingested_measurements
