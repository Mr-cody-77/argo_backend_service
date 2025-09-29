import os
import re
import time
import random
import requests
import numpy as np
import xarray as xr
from urllib.parse import urljoin
from datetime import datetime, timedelta, timezone 
from django.db import transaction
from django.utils import timezone as django_timezone 
from .models import ArgoProfileData, ArgoMeasurement 
import logging
import io
import pandas as pd # Note: pandas is imported but not used, can be removed if not needed elsewhere

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
    # FIX: Regex for float_dirs should handle potential trailing slashes better or just use the given one
    float_dirs = list_links(base_url, r'^[0-9]+/$') 
    
    for fdir in float_dirs:
        fdir_url = urljoin(base_url, fdir)
        
        # Check for files directly in the float directory (old style)
        for fname in list_links(fdir_url, r'\_prof\.nc$'):
            full_url = urljoin(fdir_url, fname)
            yield full_url
            count += 1
            if limit is not None and count >= limit: return

        # Check for the 'profiles/' subdirectory (new style)
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
        if i < len(val):
            return val[i]
        else:
            # Return NaN for floats/ints, or None/empty string for object types like strings/bytes
            if np.issubdtype(val.dtype, np.number):
                return np.nan
            return None # Or "" based on expected default for string/object
            
def julian_to_datetime(juld_val):
    """
    Converts Argo Julian Day (days since 1950-01-01) OR misinterpreted epoch-based values
    into a timezone-aware datetime (UTC).

    Handles:
    1. Standard Argo JULD: float days since 1950-01-01.
    2. Large numbers (~1e18): nanoseconds since UNIX epoch.
    3. Large numbers (~1e12-1e15): milliseconds or microseconds since UNIX epoch.
    """
    if isinstance(juld_val, np.ndarray):
        juld_val = juld_val.item()

    if juld_val is None:
        return None

    try:
        juld_numeric = float(juld_val)
    except (ValueError, TypeError):
        logger.error(f"Cannot convert JULD value {juld_val} to float.")
        return None

    if np.isnan(juld_numeric):
        return None

    reference_date = datetime(1950, 1, 1)

    # --- CASE 1: Standard Argo JULD (days since 1950-01-01) ---
    if 0 <= juld_numeric <= 50000:  # valid range until ~2087
        try:
            naive_dt = reference_date + timedelta(days=juld_numeric)
            return django_timezone.make_aware(naive_dt, timezone.utc)
        except OverflowError:
            logger.error(f"❌ Overflow in timedelta for JULD={juld_numeric}")
            return None

    # --- CASE 2: Nanoseconds since UNIX epoch (common with 1e18 scale) ---
    if juld_numeric > 1e17:  
        try:
            naive_dt = datetime.utcfromtimestamp(juld_numeric / 1e9)
            logger.info(f"🔄 Interpreted JULD={juld_numeric:.2e} as ns since epoch → {naive_dt}")
            return django_timezone.make_aware(naive_dt, timezone.utc)
        except Exception as e:
            logger.error(f"❌ Failed nanosecond conversion for {juld_numeric}: {e}")
            return None

    # --- CASE 3: Microseconds / Milliseconds since epoch ---
    if 1e12 < juld_numeric < 1e17:  
        scale = 1e3 if juld_numeric < 1e14 else 1e6
        try:
            naive_dt = datetime.utcfromtimestamp(juld_numeric / scale)
            logger.info(f"🔄 Interpreted JULD={juld_numeric:.2e} as epoch/{int(scale)} → {naive_dt}")
            return django_timezone.make_aware(naive_dt, timezone.utc)
        except Exception as e:
            logger.error(f"❌ Failed scaled epoch conversion for {juld_numeric}: {e}")
            return None

    logger.warning(f"⚠️ JULD value {juld_numeric} could not be interpreted")
    return None



# --- Ocean Lookup ---
OCEAN_COORDS = {
    "Pacific Ocean": {"lat": 0, "lon": -160},
    "Atlantic Ocean": {"lat": 0, "lon": -30},
    "Indian Ocean": {"lat": -20, "lon": 80},
    "Southern Ocean": {"lat": -60, "lon": 0},
    "Arctic Ocean": {"lat": 75, "lon": 0},
    "Arabian Sea": {"lat": 15, "lon": 65},
    "Bay of Bengal": {"lat": 15, "lon": 90},
    "Mediterranean Sea": {"lat": 35, "lon": 18},
    "Caribbean Sea": {"lat": 15, "lon": -75},
    "Bering Sea": {"lat": 60, "lon": -180}
}

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat / 2)**2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon / 2)**2
    # Small correction for arctan2 for better numerical stability (though arcsin is fine)
    return 2 * R * np.arcsin(np.sqrt(a))

def get_nearest_ocean(lat, lon):
    """Determines the nearest defined ocean or sea name based on coordinates."""
    if np.isnan(lat) or np.isnan(lon):
        return "Unknown"
    nearest, min_dist = "Unknown", float("inf")
    # Ensure lat/lon are floats for haversine
    lat, lon = float(lat), float(lon)
    for ocean, coords in OCEAN_COORDS.items():
        d = haversine_distance(lat, lon, coords["lat"], coords["lon"])
        if d < min_dist:
            min_dist, nearest = d, ocean
    return nearest

def _extract_profile_array(ds, var_name, i):
    """
    Safely extracts an xarray variable array for profile 'i', handling single-profile files.
    """
    # Check if the variable exists and is not a scalar (often due to 'N_MEASUREMENT' dim)
    if var_name not in ds:
        raise KeyError(f"Variable {var_name} not found in dataset.")
        
    var = ds[var_name]

    # Handle the 'N_PROF' dimension: is it present and larger than 1?
    if 'N_PROF' in var.dims and var.sizes.get('N_PROF', 1) > 1:
        # Multi-profile file: use isel
        return var.isel(N_PROF=i).values.flatten()
    elif 'N_PROF' not in var.dims or var.sizes.get('N_PROF', 1) == 1:
        # Single-profile file (or variable only has N_LEVELS/N_MEASUREMENT): return array directly
        return var.values.flatten()
    else:
        # Should not happen, but safe fallback
        raise ValueError(f"Unexpected dimensions for {var_name}")


def get_array_or_default(ds, var_name, i, is_qc_flag=False):
    """
    Retrieves the array for a variable, or returns a default array of the correct size 
    if the variable is missing or the profile index is invalid.
    """
    pres_arr = None
    try:
        # 1. Get reference array size from pressure
        pres_arr = _extract_profile_array(ds, "PRES", i)
        array_size = len(pres_arr)
        # Determine the default dtype based on the pressure array's type if possible
        default_dtype = pres_arr.dtype
    except Exception as e:
        # If PRES is completely missing or unextractable, return small default array
        logger.debug(f"Could not extract reference PRES array: {e}. Returning size 1 default.")
        if is_qc_flag:
            return np.array([b'9'], dtype=np.bytes_)
        else:
            return np.array([np.nan], dtype=np.float64)


    # 2. Try to get the specific variable array
    try:
        arr = _extract_profile_array(ds, var_name, i)
        
        # 3. Handle length mismatch (crucial check for level data)
        if len(arr) != array_size:
            logger.debug(f"Variable {var_name} length mismatch ({len(arr)} vs {array_size}). Using NaN/9 default.")
            raise ValueError("Length mismatch.")
            
        return arr
        
    except (KeyError, ValueError, IndexError) as e:
        # 4. Fall through to default if extraction fails, variable is missing, or length mismatch occurs
        logger.debug(f"Failed to extract {var_name} for profile {i} (Error: {e.__class__.__name__}). Using default.")
        
        # Determine the appropriate default value and dtype
        if is_qc_flag:
            default_val = b'9'
            default_dtype = np.bytes_ # QC flags are usually byte strings
        else:
            default_val = np.nan
            # If the variable was found but failed extraction/size check, use its dtype if available
            if var_name in ds:
                 # Check the type of the variable itself, not the values
                 if np.issubdtype(ds[var_name].dtype, np.object_):
                     default_dtype = np.object_ # For string-like arrays
                 else:
                     default_dtype = ds[var_name].dtype
            else:
                 default_dtype = np.float64
                 
        return np.full(array_size, default_val, dtype=default_dtype)


# --- CORE INGESTION FUNCTIONS ---

def process_single_netcdf_file(file_content, file_source):
    """
    Processes a single NetCDF file (either from URL or upload) and saves data to Django DB.
    Returns: total_measurements_saved
    """
    logger.info(f"📂 Parsing file: {file_source}")

    # No need for measurements_to_create list outside the loop since we bulk_create per profile
    total_measurements_saved = 0
    
    try:
        # Use io.BytesIO to read content from memory
        with xr.open_dataset(io.BytesIO(file_content), decode_timedelta=False) as ds:
            
            # Determine profile count (safe default to 1 if N_PROF is not a dimension)
            n_profiles = ds.sizes.get("N_PROF", 1) if 'N_PROF' in ds.sizes else 1
            
            for i in range(n_profiles):
                try:
                    # 1. EXTRACT PROFILE METADATA & CHECK EXISTENCE
                    # Decode bytes for PLATFORM_NUMBER and DATA_MODE
                    platform_number_raw = safe_index(ds["PLATFORM_NUMBER"], i)
                    platform_number = decode_bytes(platform_number_raw) if platform_number_raw is not None else None
                    cycle_number_raw = safe_index(ds["CYCLE_NUMBER"], i)
                    cycle_number = int(cycle_number_raw) if cycle_number_raw is not None and not np.isnan(float(cycle_number_raw)) else -999 # Use sentinel
                    
                    if platform_number is None or platform_number == "" or cycle_number == -999:
                         logger.error(f"❌ Skipping profile {i+1} in {file_source}: Missing PLATFORM_NUMBER or CYCLE_NUMBER.")
                         continue
                         
                    composite_key = f"{platform_number}-{cycle_number}"

                    if ArgoProfileData.objects.filter(platform_number=platform_number, cycle_number=cycle_number).exists():
                        logger.info(f"➡️ Profile {composite_key} already exists. Skipping.")
                        continue
                    
                    # 2. Prepare Profile Data for Django DB
                    lat = float(safe_index(ds["LATITUDE"], i))
                    lon = float(safe_index(ds["LONGITUDE"], i))
                    ocean_name = get_nearest_ocean(lat, lon)
                    juld_date = julian_to_datetime(safe_index(ds["JULD"], i))
                    
                    data_mode_raw = safe_index(ds["DATA_MODE"], i)
                    data_mode = decode_bytes(data_mode_raw) if data_mode_raw is not None else "R" # Default to Real-Time

                    # Get data arrays robustly
                    pres_arr = get_array_or_default(ds, "PRES", i)
                    temp_arr = get_array_or_default(ds, "TEMP", i)
                    temp_adj_arr = get_array_or_default(ds, "TEMP_ADJUSTED", i)
                    sal_arr = get_array_or_default(ds, "PSAL", i)
                    sal_adj_arr = get_array_or_default(ds, "PSAL_ADJUSTED", i)

                    # Get QC flags robustly (ensure these are byte arrays or string arrays for decoding)
                    pres_qc_arr = get_array_or_default(ds, "PRES_QC", i, is_qc_flag=True)
                    temp_qc_arr = get_array_or_default(ds, "TEMP_QC", i, is_qc_flag=True)
                    psal_qc_arr = get_array_or_default(ds, "PSAL_QC", i, is_qc_flag=True)
                    
                    # 3. Save Profile to Django DB
                    with transaction.atomic():
                        profile_obj = ArgoProfileData.objects.create(
                            platform_number=platform_number,
                            cycle_number=cycle_number,
                            juld_date=juld_date,
                            latitude=lat,
                            longitude=lon,
                            ocean_name=ocean_name, 
                            data_mode=data_mode,
                            data_centre_ref=composite_key # Use the composite key for unique reference
                        )
                        
                        # 4. Extract and Flatten Measurements for Django DB
                        current_measurements = []
                        # The length of all arrays should be the same here (due to get_array_or_default logic)
                        for level in range(len(pres_arr)):
                            # Only save measurement if pressure is a valid (non-NaN) value
                            if not np.isnan(pres_arr[level]):
                                
                                # Use decode_bytes on QC flags before assignment
                                pres_qc = decode_bytes(pres_qc_arr[level])
                                temp_qc = decode_bytes(temp_qc_arr[level])
                                psal_qc = decode_bytes(psal_qc_arr[level])
                                
                                # Set data values to None if NaN
                                temp_val = float(temp_arr[level]) if not np.isnan(temp_arr[level]) else None
                                temp_adj_val = float(temp_adj_arr[level]) if not np.isnan(temp_adj_arr[level]) else None
                                sal_val = float(sal_arr[level]) if not np.isnan(sal_arr[level]) else None
                                sal_adj_val = float(sal_adj_arr[level]) if not np.isnan(sal_adj_arr[level]) else None

                                current_measurements.append(
                                    ArgoMeasurement(
                                        profile=profile_obj,
                                        pressure=float(pres_arr[level]),
                                        temperature=temp_val,
                                        temperature_adjusted=temp_adj_val,
                                        salinity=sal_val,
                                        salinity_adjusted=sal_adj_val,
                                        pres_qc=pres_qc,
                                        temp_qc=temp_qc,
                                        psal_qc=psal_qc,
                                    )
                                )
                        
                        if current_measurements:
                            # Use batch size for very large profiles to prevent a single huge transaction
                            ArgoMeasurement.objects.bulk_create(current_measurements, batch_size=5000)
                            total_measurements_saved += len(current_measurements)
                            logger.info(f"✅ Saved {len(current_measurements)} measurements for profile {composite_key}")

                except Exception as e:
                    logger.error(f"❌ Error processing profile {i+1} in {file_source}: {e}", exc_info=True)
                    # Continue to next profile in multi-profile file
                    continue
            
            return total_measurements_saved

    except Exception as e:
        logger.error(f"❌ Failed to parse and save {file_source}: {e}", exc_info=True)
        return 0

# --- REMAINING FUNCTIONS (Unchanged, as they were correct) ---

def coordinate_argo_ingestion(base_url):
    """
    Coordinates the ingestion from a URL (single file or directory crawl).
    Handles downloading and Django DB save.
    """
    ingested_measurements = 0
    nc_urls_to_process = []

    if base_url.endswith(".nc"):
        nc_urls_to_process.append(base_url)
    elif base_url.endswith("/"):
        try:
            for url in recursive_nc_files(base_url, limit=None):
                 nc_urls_to_process.append(url)
        except Exception as e:
            logger.error(f"Failed to crawl directory {base_url}: {e}")
            return 0
    else:
        logger.error(f"Invalid or unsupported URL format: {base_url}")
        return 0

    
    for url in nc_urls_to_process:
        if "_prof.nc" in url or "/profiles/" in url:
            try:
                # 1. Download file content
                response = requests.get(url, timeout=60)
                response.raise_for_status()
                file_content = response.content
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Failed to download {url}: {e}")
                continue
            
            # 2. Process and save to Django DB
            measurements_saved = process_single_netcdf_file(file_content, url)
            ingested_measurements += measurements_saved
            
    return ingested_measurements
    
def process_uploaded_netcdf_file(uploaded_file):
    """
    Processes an uploaded .nc file from Django (request.FILES["file"]).
    Saves data to Django DB.
    """
    uploaded_file.seek(0)
    file_content = uploaded_file.read()
    file_source = f"Uploaded File: {uploaded_file.name}"
    
    # Process and save to Django DB
    measurements_saved = process_single_netcdf_file(file_content, file_source)
            
    return measurements_saved