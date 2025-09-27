from django.db import models

# --------------------------------------------------------------------------
# 1. ARGO PROFILE MODEL (The Header/Metadata Table)
# Stores unique data for each measurement profile (float, cycle, location).
# --------------------------------------------------------------------------

class ArgoProfile(models.Model):

    
    # Primary Identifiers
    platform_number = models.CharField(
        max_length=8, 
        db_index=True, 
        help_text="WMO identifier of the float (e.g., 1901820)"
    )
    cycle_number = models.IntegerField(
        db_index=True,
        help_text="Mission cycle number for the profile"
    )
    
    # Time and Location Data
    juld_date = models.DateTimeField(
        db_index=True, 
        help_text="Date and time of the profile measurement (UTC)"
    )
    latitude = models.FloatField(
        help_text="Best estimate of the profile's latitude"
    )
    longitude = models.FloatField(
        help_text="Best estimate of the profile's longitude"
    )

    # Data Status and Source
    data_mode = models.CharField(
        max_length=1, 
        help_text="Data type: R (Real-Time), D (Delayed Mode), or A (Adjusted)"
    )
    data_centre_ref = models.CharField(max_length=50, unique=True, null=True)
    class Meta:
        # Ensures that a combination of float and cycle number is always unique
        unique_together = ('platform_number', 'cycle_number')
        ordering = ['platform_number', 'cycle_number']
        verbose_name = "ARGO Profile"
        verbose_name_plural = "ARGO Profiles"

    def __str__(self):
        return f"Float {self.platform_number} - Cycle {self.cycle_number}"

# --------------------------------------------------------------------------
# 2. ARGO MEASUREMENT MODEL (The Level Data Table)
# Stores individual measurements for pressure, temperature, and salinity.
# --------------------------------------------------------------------------

class ArgoMeasurement(models.Model):
    """
    Represents a single depth level measurement within a specific ArgoProfile.
    Corresponds to the N_LEVELS dimension in the NetCDF file.
    """
    
    # Foreign Key linking back to the profile
    profile = models.ForeignKey(
        ArgoProfile, 
        on_delete=models.CASCADE, 
        related_name='measurements',
        help_text="The profile this measurement belongs to"
    )
    
    # Measured Variables
    pressure = models.FloatField(
        db_index=True, 
        help_text="Sea water pressure (dbar), which serves as the depth index"
    )
    
    temperature = models.FloatField(
        null=True, 
        blank=True,
        help_text="Unadjusted sea temperature in-situ (°C)"
    )
    temperature_adjusted = models.FloatField(
        null=True, 
        blank=True,
        help_text="Adjusted sea temperature in-situ (°C)"
    )
    
    salinity = models.FloatField(
        null=True, 
        blank=True,
        help_text="Unadjusted practical salinity (psu)"
    )
    salinity_adjusted = models.FloatField(
        null=True, 
        blank=True,
        help_text="Adjusted practical salinity (psu)"
    )
    
    # Quality Control Flags
    pres_qc = models.CharField(
        max_length=1, 
        null=True, 
        blank=True,
        help_text="Quality flag for pressure data"
    )
    temp_qc = models.CharField(
        max_length=1, 
        null=True, 
        blank=True,
        help_text="Quality flag for temperature data"
    )
    psal_qc = models.CharField(
        max_length=1, 
        null=True, 
        blank=True,
        help_text="Quality flag for salinity data"
    )
    
    class Meta:
        # Ensures that for any given profile, the pressure level is unique
        unique_together = ('profile', 'pressure')
        ordering = ['profile', 'pressure']
        verbose_name = "ARGO Measurement"
        verbose_name_plural = "ARGO Measurements"

    def __str__(self):
        return f"Profile {self.profile.id} @ {self.pressure} dbar"