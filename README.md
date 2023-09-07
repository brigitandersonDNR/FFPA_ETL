# FFPA ETL

## Goal

The goal of these scripts are to create a modular process that can be run within ArcGIS Pro for the FFPA ETL process. These are a series of python tools that are loaded into an FFPA template that are then used to walk through the necessary steps to prepare the data for loading into the FFPA portal. 

## Script Details

### Step 1: Create Datasets

This script takes in 1) the provided excel of the parcel data from the county and 2) the state parcel layer to create a county specific shape and tabular dataset in the correct format for FFPA processing. 

### Step 2: QAQC

This script creates duplicate tables on the parcel numbers for both the spatial and tabular FFPA datasets. Duplicates are not allowed in the FFPA database and so this step then requires manual QAQC to remove and fix duplicates. Additional QAQC is required during the step to ensure that all necessary fields are present and in the correct format.

### Step 3: Field Pops

This script populates general fields in the FFPA spatial data that are unique to a county. This includes region code, county codes, and pulling data from the county provided tabular data into the FFPA spatial data layer. 

### Step 4: Exempt and Assess

This is the first script to make any assessment "decisions." It populates exemption status from the county provided data, makes assessment decisions based on DOR code, and makes recommendations based on NLCD land cover for each parcel. 

### Step 5A: First Year Flags

This script adds flag values for any year of data. 

### Step 5B: Second Year Flags and Logic

This script adds flags for parcel if it is a second year (or more) load into the FFPA parcel. It takes into account prior decisions and changes between the previous year loads. 

### Step 6: Export and Append

This script exports the FFPA data into an excel spreadsheet that then gets manually formatted and loaded into FFPA. It also appends the spatial data into the correct FFPA spatial view. 

### Step 7: Load Stage Tables

This script is the one provided by Timmons to load the FFPA tabular data. It currently does not work in ArcGIS Pro. 
