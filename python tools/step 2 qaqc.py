#need to do: add in join and select, add try/exept handling, add messages - add in more QA/QC steps??

import arcpy

#Get Variables
county = arcpy.GetParameter(0)
county_parcel_field = arcpy.GetParameter(1)


#Find duplicate parcel numbers in both county and state for manual QA/QC
arcpy.management.FindIdentical("{0}_county".format(county), "county_duplicates", county_parcel_field, output_record_option="ONLY_DUPLICATES")
arcpy.management.FindIdentical("{0}_state".format(county), "state_duplicates", "PRCL_REVW_ORIGINAL_PRCL_ID", output_record_option="ONLY_DUPLICATES")

#required QC/QC: no duplicates, all have correct field type to match to state, all fields exist