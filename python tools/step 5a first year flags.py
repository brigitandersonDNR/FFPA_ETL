#need to do: add try/exept handling, add messages

import arcpy
arcpy.env.overwriteOutput = True

#Get Variables
county = arcpy.GetParameter(0)
year= arcpy.GetParameter(1)
county_parcel_num_field = arcpy.GetParameterAsText(2)

#Set Variables
ffpa_county = "{0}_state".format(county)
state_boundaries = r"\\dnr\divisions\RP_GIS\projects\ffpa_ETL\data and tools\standard input data\input_analysis_data.gdb\state_land_ownership"
DNR_boundaries=r"\\dnr\divisions\RP_GIS\projects\ffpa_ETL\data and tools\standard input data\input_analysis_data.gdb\dnr_land_ownership"
fed_boundaries = r"\\dnr\divisions\RP_GIS\projects\ffpa_ETL\data and tools\standard input data\input_analysis_data.gdb\federal_land_ownership"
city_boundaries = r"\\dnr\divisions\RP_GIS\projects\ffpa_ETL\data and tools\standard input data\input_analysis_data.gdb\cities_UGA"
forest_prot_zone_boundaries = r"\\dnr\divisions\RP_GIS\projects\ffpa_ETL\data and tools\standard input data\input_analysis_data.gdb\forest_protection_zones"

#Create Flag Where Clauses
city_where_clause = "'PRCL_OWNER_NM' NOT LIKE 'CITY OF'"
dnr_where_clause = "'PRCL_OWNER_NM' NOT LIKE  'DNR' AND 'PRCL_OWNER_NM' NOT LIKE  'Department of Natural Resources'"
state_where_clause = "'PRCL_OWNER_NM' NOT LIKE  'STATE OF' AND 'PRCL_OWNER_NM' NOT LIKE  'WASHINGTON STATE' AND 'PRCL_OWNER_NM' NOT LIKE  'WDFW'"
fed_where_clause= "'PRCL_OWNER_NM' NOT LIKE  'America' AND 'PRCL_OWNER_NM' NOT LIKE  'UNITED STATES' AND 'PRCL_OWNER_NM' NOT LIKE  'USFS'"
county_data_null_clause = "{0} IS NULL".format(county_parcel_num_field)
duplicate_geometry_clause = "'IN_FID' IS NOT NULL"


#City Overlap Flag
arcpy.SelectLayerByLocation_management(ffpa_county, "WITHIN", city_boundaries, selection_type="NEW_SELECTION")
arcpy.SelectLayerByAttribute_management(ffpa_county, "SUBSET_SELECTION", city_where_clause)
arcpy.management.CalculateField(ffpa_county, "FLAG_TY_CD", "!FLAG_TY_CD! + '; 14'", "PYTHON3")

#Federal Overlap Flag
arcpy.SelectLayerByLocation_management(ffpa_county, "WITHIN", fed_boundaries, selection_type="NEW_SELECTION")
arcpy.SelectLayerByAttribute_management(ffpa_county, "SUBSET_SELECTION", fed_where_clause)
arcpy.management.CalculateField(ffpa_county, "FLAG_TY_CD", "!FLAG_TY_CD! + '; 7'", "PYTHON3")

#Non-DNR State Overlap Flag
arcpy.SelectLayerByLocation_management(ffpa_county, "WITHIN", state_boundaries, selection_type="NEW_SELECTION")
arcpy.SelectLayerByAttribute_management(ffpa_county, "SUBSET_SELECTION", state_where_clause)
arcpy.management.CalculateField(ffpa_county, "FLAG_TY_CD", "!FLAG_TY_CD! + '; 8'", "PYTHON3")

#DNR State Overlap Flag
arcpy.SelectLayerByLocation_management(ffpa_county, "WITHIN", DNR_boundaries, selection_type="NEW_SELECTION")
arcpy.SelectLayerByAttribute_management(ffpa_county, "SUBSET_SELECTION", dnr_where_clause)
arcpy.management.CalculateField(ffpa_county, "FLAG_TY_CD", "!FLAG_TY_CD! + '; 8'", "PYTHON3")

#Forest Protection Zone Overlap Flag
arcpy.SelectLayerByLocation_management(ffpa_county, "WITHIN", forest_prot_zone_boundaries, selection_type="NEW_SELECTION")
arcpy.management.CalculateField(ffpa_county, "FLAG_TY_CD", "!FLAG_TY_CD! + '; 9'", "PYTHON3")

#County/State Mismatch Flag - parcel exists in state but not county data
arcpy.SelectLayerByAttribute_management(ffpa_county, "NEW_SELECTION", county_data_null_clause)
arcpy.management.CalculateField(ffpa_county, "FLAG_TY_CD", "!FLAG_TY_CD! + '; 22'", "PYTHON3")

#Coincident Geometry Flag
arcpy.management.FindIdentical(ffpa_county, "coincident_shape", "Shape")
arcpy.management.MakeFeatureLayer(r"\\dnr\divisions\RP_GIS\projects\ffpa_ETL\county loads\{0}\{1}\aprx\ffpa_{0}_{1}\{0}_{1}.gdb\{0}_state".format(county, year),"join_layer")
arcpy.management.JoinField("join_layer", "OBJECTID", "coincident_shape", "IN_FID")
arcpy.SelectLayerByAttribute_management("join_layer", "NEW_SELECTION", duplicate_geometry_clause)
arcpy.management.CalculateField(ffpa_county, "FLAG_TY_CD", "!FLAG_TY_CD! + '; 23'", "PYTHON3")

#Clear All Selections
arcpy.SelectLayerByAttribute_management(ffpa_county, "CLEAR_SELECTION")