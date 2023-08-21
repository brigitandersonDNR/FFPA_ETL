#need to do: add try/exept handling, add messages, confirm archiving/second year

import arcpy
arcpy.env.overwriteOutput = True

#Get Variables
county = arcpy.GetParameter(0)
year= arcpy.GetParameter(1)
load_type = arcpy.GetParameter(2)
previous_year_load = arcpy.GetParameter(3)

#Set Variables
ffpa_county = "{0}_state".format(county)
county_append=r"\\dnr\divisions\RP_GIS\projects\ffpa_ETL\county loads\{0}\{1}\aprx\{0}_{1}.gdb\{0}_{1}_SPATIALappend".format(county, year)
FFPA_spatial = "FFPA.FFPA_PARCEL_SPATIAL"
archive_spatial = r"\\dnr\divisions\RP_GIS\projects\ffpa_ETL\data and tools\archived spatial\ffpa_spatial_archive.gdb\{0}_archive".format(county)
output_excel = r"\\dnr\divisions\RP_GIS\projects\ffpa_ETL\county loads\{0}\{1}\results\{0}Export.xlsx".format(county, year)


#Create Spatial FC for ROPA appending
for_append = arcpy.conversion.ExportFeatures(ffpa_county, county_append, field_mapping='PRCL_REVW_NORMALIZED_PRCL_ID "PRCL_REVW_NORMALIZED_PRCL_ID" true true false 24 Text 0 0,First,#,{0}_state,PRCL_REVW_NORMALIZED_PRCL_ID,0,24;RS_PRED_NM "RS_PRED_NM" true true false 255 Text 0 0,First,#,{0}_state,RS_PRED_NM,0,255'.format(county))

rename_fields = {
    "RS_PRED_NM":"RS_PRED_LABEL_NM",
    "PRCL_REVW_NORMALIZED_PRCL_ID":"PSL_NRML_PRCL_ID"
}

for key in rename_fields:
    arcpy.management.AlterField(for_append, key, rename_fields[key], rename_fields[key])

#If First Year Load, append new data
if load_type == "First Year":
    arcpy.management.Append(for_append, FFPA_spatial)



#Export and Archive First Year ROPA Shapes and Append New Data (Delete Old Data if second year)

#County Codes Lookup Table
county_codes = {
    "Adams":[1, 1, 41],
    "Asotin": [3, 2, 42],
    "Benton": [5, 3, 43],
    "Chelan": [7, 4, 44],
    "Clallum": [9, 5,45],
    "Clark": [11, 6, 46],
    "Columbia": [13, 7, 47],
    "Cowlitz": [15, 8, 48],
    "Douglas": [17, 9, 49],
    "Ferry": [19, 10, 50],
    "Franklin": [21, 11,51],
    "Garfield": [23, 12, 52],
    "Grant": [25, 13, 53],
    "Grays Harbor": [27, 14, 54],
    "Island County": [29, 15, 55],
    "Jefferson": [31, 16, 56],
    "King": [33, 17, 57],
    "Kitsap": [35, 18, 58],
    "Kittitas": [37, 19, 59],
    "Klickitat": [39, 20, 60],
    "Lewis": [41, 21, 61],
    "Lincoln": [43, 22, 62],
    "Mason": [45, 23, 63],
    "Okanogan": [47, 24, 64],
    "Pacific": [49, 25, 65],
    "Pend Oreille": [51, 26, 66],
    "Pierce": [53, 27, 67],
    "San Juan": [55, 28, 68],
    "Skagit": [57, 29, 69],
    "Skamania": [59, 30, 70],
    "Snohomish": [61, 31, 71],
    "Spokane": [63, 32, 72],
    "Stevens": [65, 33, 73],
    "Thurston": [67, 34, 74],
    "Wahkiakum": [69, 35, 75],
    "Walla-Walla": [71, 36, 76],
    "Whatcom": [73, 37, 77],
    "Whitman": [75, 38, 78],
    "Yakima": [77, 39, 79]
}

#Set County Code prefix
for key in county_codes:
    if key == county:
        county_prefix = county_codes[key][1]

#Calculate Previous Load to Old Year Suffix, Archive That Data, Append New Data, Delete Old Data
if load_type == "Not First Year":
    where_clause = 'PSL_NRML_PRCL_ID LIKE "{0}-%"'.format(county_prefix)
    arcpy.SelectLayerByAttribute_management(FFPA_spatial, "NEW_SELECTION", where_clause)
    arcpy.management.CalculateField(FFPA_spatial, "PSL_NRML_PRCL_ID LIKE", '!PSL_NRML_PRCL_ID! + "_{0}"'.format(previous_year_load))
    arcpy.conversion.ExportFeatures(FFPA_spatial, archive_spatial)
    arcpy.management.Append(for_append, FFPA_spatial)
    old_year_select = 'PSL_NRML_PRCL_ID LIKE "%{0}"'.format(previous_year_load)
    arcpy.SelectLayerByAttribute_management(FFPA_spatial, "NEW_SELECTION", old_year_select)
    arcpy.DeleteRows_management(FFPA_spatial)

 


#Export FFPA Data to Excel
arcpy.conversion.TableToExcel(ffpa_county, output_excel)