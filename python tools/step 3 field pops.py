## Need to do:  make script work even if one of those fields isn't in county data, add try/exept handling, add messages


import arcpy
arcpy.env.overwriteOutput = True

#Get Variables
county = arcpy.GetParameter(0)
year = arcpy.GetParameterAsText(1)
county_parcel_field = arcpy.GetParameterAsText(2)
county_total_acres_field=arcpy.GetParameterAsText(3)
county_land_value_field=arcpy.GetParameterAsText(4)
county_improved_land_value_field=arcpy.GetParameterAsText(5)
county_unimproved_acres_field=arcpy.GetParameterAsText(6)


#Set Variables
regions_fc = r"\\dnr\divisions\RP_GIS\projects\ffpa_ETL\data and tools\standard input data\input_analysis_data.gdb\DNR_regions"
ffpa_county = "{0}_state".format(county)
county_input_data = "{0}_county".format(county)


#Update DNR Region Code
#ffpa_county MUST be added to map to populate correctly
region_codes = {"Northeast":23, "Southeast":1, "Pacific Cascade":4, "Olympic":2, "Northwest":19, "South Puget Sound":9}
field_name = "JURISDICT_LABEL_NM"

for region in region_codes:
    rgn_cd = region_codes[region]
    rgn_nm = region
    where_clause = '"' + field_name + '" = ' + "'" + rgn_nm + "'"
    region_select = arcpy.SelectLayerByAttribute_management(regions_fc, "NEW_SELECTION", where_clause)
    arcpy.SelectLayerByLocation_management(ffpa_county, "WITHIN", region_select, selection_type="NEW_SELECTION")
    arcpy.management.CalculateField(ffpa_county, "RGN_CD", rgn_cd, "PYTHON3")
    arcpy.SelectLayerByAttribute_management(ffpa_county, "CLEAR_SELECTION")
    arcpy.SelectLayerByAttribute_management(regions_fc, "CLEAR_SELECTION")


#County Code Look Up Table
#County Name: [fips code, FFPA code, FFPA county extension]
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

#Update county codes
for key in county_codes:
    if key == county:
        #Parcel Review Normalized ID - needs to have the county code as the prefix (not the FIPS code)
        arcpy.management.CalculateField(ffpa_county, "PRCL_REVW_NORMALIZED_PRCL_ID", "!PRCL_REVW_NORMALIZED_PRCL_ID!.replace('{0}-','{1}-')".format(county_codes[key][0], county_codes[key][1]), "PYTHON3")
        arcpy.management.CalculateField(ffpa_county, "PRCL_REVW_NORMALIZED_PRCL_ID", "!PRCL_REVW_NORMALIZED_PRCL_ID!.lstrip('0')", "PYTHON3")
        #Update county extention ID
        arcpy.management.CalculateField(ffpa_county, "FFPA_CO_EXT_ID", "{0}".format(county_codes[key][2]), "PYTHON3")



##Calculate Values from joining county to state data

#Join Data
arcpy.management.MakeFeatureLayer(r"\\dnr\divisions\RP_GIS\projects\ffpa_ETL\county loads\{0}\{1}\aprx\ffpa_{0}_{1}\{0}_{1}.gdb\{0}_state".format(county, year),"join_layer")
arcpy.management.JoinField("join_layer", "PRCL_REVW_ORIGINAL_PRCL_ID", r"\\dnr\divisions\RP_GIS\projects\ffpa_ETL\county loads\{0}\{1}\aprx\ffpa_{0}_{1}\{0}_{1}.gdb\{0}_county".format(county, year), county_parcel_field)


#Calcuate Total Acreages, Land Value, Improved Land Value, Unimproved Acres, Improved Acres, Year

field_list = ["PRCL_REVW_TOTAL_ACRES", county_total_acres_field, "PRCL_REVW_LAND_VALUE_AMT", county_land_value_field, "PRCL_REVW_IMPROVED_VALUE_AMT",
              county_improved_land_value_field, "PRCL_REVW_UNIMPROVED_ACRES", county_unimproved_acres_field, "PRCL_REVW_IMPROVED_ACRES", "PRCL_REVW_YR", "FFPA_PRCL_ADDR", "SUB_ADDRESS", "SITUS_CITY_NM", 
              "SITUS_ZIP_NR"]

with arcpy.da.UpdateCursor("join_layer", field_list) as cursor:
    for row in cursor:
        #Total Acres
        row[0] = row [1]
        #Land Values Amount
        row[2] = row [3]
        #Improved Land Value Amount
        row[4] = row [5]
        #Unimproved Acres
        if row [7] != None:
            if row[7] != "":
                unimproved = round(float(row[7]), 2)
                row[6] = unimproved
        #Improved Acres
        if row [6] != None:
            row[8] = row[0] - row[6]
        #FFPA Year
        row[9] = year
        #Parcel Address calced to one
        if row [10] == None:
            row[10] = " "
        if row [11] == None:
            row [11] = " "
        if row [12] == None:
            row[12] = " "
        if row [13] == None:
            row[13] = " "  
        row[10] = str(row[10]) + " " + str(row[11]) + " " + str(row[12]) + " " + str(row[13])

        cursor.updateRow(row)

del cursor



   