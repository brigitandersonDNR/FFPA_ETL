#need to do: add try/exept handling, add messages

import arcpy
arcpy.env.overwriteOutput = True

#Get Variables
county = arcpy.GetParameter(0)
year = arcpy.GetParameterAsText(1)
ffpa_county=arcpy.GetParameter(2)
county_exempt_clause=arcpy.GetParameter(3)

#Set Variables
ffpa_county = "{0}_state".format(county)
nlcd_fc = r"\\dnr\divisions\RP_GIS\projects\ffpa_ETL\data and tools\standard input data\input_analysis_data.gdb\NLCD_2019_vegetation"


#Calculate Exemption Status

#Select and Calulate Exempt Values
arcpy.management.SelectLayerByAttribute(ffpa_county, "NEW_SELECTION", county_exempt_clause)
arcpy.management.CalculateField(ffpa_county, "FFPA_TAX_STATUS_CD", 2, "PYTHON3")
arcpy.management.CalculateField(ffpa_county, "FFPA_PRCL_EXEMPT_FLG", '"Y"', "PYTHON3")
#Select and Calculate Non-Exempt Values 
arcpy.management.SelectLayerByAttribute(ffpa_county, "NEW_SELECTION", county_exempt_clause, invert_where_clause= "INVERT")
arcpy.management.CalculateField(ffpa_county, "FFPA_TAX_STATUS_CD", 1, "PYTHON3")
arcpy.management.CalculateField(ffpa_county, "FFPA_PRCL_EXEMPT_FLG", '"N"', "PYTHON3")
arcpy.SelectLayerByAttribute_management(ffpa_county, "CLEAR_SELECTION")


#Make Assessment Decisions from DOR Code and Land Owner

field_list = ["DOR_LAND_USE_CD", "ASMNT_DECSN_CD", "ASMNT_DECSN_NM", "PRCL_OWNER_NM"]
dor_land_codes_assess = [88, 92, 95]
us_ownership_options = ["United States", "America" "USFS", "USDA", "US Postal"]

with arcpy.da.UpdateCursor(ffpa_county, field_list) as cursor:
    for row in cursor:
        for code in dor_land_codes_assess:
            if row[0] == code:
                row [1] = 1
                row[2] = "ASSESS"

                cursor.updateRow(row)
        
        for owner in us_ownership_options:
            if row[3] == owner:
                row[1] = 2
                row[2] = "NO ASSESS"

                cursor.updateRow(row)
del cursor


#Remote Sensing Flags and Predictions Analysis

arcpy.SelectLayerByLocation_management(ffpa_county, "INTERSECT", nlcd_fc, selection_type="NEW_SELECTION")
arcpy.management.CalculateField(ffpa_county, "RS_PRED_NM", "'REVIEW'", "PYTHON3")
arcpy.management.CalculateField(ffpa_county, "FLAG_TY_CD", 6, "PYTHON3")

arcpy.SelectLayerByLocation_management(ffpa_county, "INTERSECT", nlcd_fc, selection_type="NEW_SELECTION", invert_spatial_relationship= "INVERT")
arcpy.management.CalculateField(ffpa_county, "RS_PRED_NM", "'NOT ASSESS' ", "PYTHON3")
arcpy.management.CalculateField(ffpa_county, "FLAG_TY_CD", 5, "PYTHON3")
arcpy.SelectLayerByAttribute_management(ffpa_county, "CLEAR_SELECTION")


#Populate all other first round assessment decisions

assessment_field_list = ["ASMNT_DECSN_CD", "RS_PRED_NM", "ASMNT_DECSN_NM"]

with arcpy.da.UpdateCursor(ffpa_county, assessment_field_list) as cursor:
    for row in cursor:
        if row[0] == None:
            if row [1] == "REVIEW":
                row[0] = 3
                row[2] = "REVIEW"

                cursor.updateRow(row)

        if row[0] == None:
            if row[1] == "NOT ASSESS":
                row[0] = 2
                row[2] = "NO ASSESS"

                cursor.updateRow(row) 

del cursor
                
