#need to do: make queries, check field names, test with second year load, add try/exept handling, add messages - this whole script is untested

import arcpy
arcpy.env.overwriteOutput = True

#Get Variables
county = arcpy.GetParameter(0)
year= arcpy.GetParameter(1)
county_parcel_num_field = arcpy.GetParameterAsText(2)

#Set Variables
ffpa_county = "{0}_state".format(county)
ffpa_SV = "FFPA.FFPA_PARCEL_SV"


##Second Year Flags

#Where Clauses ***THESE NEED TO BE TESTED AND WILL LIKELY NOT WORK AT ALL!!***
geometry_change_clause = "'PRCL_REVW_TOTAL_ACRES' <> 'FFPA.FFPA_PARCEL_SV.PRCL_REVW_TOTAL_ACRES'"
exemption_change_clause = "'FFPA_PRCL_EXEMPT_FLG' <> 'FFPA.FFPA_PARCEL_SV.FFPA_PRCL_EXEMPT_FLG'"
dor_change_clause = "'DOR_LAND_USE_CD' <> 'FFPA.FFPA_PARCEL_SV.DOR_LAND_USE_CD'"
new_parcel_number_clause = "'PRCL_OWNER_NM' <> 'FFPA_PARCEL_SV.PRCL_OWNER_NM'"
landowner_change_clause = "'FFPA_PARCEL_SV.PRCL_REVW_NORMALIZED_PRCL_ID' IS NULL"

arcpy.management.MakeFeatureLayer(r"\\dnr\divisions\RP_GIS\projects\ffpa_ETL\county loads\{0}\{1}\aprx\{0}_{1}.gdb\{0}_state".format(county, year),"join_layer")
arcpy.management.JoinField("join_layer", "PRCL_REVW_NORMALIZED_PRCL_ID", ffpa_SV, "PRCL_REVW_NORMALIZED_PRCL_ID")

#Geometry Change Flag
arcpy.SelectLayerByAttribute_management("join_layer", "NEW_SELECTION", geometry_change_clause)
arcpy.management.CalculateField(ffpa_county, "FLAG_TY_CD", "!FLAG_TY_CD! + '; 17'", "PYTHON3")

#Exemption Change Flag
arcpy.SelectLayerByAttribute_management("join_layer", "NEW_SELECTION", exemption_change_clause)
arcpy.management.CalculateField(ffpa_county, "FLAG_TY_CD", "!FLAG_TY_CD! + '; 19'", "PYTHON3")

#DOR Change Flag
arcpy.SelectLayerByAttribute_management("join_layer", "NEW_SELECTION", dor_change_clause)
arcpy.management.CalculateField(ffpa_county, "FLAG_TY_CD", "!FLAG_TY_CD! + '; 20'", "PYTHON3")

#New Parcel Flag
arcpy.SelectLayerByAttribute_management("join_layer", "NEW_SELECTION", new_parcel_number_clause)
arcpy.management.CalculateField(ffpa_county, "FLAG_TY_CD", "!FLAG_TY_CD! + '; 3'", "PYTHON3")

#Landowner Change Flag
arcpy.SelectLayerByAttribute_management("join_layer", "NEW_SELECTION", landowner_change_clause)
arcpy.management.CalculateField(ffpa_county, "FLAG_TY_CD", "!FLAG_TY_CD! + '; 16'", "PYTHON3")


##Second Year Assessment Decisions

assessment_field_list = ["ASMNT_DECSN_CD", county_parcel_num_field, "ASMNT_DECSN_NM", "FFPA.FFPA_PARCEL_SV.ASMNT_DECSN_CD", "FFPA.FFPA_PARCEL_SV.ASMNT_DECSN_NM", 
                         "PRCL_REVW_TOTAL_ACRES", "FFPA.FFPA_PARCEL_SV.PRCL_REVW_TOTAL_ACRES"]

with arcpy.da.UpdateCursor(ffpa_county, assessment_field_list) as cursor:
    for row in cursor:
        #Adopt old assessment decision unless DOR land use code in new data says to Review OR if it is a new parcel
        if row[0] != 1:
            if row [1] != None:
                row[0] = row[3]
                row[2] = row[5]
        #Change Assessment Decision to Needs Reivew if acres went over/under 50 aces
        if row[6] > 50:
            if row[7] < 50:
                row[0] = 3
                row[2] = "REVIEW"
        if row[6] < 50:
            if row[7] > 50:
                row[0] = 3
                row[2] = "REVIEW"

        cursor.updateRow(row) 

del cursor
                