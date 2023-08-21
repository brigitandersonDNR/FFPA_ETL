#need to do: add try/exept handling, add messages

import arcpy
arcpy.env.overwriteOutput = True

#Get Variables
county = arcpy.GetParameterAsText(0)
year = arcpy.GetParameter(1)
county_dataset=arcpy.GetParameterAsText(2)

#Set Variables
state_dataset = r"\\dnr\divisions\RP_GIS\projects\ffpa_ETL\data and tools\state parcel data\{0}\Current_Parcels_with_Ownership.gdb\Parcels_{0}".format(year)
county_state=r"\\dnr\divisions\RP_GIS\projects\ffpa_ETL\county loads\{0}\{1}\aprx\ffpa_{0}_{1}\{0}_{1}.gdb\{0}_state".format(county, year)
where_clause = "COUNTY_NM = '{0}'".format(county) 


## Convert county data excel to table
arcpy.conversion.ExcelToTable(county_dataset, "{0}_county".format(county))

##Create main feature class from state data
ffpa_county = arcpy.conversion.ExportFeatures(state_dataset, county_state, where_clause)


##Match Field Values of New Feature Class to FFPA Needs

#Delete Fields
drop_fields = ["VALUE_LAND", "VALUE_BLDG", "DATA_LINK", "FILE_DATE"]

for field in drop_fields:
    arcpy.management.DeleteField(ffpa_county, field)

#Add Fields
arcpy.management.AddFields(ffpa_county, [["PRCL_REVW_UNIMPROVED_ACRES", "Double"], ["PRCL_REVW_IMPROVED_ACRES", "Double"], ["PRCL_REVW_TOTAL_ACRES", "Double"],
                                         ["PRCL_REVW_LAND_VALUE_AMT", "Double"], ["PRCL_REVW_IMPROVED_VALUE_AMT", "Double"], ["RGN_CD", "Short"],
                                         ["FFPA_CO_EXT_ID", "Short"], ["FFPA_TAX_STATUS_CD", "Short"], ["PRCL_REVW_YR", "Short"], ["ASMNT_DECSN_CD", "Short"],
                                         ["FFPA_PRCL_EXEMPT_FLG", "Text"], ["ASMNT_DECSN_NM", "Text"], ["RS_PRED_NM", "Text"], ["FLAG_TY_CD", "Text"]])

#Rename Fields
rename_fields = {
    "PARCEL_ID_NR":"PRCL_REVW_NORMALIZED_PRCL_ID",
    "ORIG_PARCEL_ID":"PRCL_REVW_ORIGINAL_PRCL_ID",
    "OWNER_NM":"PRCL_OWNER_NM",
    "OWNER_NM2":"PRCL_OWNER_NM2",
    "OWNER_LINE1_AD":"PRCL_OWNER_ADDR_LINE1",
    "OWNER_LINE2_AD":"PRCL_OWNER_ADDR_LINE2",
    "OWNER_LINE3_AD":"PRCL_OWNER_ADDR_LINE3",
    "OWNER_CITY_NM":"PRCL_OWNER_ADDR_CITY",
    "OWNER_ST_NM":"PRCL_OWNER_ADDR_STATE",
    "OWNER_ZIP_NR":"PRCL_OWNER_ADDR_ZIP",
    "SITUS_ADDRESS":"FFPA_PRCL_ADDR",
    "LANDUSE_CD":"DOR_LAND_USE_CD"
}

for key in rename_fields:
    arcpy.management.AlterField(ffpa_county, key, rename_fields[key], rename_fields[key])

#Make FFPA Parcel Address field longer for next step calcing to full address
arcpy.management.AlterField(ffpa_county, "FFPA_PRCL_ADDR", field_length = 250)



    