import os
import csv
import time
import pandas as pd
import cx_Oracle
import arcpy

TIMESTAMP = time.strftime("%Y%m%d_%H%M%S")

# change these variables
# -------------------------
county = arcpy.GetParameter(0)
year = arcpy.GetParameter(1)

# input CSV data directory
#OUTPUT_CSV_DIR = r"K:\users\kdav490\projects\ffpa_timmons\20190501_transform_script\runs\20210608_132201_ffpa_id_reset_mason_PROD\csv_data"
OUTPUT_CSV_DIR = r"\\dnr\divisions\RP_GIS\projects\ffpa_ETL\county loads\{0}\{1}\results\csv".format(county, year)

# DEVL or DEMO or PROD
DB_ENVIRONMENT = arcpy.GetParameter(2)
# -------------------------


## Call this function when your *.csv file does contain an "IMPORT_ID" column as the first column
def cast_row_to_correct_datatype_with_import_id(in_row, in_field_type_list, in_field_len_list):
    
    # NUMBER or STRING
    temp_list = list(in_row)
    
    for i, s in enumerate(temp_list):
        
        if in_field_type_list[i] == "DB_TYPE_NUMBER":
            
            if "." in s:
                temp_list[i] = float(s)
            else:
                try:
                    if s == '-1' or s == -1:
                        temp_list[i] = None
                    else:
                        temp_list[i] = int(s)
                except ValueError:
                    # value is Null
                    temp_list[i] = None
                    
        if in_field_type_list[i] == "DB_TYPE_VARCHAR":
            
            if len(s) > in_field_len_list[i]:
                # shorten the field 
                temp_list[i] = str(s)[0:in_field_len_list[i]]
            else:
                if s == "None" or s == '-1':
                    temp_list[i] = None    
                else:
                    temp_list[i] = str(s)
                    
    return tuple(temp_list)

## Call this function when your *.csv file does not contain an "IMPORT_ID" column as the first column
def cast_row_to_correct_datatype_without_import_id(in_row, in_field_type_list, in_field_len_list):
    
    # NUMBER or STRING
    temp_list = list(in_row)
    #print("temp_list: {}".format(temp_list))    
    
    for i, s in enumerate(temp_list):
        
        if in_field_type_list[i + 1] == "DB_TYPE_NUMBER":
            
            if "." in s:
                temp_list[i] = float(s)
            else:
                try:
                    if s == '-1' or s == -1:
                        temp_list[i] = None
                    else:
                        temp_list[i] = int(s)
                except ValueError:
                    # value is Null
                    temp_list[i] = None
                    
        if in_field_type_list[i + 1] == "DB_TYPE_VARCHAR":
            
            if len(s) > in_field_len_list[i + 1]:
                # shorten the field 
                temp_list[i] = str(s)[0:in_field_len_list[i + 1]]
            else:
                if s == "None" or s == '-1':
                    temp_list[i] = None    
                else:
                    temp_list[i] = str(s)
                    
    return tuple(temp_list)

# stage table structures
FFPA_PARCEL_TBL_STRUCTURE = [
    "IMPORT_ID",
    "FFPA_PRCL_ID",
    "FFPA_PRCL_ADDR",
    "DNR_ADMIN_DISTRICT_CD",
    "RGN_CD",
    "FIRE_PROTECTION_DIST_CD",
    "FFPA_CO_EXT_ID",
    "FFPA_PRCL_EXEMPT_FLG",
    "FFPA_PRCL_DIRECT_BILL_FLG",
    "FFPA_JUR_TY_CD",
    "FFPA_PRCL_COMBINATION_ID",
]

PARCEL_REVIEW_TBL_STRUCTURE = [
    "IMPORT_ID",
    "PRCL_REVW_ID",
    "FFPA_PRCL_ID",
    "PRCL_REVW_YR",
    "PRCL_REVW_TOTAL_ACRES",
    "PRCL_REVW_UNIMPROVED_ACRES",
    "DOR_LAND_USE_CD",
    "FFPA_TAX_STATUS_CD",
    "PRCL_REVW_ORIGINAL_PRCL_ID",
    "PRCL_REVW_NORMALIZED_PRCL_ID",
    "PRCL_REVW_LAND_VALUE_AMT",
    "PRCL_REVW_IMPROVED_VALUE_AMT",
    "FFPA_STATUS_CD",
    "PRCL_REVW_CREATED_TMDT",
    "PRCL_REVW_UPDATED_TMDT",
    "PRCL_REVW_UPDATED_BY_ID",
    "PRCL_REVW_FIRE_DIST_ASMNT_FLG",
    "PRCL_REVW_NEEDS_SV_FLG",
    "PRCL_REVW_SV_COMP_FLG",
    "ASMNT_DECSN_CD",
]

FFPA_PARCEL_REVIEW_FLAG_TBL_STRUCTURE = [
    "IMPORT_ID",
    "PRCL_REVW_FLG_ID",
    "PRCL_REVW_ID",
    "FLAG_TY_CD",
    "PRCL_REVW_IGNORE_FLG",
]

FFPA_PARCEL_OWNER_TBL_STRUCTURE = [
    "IMPORT_ID",
    "PRCL_OWNER_ID",
    "OWNER_TY_CD",
    "PRCL_OWNER_NM",
    "PRCL_OWNER_ADDR_LINE1",
    "PRCL_OWNER_ADDR_LINE2",
    "PRCL_OWNER_ADDR_CITY",
    "PRCL_OWNER_ADDR_STATE",
    "PRCL_OWNER_ADDR_ZIP",
    "PRCL_OWNER_EMAIL",
]

FFPA_OWNER_PARCEL_REVIEW_TBL_STRUCTURE = [
    "IMPORT_ID",
    "PRCL_REVW_ID",
    "PRCL_OWNER_ID",
]

##FFPA_PARCEL_SPATIAL_STRUCTURE = [
##    ("IMPORT_ID", "LONG"),
##    ("PSL_ID","LONG"),
##    ("PSL_YR", "SHORT"),
##    ("PSL_NRML_PRCL_ID","TEXT"),
##    ("RS_PRED_LABEL_NM","TEXT"),
##    ("COUNTY_CD", "SHORT"),
##    ("COUNTY_NM", "TEXT"),
##    ("PSL_REC_CNT", "SHORT"),
##    ("PSL_SPATIAL_ACRES", "DOUBLE"),
##]

# order matters, wait no it doesn't
tables_to_process = [
    ("FFPA.STAGE_FFPA_PARCEL", "ffpa_parcel_tbl.csv", FFPA_PARCEL_TBL_STRUCTURE),
    ("FFPA.STAGE_PARCEL_REVIEW", "parcel_review_tbl.csv", PARCEL_REVIEW_TBL_STRUCTURE),
    ("FFPA.STAGE_PARCEL_REVIEW_FLAG", "parcel_review_flag_tbl.csv", FFPA_PARCEL_REVIEW_FLAG_TBL_STRUCTURE),
    ("FFPA.STAGE_PARCEL_OWNER", "parcel_owner_tbl.csv", FFPA_PARCEL_OWNER_TBL_STRUCTURE),
    ("FFPA.STAGE_OWNER_PARCEL_REVIEW", "parcel_owner_review_tbl.csv", FFPA_OWNER_PARCEL_REVIEW_TBL_STRUCTURE)
]

# 'ffpa_load'_user is the username, 'ffpa_loaddevl' is the password, gondordev:1521 is host and port, dev1.wadnr.gov is the instance
# maybe the connection strings for DEMO and PROD refer to aliases in tnsnames.ora
if DB_ENVIRONMENT == "DEVL":
    connection_str = 'ffpa_load_user/ffpa_loaddevl@gondordev:1521/devl.wadnr.gov'
if DB_ENVIRONMENT == "DEMO":
    connection_str = 'ffpa_load_user/ffpa_loaddemo@DEMO'
if DB_ENVIRONMENT == "PROD":
    connection_str = 'ffpa_load_user/ffpa_load1prod@PROD'

print("Begin processing\n")

print("Begin connecting to {0}".format(DB_ENVIRONMENT))

with cx_Oracle.connect(connection_str) as conn:

    print("End connecting to {0}\n".format(DB_ENVIRONMENT))
    
    conn.autocommit = True
    cur = conn.cursor()

    for ffpa_tbl_nm, csv_nm, tbl_structure_list in tables_to_process:

        ## does the csv file contain an 'IMPORT_ID' column?
        import_id_column = len((list(filter(lambda e: e == "IMPORT_ID", tbl_structure_list))))

        # create list of tuples
        
        print("Begin loading table: {}".format(ffpa_tbl_nm))

        print("Begin deleting all rows from table: {}".format(ffpa_tbl_nm))
        cur.execute("DELETE FROM {}".format(ffpa_tbl_nm))
        print("End deleting all rows from table: {}".format(ffpa_tbl_nm))
        
        print("Begin reading file: {0}".format(csv_nm))
        #processing_df = pd.read_csv(os.path.join(OUTPUT_CSV_DIR, csv_nm), dtype=object, sep="|", header=0)
        processing_df = pd.read_csv(os.path.join(OUTPUT_CSV_DIR, csv_nm), dtype=object, sep="|", header=0)
        print("End reading file: {0}".format(csv_nm))
        
        # fill nans
        processing_df.fillna('-1', inplace=True)
        data = list(processing_df.itertuples(index = False, name = None))        
        field_list_str = ", ".join(i for i in tbl_structure_list)        
        val_pos_str = val_pos_str = ", ".join(":" + str(i+1) for i in range(len(tbl_structure_list)))

        try:
            
            statement = "INSERT INTO {0} ({1}) values ({2})".format(ffpa_tbl_nm, field_list_str, val_pos_str)
            
            # for field value test
            cur.execute("SELECT * FROM {0}".format(ffpa_tbl_nm))
            
            typeObject = cur.description[0][1];
            typeObjectDictionary = dir(typeObject)
            cur_description = cur.description
            
            field_value_test = [i[1].name for i in cur_description]
            #print("field_value_test: {0}".format(field_value_test))
            
            field_len_test = [i[2] for i in cur_description]
            #print("field_len_test: {0}".format(field_len_test))
            
            count = 0

            print("Begin inserting rows")
            for row in data:
                
                if import_id_column > 0:
                    corrected_row = cast_row_to_correct_datatype_with_import_id(row, field_value_test, field_len_test)
                else:
                    corrected_row = cast_row_to_correct_datatype_without_import_id(row, field_value_test, field_len_test)
                    
                cur.execute(statement, corrected_row)
                
                count += 1
                
                if count % 5000 == 0:
                    print("{0} rows inserted...".format(count))
                    
            print("End inserting rows")
            print("{0} rows inserted!".format(count))
            print("End loading table: {0}\n".format(ffpa_tbl_nm))

        except cx_Oracle.DatabaseError as e:
            errorObj, = e.args
            print("Row {} has an error: {}".format(cur.rowcount, errorObj.message))

    cur.close()
    
with open(os.path.join(os.path.dirname(OUTPUT_CSV_DIR),"load_ffpa_stage_tables_{}_{}.txt".format(DB_ENVIRONMENT, TIMESTAMP)), 'a') as file_obj:
    file_obj.write("[SCRIPTING TO DATABASE DOCUMENTATION]\n")
    file_obj.write("FFPA stage tables successfully loaded into {} on {}\n".format(DB_ENVIRONMENT, time.ctime()))
    file_obj.write("Loaded via python script: 'load_ffpa_stage_tables.py'\n")
    file_obj.write("Kirk Davis - Wildfire Division - WA DNR\n")
    file_obj.flush()
    file_obj.close()

print("End processing\n")
