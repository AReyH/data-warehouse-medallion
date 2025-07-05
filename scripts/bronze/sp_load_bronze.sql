/*
Stored procedure: Load Bronze Layer (Source -> Bronze)

Script purpose:
  This stored procedure loads data into the `bronze` schema layer from an external .csv file.
  It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses a `FULL LOAD` method of loading the data.
  It uses Javascript in order to perform these actions.

Parameters:
  None.
  This stored procedure does not accept any parameters or returns any values.

Usage example:
  call bronze.load_bronze();
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var log = "";
try {
    var sql_commands = [
        "TRUNCATE TABLE bronze.crm_cust_info;",
        `COPY INTO bronze.crm_cust_info
         FROM '@DATAWAREHOUSE.PUBLIC.DWH_FILES/crm/cust_info.txt'
         FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1);`,

        "TRUNCATE TABLE bronze.crm_prd_info;",
        `COPY INTO bronze.crm_prd_info
         FROM '@DATAWAREHOUSE.PUBLIC.DWH_FILES/crm/crm_prd_info.txt'
         FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1);`,

        "TRUNCATE TABLE bronze.crm_sales_details;",
        `COPY INTO bronze.crm_sales_details
         FROM '@DATAWAREHOUSE.PUBLIC.DWH_FILES/crm/crm_sales_details.txt'
         FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1);`,

        "TRUNCATE TABLE bronze.erp_loc_a101;",
        `COPY INTO bronze.erp_loc_a101
         FROM '@DATAWAREHOUSE.PUBLIC.DWH_FILES/erp/erp_loc_a101.txt'
         FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1);`,

        "TRUNCATE TABLE bronze.erp_cust_az12;",
        `COPY INTO bronze.erp_cust_az12
         FROM '@DATAWAREHOUSE.PUBLIC.DWH_FILES/erp/erp_cust_az12.txt'
         FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1);`,

        "TRUNCATE TABLE bronze.erp_px_cat_g1v2;",
        `COPY INTO bronze.erp_px_cat_g1v2
         FROM '@DATAWAREHOUSE.PUBLIC.DWH_FILES/erp/erp_px_cat_g1v2.txt'
         FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1);`
    ];

    for (var i = 0; i < sql_commands.length; i++) {
        snowflake.execute({sqlText: sql_commands[i]});
    }

    return "Bronze layer loaded successfully.";
} catch(err) {
    return "Failed: " + err; 
}
$$;
