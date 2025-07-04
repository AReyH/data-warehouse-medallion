/*
DDL Script: Create bronze tables

Script purpose:
  This script creates tables in the `bronze` layer schema. It drops the table if it
  exists  prior to creating to avoid any errors. Run this script ONLY WHEN DOING A FULL LOAD.
  Run this script to re-define the DDL structure of the `bronze` tables.
*/

drop table if exists bronze.crm_cust_info;
create table bronze.crm_cust_info (
    cst_id int,
    cst_key varchar(50),
    cst_firstname varchar(50),
    cst_lastname varchar(50),
    cst_marital_status varchar(50),
    cst_gndr varchar(50),
    cst_create_date date
);

drop table if exists bronze.crm_prd_info;
create table bronze.crm_prd_info (
    prd_id int,
    prd_key varchar(50),
    prd_nm varchar(50),
    prd_cost int,
    prd_line varchar(50),
    prd_start_dt datetime,
    prd_end_dt datetime
);

drop table if exists bronze.crm_sales_details;
create table bronze.crm_sales_details (
    sls_ord_num varchar(50),
    sls_prd_key varchar(50),
    sls_cust_id int,
    sls_order_dt datetime,
    sls_ship_dt datetime,
    sls_due_dt datetime,
    sls_sales int,
    sls_quantity int,
    sls_price int
);

drop table if exists bronze.erp_loc_a101;
create table bronze.erp_loc_a101 (
    cid varchar(50),
    cntry varchar(50)
);

drop table if exists bronze.erp_cust_az12;
create table bronze.erp_cust_az12 (
    cid varchar(50),
    bdate date,
    gen varchar(50)
);

drop table if exists bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2 (
    id varchar(50),
    cat varchar(50),
    subcat varchar(50),
    maintenance varchar(50)
);
