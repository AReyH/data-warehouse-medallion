/*
Create tables on the silver layer

Script purpose:
  This script creates the tables (drops and creates if they previously existed)
  in the Silver layer. Do not run this unless you want to create the Silver layer
  from scratch.

*/


drop table if exists silver.crm_cust_info;
create table silver.crm_cust_info (
    cst_id int,
    cst_key varchar(50),
    cst_firstname varchar(50),
    cst_lastname varchar(50),
    cst_marital_status varchar(50),
    cst_gndr varchar(50),
    cst_create_date date,
    dwh_create_date datetime default CURRENT_DATE()
);

-- Changed due to processing in the silver layer
drop table if exists silver.crm_prd_info;
create table silver.crm_prd_info (
    prd_id int,
    cat_id  varchar(50),
    prd_key varchar(50),
    prd_nm varchar(50),
    prd_cost int,
    prd_line varchar(50),
    prd_start_dt date,
    prd_end_dt date,
    dwh_create_date datetime default CURRENT_DATE()
);

drop table if exists silver.crm_sales_details;
create table silver.crm_sales_details (
    sls_ord_num varchar(50),
    sls_prd_key varchar(50),
    sls_cust_id int,
    sls_order_dt datetime,
    sls_ship_dt datetime,
    sls_due_dt datetime,
    sls_sales int,
    sls_quantity int,
    sls_price decimal,
    dwh_create_date datetime default CURRENT_DATE()
);

drop table if exists silver.erp_loc_a101;
create table silver.erp_loc_a101 (
    cid varchar(50),
    cntry varchar(50),
    dwh_create_date datetime default CURRENT_DATE()
);

drop table if exists silver.erp_cust_az12;
create table silver.erp_cust_az12 (
    cid varchar(50),
    bdate date,
    gen varchar(50),
    dwh_create_date datetime default CURRENT_DATE()
);

drop table if exists silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2 (
    id varchar(50),
    cat varchar(50),
    subcat varchar(50),
    maintenance varchar(50),
    dwh_create_date datetime default CURRENT_DATE()
);
