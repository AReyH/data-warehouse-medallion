/*
Quality Checks:

Script Purpose:
  This script performs varios quality checks for data consistency, accuracy,
  and standardization across the `silver` schema. It includes checks for:
  - Null or duplicate primary keys.
  - Unwanted spaces in string.
  - Data standardization and cosistency.
  - Invalid date ranges and orders.
  - Data consistency between related fields.

Usage Notes:
  This code is the thought process that went into the transformations.
  This is not meant to be run in production, but instead to look at the logic
  behind every decision taken into creating the silver layer.
*/

----------------------------------
-- silver.crm_cust_info
----------------------------------

-- Check for nulls or duplicates in primary key
-- Expectation: No result

select cst_id,
        count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null;

-- Now we're gonna focus on one result: 29449

select 
    *,
    row_number() over (partition by cst_id order by cst_create_date desc) flag_last
from bronze.crm_cust_info
where cst_id = '29449';

-- Now we're gonna check all the duplicates

select *
from (
    select 
        *,
        row_number() over (partition by cst_id order by cst_create_date desc) flag_last
    from bronze.crm_cust_info
)
where flag_last <> 1;

-- Check unwanted spaces
select cst_firstname
from bronze.crm_cust_info
where cst_firstname <> trim(cst_firstname);

-- Check low cardinality value columns like `cst_marital_status` and `cst_gndr`
select distinct cst_gndr
from bronze.crm_cust_info;

--------
insert into silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
select 
    cst_id,
    cst_key,
    trim(cst_firstname),
    trim(cst_lastname),
    case when upper(trim(cst_marital_status)) = 'S' then 'Single'
        when upper(trim(cst_marital_status)) = 'M' then 'Married'
        else 'n/a'
    end as cst_marital_status,
    case when upper(trim(cst_gndr)) = 'F' then 'Female'
        when upper(trim(cst_gndr)) = 'M' then 'Male'
        else 'n/a' 
    end as cst_gndr,
    cst_create_date
from (
    select 
        *,
        row_number() over (partition by cst_id order by cst_create_date desc) flag_last
    from bronze.crm_cust_info
    where cst_id is not null
)
where flag_last = 1;

-- Now we want to do all the checks done on `bronze` now on `silver`
-- to check the quality of the data


-- Check for duplicate or null ids
select cst_id,
        count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null;

-- Check for white spaces
select *
from silver.crm_cust_info
where cst_firstname <> trim(cst_firstname)
or cst_lastname <> trim(cst_lastname);


----------------------------------
-- silver.crm_prd_info
----------------------------------
select
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
from bronze.crm_prd_info;

-- Check for null ids
select 
    prd_id,
    count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;

-- Check for unwanted spaces
select *
from bronze.crm_prd_info
where prd_nm <> trim(prd_nm);

-- Check for null or negative numbers
select *
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null;

-- Check all unique values for `prd_line`
select distinct prd_line
from bronze.crm_prd_info;

-- Check for invalid dates
-- End date must NOT be less than start date
select *
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt;

select 
    prd_id,
    prd_key,
    prd_nm,
    prd_start_dt,
    prd_end_dt,
    -- This column partitions the rows based on the `prd_key` and orders by `prd_start_dt`
    -- And selects the next value in `prd_start_dt`, or null if it's the last value
    dateadd(day,'-1',lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)) as prd_end_dt_test
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R','AC-HE-HL-U509');



insert into silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
select
    prd_id,
    replace(substring(prd_key,1,5),'-','_') as cat_id,
    substring(prd_key,7,length(prd_key)) as prd_key,
    prd_nm,
    ifnull(prd_cost,0) as prd_cost,
    case upper(trim(prd_line)) -- This is only done when mapping values
        when 'M' then 'Mounting'
        when 'R' then 'Road'
        when 'S' then 'Other Sales'
        when 'T' then 'Touring'
        else 'n/a' 
    end as prd_line,
    cast(prd_start_dt as date) as prd_start_dt,
    cast(dateadd(day,'-1',lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)) as date) as prd_end_dt_test
from bronze.crm_prd_info;


-- Check table
select 
    prd_key,
    count(*)
from silver.crm_prd_info
group by prd_key
having count(*) > 1 or prd_key is null;

-- Check for date order
select *
from silver.crm_prd_info
where prd_end_dt < prd_start_dt;


-- Check the table
select *
from silver.crm_prd_info;


----------------------------------
-- silver.crm_sales_details
----------------------------------

-- Check for extra spaces
-- No extra spaces
select 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
from bronze.crm_sales_details
where sls_ord_num <> trim(sls_ord_num)
or sls_prd_key <> trim(sls_prd_key);

-- Convert the date columns from integer to date type
-- Check the quality of the columns
select 
    sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0;

-- Since we have a lot of rows with value 0, and this cannot be converted
-- into a date, we need to make them null
select 
    nullif(sls_order_dt,0)
from bronze.crm_sales_details
where sls_order_dt <= 0
or length(sls_order_dt) <> 8
or sls_order_dt > 20500101
or sls_order_dt < 19000101;

-- Check `sls_due_dt`
select 
    sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0;

-- Check that `sls_order_dt` is smaller than `sls_ship_dt`
select *
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt
or sls_order_dt > sls_due_dt;

-- Check the business logic of `sls_sales = sls_quantity = sls_price`
select 
    sls_sales,
    sls_quantity,
    sls_price
from bronze.crm_sales_details
where sls_sales <> (sls_quantity*sls_price);

-- Fix
-- If `sls_sales` is negative, zero or null, calculate it using `sls_quantity` and `sls_price`
-- If `sls_price` is zero or null, calculate it using `sls_sales` and `sls_quantity`
-- If `sls_price` is negative, convert it to a positive value
select 
    case when sls_sales <= 0 or sls_sales is null or sls_sales <> sls_quantity*abs(sls_price) then sls_quantity*abs(sls_price)
        else sls_sales
    end as sls_sales,
    sls_quantity,
    case when sls_price = 0 or sls_price is null then sls_sales/sls_quantity 
        when sls_price < 0 then -sls_price
        else sls_price
    end as sls_price
from bronze.crm_sales_details
where sls_sales <> (sls_quantity*sls_price);

insert into silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
select 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    case when sls_order_dt = 0 or length(sls_order_dt) <> 8 then null
        else to_date(sls_order_dt::string, 'YYYYMMDD')
    end as sls_order_dt,
    case when sls_ship_dt = 0 or length(sls_ship_dt) <> 8 then null
        else to_date(sls_ship_dt::string, 'YYYYMMDD')
    end as sls_ship_dt,
    case when sls_due_dt = 0 or length(sls_due_dt) <> 8 then null
        else to_date(sls_due_dt::string, 'YYYYMMDD')
    end as sls_due_dt,
    case when sls_sales <= 0 or sls_sales is null or sls_sales <> sls_quantity*abs(sls_price) then sls_quantity*abs(sls_price)
        else sls_sales
    end as sls_sales,
    sls_quantity,
    case when sls_price = 0 or sls_price is null then sls_sales/sls_quantity
        when sls_price < 0 then -sls_price
        else sls_price
    end as sls_price
from bronze.crm_sales_details;


-- Check for data integrity
select *
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt
or sls_order_dt > sls_due_dt;

select 
    sls_sales,
    sls_quantity,
    sls_price
from silver.crm_sales_details
where sls_sales <> (sls_quantity*sls_price);

----------------------------------
-- silver.erp_cust_az12
----------------------------------

select *
from bronze.erp_cust_az12
where cid like '%AW00011000';

-- Clean up the `cid` column since some users start with a "NASAW" and others with "AW"
select
    cid,
    case when cid like 'NAS%' then right(cid,length(cid)-3)
        else cid
    end as cid_2
    /*
    case when cid like 'NAS%' then replace(cid,'NAS','')
        else cid
    end as cid_2
    */
from bronze.erp_cust_az12;

-- Check for very old costumers
select distinct bdate
from bronze.erp_cust_az12
where bdate < '1925-01-01' or bdate > current_date();


-- Check for genders

select distinct case when upper(trim(gen)) = 'M' then 'Male'
        when upper(trim(gen)) = 'F' then 'Female'
        else 'n/a'
    end as gen
from bronze.erp_cust_az12;


insert into silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
select 
    case when cid like 'NAS%' then right(cid,length(cid)-3)
        else cid
    end as cid,
    case when bdate > current_date() then null
        else bdate
    end as bdate,
    case when upper(trim(gen)) in ('M','MALE') then 'Male'
        when upper(trim(gen)) in ('F','FEMALE') then 'Female'
        else 'n/a'
    end as gen
from bronze.erp_cust_az12;


select *
from silver.erp_cust_az12;


----------------------------------
-- silver.erp_loc_a101
----------------------------------

-- Check if the `cid` matches with the `crm_cust_info`
select 
    cid,
    cntry
from bronze.erp_loc_a101;

-- Get rid of the `-` in the `cid` column and check if there 
-- are any empty spaces
select 
    cid
from bronze.erp_loc_a101
where cid <> trim(cid);



select 
    replace(cid,'-','') as cid,
    case when trim(cntry) = 'DE' then 'Germany'
        when trim(cntry) in ('US','USA') then 'United States'
        when trim(cntry) = '' or cntry is null then 'n/a'
        else trim(cntry)
    end as cntry
from bronze.erp_loc_a101;

-- Data Standardization & Consistency
select distinct cntry,
    case when trim(cntry) = 'DE' then 'Germany'
        when trim(cntry) in ('US','USA') then 'United States'
        when trim(cntry) = '' or cntry is null then 'n/a'
        else trim(cntry)
    end as new_cntry
from bronze.erp_loc_a101
order by cntry;

insert into silver.erp_loc_a101 (
    cid,
    cntry
)
select 
    replace(cid,'-','') as cid,
    case when trim(cntry) = 'DE' then 'Germany'
        when trim(cntry) in ('US','USA') then 'United States'
        when trim(cntry) = '' or cntry is null then 'n/a'
        else trim(cntry)
    end as cntry -- Normalize and handle missing or blank country codes
from bronze.erp_loc_a101;

select *
from silver.erp_loc_a101;


----------------------------------
-- silver.erp_px_cat_g1v2
----------------------------------

select 
    id,
    cat,
    subcat,
    maintenance
from bronze.erp_px_cat_g1v2;

-- Check for unwanted spaces

select *
from bronze.erp_px_cat_g1v2
where cat <> trim(cat)
or subcat <> trim(subcat)
or maintenance <> trim(maintenance);


-- Data standardization and consistency
select distinct cat
from bronze.erp_px_cat_g1v2;

select distinct subcat
from bronze.erp_px_cat_g1v2;

select distinct maintenance
from bronze.erp_px_cat_g1v2; 


insert into silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
)
select 
    id,
    cat,
    subcat,
    maintenance
from bronze.erp_px_cat_g1v2;
