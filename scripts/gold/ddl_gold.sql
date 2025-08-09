/*
DDL Script: Create Gold Views

Script Purporse:
  This script creates thw views for the Gold layer in the data warehouse.
  The Gold layer represents the final dimensions and fact tables (Star Schema).

  Each view performs transformations and combines data from the Silver layer to
  produce a clean, enriched, and business ready dateset.

Usage:
  - These views can be queried directly for analytics and reporting.

*/

----------------------------------
-- dim_customer
----------------------------------

create view gold.dim_customer as (
    select
        row_number() over (order by ci.cst_id) as customer_key,
        ci.cst_id as customer_id,
        ci.cst_key as customer_number,
        ci.cst_firstname as first_name,
        ci.cst_lastname as last_name,
        la.cntry as country,
        ci.cst_marital_status as marital_status,
        case                                            -- CRM is the master for gender
            when ci.cst_gndr  <> 'n/a' then ci.cst_gndr 
            else coalesce(ca.gen,'n/a') 
            end as gender,
        ca.bdate as birthdate,
        ci.cst_create_date as create_date,
    from silver.crm_cust_info ci
    left join silver.erp_cust_az12 ca
    on ci.cst_key = ca.cid
    left join silver.erp_loc_a101 la
    on ci.cst_key = la.cid
);



------------------------------
-- dim_product
------------------------------

create view gold.dim_product as (
    select 
        row_number() over (order by pn.prd_start_dt, pn.prd_key) as product_key,
        pn.prd_id as product_id,
        pn.prd_key as product_number,
        pn.prd_nm as product_name,
        pn.cat_id as category_id,
        pc.cat as category,
        pc.subcat as subcategory,
        pc.maintenance,
        pn.prd_cost as cost,
        pn.prd_line as product_line,
        pn.prd_start_dt as start_date
    from silver.crm_prd_info pn
    left join silver.erp_px_cat_g1v2 pc
    on pn.cat_id = pc.id
    where prd_end_dt is null
);


--------------------------------
-- fact_sales
--------------------------------

create view gold.fact_sales as (
    select 
        sd.sls_ord_num as order_number,
        pr.product_key, -- This is a replacement for sd.sls_prd_key
        dc.customer_key, -- This is a replacement for sd.sls_cust_id,
        sd.sls_order_dt as order_date,
        sd.sls_ship_dt as shipping_date,
        sd.sls_due_dt as due_date,
        sd.sls_sales as sales_amount,
        sd.sls_quantity as quantity,
        sd.sls_price as price
    from silver.crm_sales_details sd
    left join gold.dim_product pr
    on sd.sls_prd_key = pr.product_number
    left join gold.dim_customer dc
    on sd.sls_cust_id = dc.customer_id
);
