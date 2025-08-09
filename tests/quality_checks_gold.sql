/*

Quality Checks

Script Purpose:
  This script perdorms quality checks to validate the integrity, consistency
  and accuracy of the Gold layer. These checks ensure:
  - Uniqueness of surrogate keys in dimension tables.
  - Referential integrity between fact and dimension tables.
  - Validation of relationships in the data model for analytical purposes.

Usage Notes:
  - Run these checks after loading Silver layer.
  - Investigate and resulve any discrepancies found during the checks.

*/


-----------------------------
-- dim_customer
-----------------------------

-- When joining multiple tables, we sometimes can get
-- duplicate data, so it's always good to check
select cst_id, count(*) 
from (
    select 
        ci.cst_id,
        ci.cst_key,
        ci.cst_firstname,
        ci.cst_lastname,
        ci.cst_marital_status,
        ci.cst_gndr,
        ci.cst_create_date,
        ca.bdate,
        ca.gen,
        la.cntry
    from silver.crm_cust_info ci
    left join silver.erp_cust_az12 ca
    on ci.cst_key = ca.cid
    left join silver.erp_loc_a101 la
    on ci.cst_key = la.cid)
group by cst_id
having count(*) > 1;

select 
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_marital_status,
    ci.cst_gndr,
    ci.cst_create_date,
    ca.bdate,
    ca.gen,
    la.cntry
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid;

-- We have two columns for gender, so we need to take a look

select distinct
    ci.cst_gndr,
    ca.gen,
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid;

-- Since we have conflicting information (where the customer is male on the CRM and female on the ERP)
-- we need to ask the stakeholders which table is the master. In this case it's the CRM.

-- It is important to rename the columns and have them be more readable for stakeholders.
-- It is also important to order the columns in a way that makes sense
-- for example, the id columns should go first, and the first_name and last_name columns should be 
-- We can generate a surrogate key for this table. This key means nothing outside of this data warehouse.

select 
    case 
        when ci.cst_gndr  <> 'n/a' then ci.cst_gndr 
        else coalesce(ca.gen,'n/a') 
        end as new_gen,
    ci.cst_gndr,
    ca.gen
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid;

------------------------------
-- dim_product
------------------------------

select 
    pn.prd_id,
    pn.cat_id,
    pn.prd_key,
    pn.prd_nm,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt,
    pc.cat,
    pc.subcat,
    pc.maintenance
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null; --filter out all historical data


-- Check on duplicate prd_keys
select prd_key, count(*) from (
    select 
        pn.prd_id,
        pn.cat_id,
        pn.prd_key,
        pn.prd_nm,
        pn.prd_cost,
        pn.prd_line,
        pn.prd_start_dt,
        pc.cat,
        pc.subcat,
        pc.maintenance
    from silver.crm_prd_info pn
    left join silver.erp_px_cat_g1v2 pc
    on pn.cat_id = pc.id
    where prd_end_dt is null --filter out all historical data
)
group by prd_key
having count(*) > 1;


--------------------------------
-- fact_sales
--------------------------------

-- Use the dimension's surrogate key instead of IDs to easily connect facts with dimensions.
-- Give the column names friendly 
-- We need to order the columns in the following way:
--  Dimension keys
--  Dates
--  Measures
