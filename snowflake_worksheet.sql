                                    
----- create roles--

use role accountadmin;
create role Admin;
grant role Admin to role accountadmin;
create role Developer;
grant role Developer to role Admin;
create role PII;
grant role PII to role accountadmin;

-- create a medium sized warehouse named assignment_wh

create or replace warehouse assignment_wh
with warehouse_size = medium
auto_suspend = 120
auto_resume = true;
 
-- give create dbs and use warehouse priviledge to Admin role

grant create database on account to role Admin; 
grant usage on warehouse assignment_wh to role Admin;

use warehouse compute_wh;
use role Admin;

-- create database
create or replace database assignment_db;

-- create schema 

create schema my_schema;

--- create table ---
create or replace table 
assignment_db.my_schema.int_stg_customer (
    elt_ts timestamp,
    elt_by varchar,
    file_name varchar,
    FirstName string,
    LastName string,
    Company string,
    Address string,
    City string,
    County string,
    State string,
    ZIP numeric,
    Phone string,
    Fax string,
    Email string,
    Web string
);

create or replace table 
assignment_db.my_schema.ext_stg_customer (
    elt_ts timestamp,
    elt_by varchar,
    file_name varchar,
    FirstName string,
    LastName string,
    Company string,
    Address string,
    City string,
    County string,
    State string,
    ZIP numeric,
    Phone string,
    Fax string,
    Email string,
    Web string
);

-- create file format --
create or replace file format assignment_db.my_schema.csv_format 
type = csv field_optionally_enclosed_by='"' field_delimiter = ','
SKIP_HEADER = 1
null_if = ('NULL', 'null') empty_field_as_null =true;


-- put data into internal stage --
-- to run on terminal using snowsql--
/* put file:///Users/sanjeev/Desktop/snowflake/rawdata/customers.csv
@assignment_db.my_schema.%int_stg_customer; */
--- xx --


---- copy data from internal stage -----

copy into assignment_db.my_schema.int_stg_customer
from (select current_timestamp(),'local',metadata$filename file_name, t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, t.$11, t.$12 from @assignment_db.my_schema.%int_stg_customer t)
file_format = assignment_db.my_schema.csv_format
on_error = 'skip_file';

select * from int_stg_customer limit 100;

 ---- copy data from external stage ---
create or replace storage integration s3_int
    type = external_stage
    storage_provider = s3
    enabled = true
    storage_aws_role_arn = 'arn:aws:iam::132847602727:role/snowrole'
    storage_allowed_locations = ('s3://snowflake5torage/rawdata/');
    
desc integration s3_int;

create or replace stage assignment_db.my_schema.ext_stg_customer
    storage_integration = s3_int
    url = 's3://snowflake5torage/rawdata/'
    file_format = assignment_db.my_schema.csv_format;

copy into assignment_db.my_schema.ext_stg_customer
from (select current_timetsamp(),'Amazon_s3',metadata$filename file_name, t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, t.$11, t.$12 from @assignment_db.my_schema.ext_stg_customer t)
on_error = 'continue';

select * from assignment_db.my_schema.ext_stg_customer limit 100;
----------------------xx -------------

-------------- create variant dataset using ext_stg_customer table data ---

create or replace table variant_dataset (
    data variant
);

insert into variant_dataset 
    (select to_variant(object_construct(*)) as data from ext_stg_customer limit 100);

select * from variant_dataset;

--------- xx --------

-- load parquet data into stage and query it without laoding into table---
create or replace stage my_parquet_stg;

create file format parquet_format
type = parquet;

/* Load data into parquet stage - run on terminal using snowsql
    put file///Users/sanjeev/Desktop/userdata1.parquet
    @my_parquet_stg
   */
select * from table(
infer_schema(location => '@my_parquet_stg', file_format => 'parquet_format'));

select
$1:"id"::int id,
$1:"first_name"::string first_name,
$1:"last_name"::string last_name,
$1:"gender"::string gender,
$1:"email"::string email
from @assignment_db.my_schema.my_parquet_stg
(file_format => 'parquet_format');

----------

----- create masking policy for developer role ---
create or replace masking policy assignment_db.my_schema.dev_mask as (val string) returns string ->
  case
    when current_role() in ('DEVELOPER') then '-----'
    else val
  end;

GRANT SELECT ON TABLE assignment_db.my_schema.ext_stg_customer TO ROLE DEVELOPER;
GRANT USAGE ON WAREHOUSE ASSIGNMENT_WH TO ROLE DEVELOPER;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE DEVELOPER;
GRANT USAGE ON SCHEMA my_schema TO ROLE Developer;

alter table if exists assignment_db.my_schema.ext_stg_customer modify column email set masking policy 
assignment_db.my_schema.dev_mask;

alter table if exists assignment_db.my_schema.ext_stg_customer modify column phone set masking policy 
assignment_db.my_schema.dev_mask;

alter table if exists assignment_db.my_schema.ext_stg_customer modify column web set masking policy 
assignment_db.my_schema.dev_mask;

use role developer;
select * from assignment_db.my_schema.ext_stg_customer;

---------- xx --------------
-- check data on PII role---

GRANT SELECT ON TABLE assignment_db.my_schema.ext_stg_customer TO ROLE PII;
GRANT USAGE ON WAREHOUSE ASSIGNMENT_WH TO ROLE PII;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE PII;
GRANT USAGE ON SCHEMA my_schema TO ROLE PII;
use role PII;
select * from assignment_db.my_schema.ext_stg_customer;
