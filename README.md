# my dbt project

Welcome to dbt demo project!  

### Demo 1
**create a virutal environment**  
```shell
python -m venv dbt-env              # create the environment  
```

**activate the virtual environment**  
```shell
dbt-env\Scripts\activate            # activate the environment for Windows  
```

**install dbt and postgres adaptor**   
```shell
python -m pip install dbt-core dbt-postgres   # install dbt and the appropriate adaptors
```

**validate installation**  
```shell
dbt --version  
```

**change dbt environment file to point to the location where the  `profiles.yml` file will be created. By default, dbt will use the user home directory**
```shell
$env:DBT_PROFILES_DIR="C:\Users\mina.sonbol\Documents\dbt_projects\dbt_demo_1"
```  

**save the db password as an environment variable**   
```shell
$env:DB_PASSWORD = "sample_password"  
echo $env:DB_PASSWORD  
# change password to ${DB_PASSWORD} in profiles.yml  
```

**modify the `profile.yml` file, and setup a prod target**   
```shell
dbt init my-project
```

```yml
my_dbt_demo:  
  target: dev
  outputs:   
    dev:  
      type: postgres  
      host: localhost  
      port: 5432  
      user: sample_user  
      pass: ${DB_PASSWORD}  
      dbname: new_sample_db  
      schema: dev  
      threads: 1  
        
    prod:  
      type: postgres  
      host: localhost  
      port: 5432  
      user: sample_user  
      pass: ${DB_PASSWORD}  
      dbname: new_sample_db  
      schema: prod  
      threads: 1  
```
 
**test connection**  
```shell
dbt debug --target dev  
dbt debug --target prod  
```

**double check the profile used in the `dbt_project.yml` file, and add the models under the models section**
```yml

# Name your project! Project names should contain only lowercase characters
# and underscores. A good package name should reflect your organization's
# name or the intended use of these models
name: 'dbt_demo_1'
version: '1.0.0'

# This setting configures which "profile" dbt uses for this project.
profile: 'dbt_demo_1'

# These configurations specify where dbt should look for different types of files.
# The `model-paths` config, for example, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"


# Configuring models
# Full documentation: https://docs.getdbt.com/docs/configuring-models

# In this example config, we tell dbt to build all models in the example/
# directory as views. These settings can be overridden in the individual model
# files using the `{{ config(...) }}` macro.
models:
  dbt_demo_1:
    # Config indicated by + and applies to all files under models/example/
    staging:
      +materialized: table
    core:
      +materialized: table
    reporting:
      +materialized: table
      +docs:
          node_color: "red"
```
**create a new directory under models called "Staging"**  

**create a new file called `schema.yml` under the staging directory, and define the sources**
```yml
version: 2

sources:
  - name: staging
    database: new_sample_db
    schema: dev

    tables:
      - name: callrecords
      - name: outcomes

models:
  - name: stg_callrecords
    description: ""
  - name: stg_outcomes
    description: ""
```
**create a new file called `stg_callrecords.sql` under the staging directory**
```sql
with cte_callrecords as (
    select 
        *,
        row_number() over (partition by call_id::int) as rn 
    from {{ source('staging', 'callrecords') }}
    where starttime is not null
)
select
    -- identifiers
    cast(call_id as integer) as call_id,
    cast(emp_id as integer) as emp_id,
    cast(cust_id as varchar(5)) as cust_id,
    -- benchmark
    cast(benchmark as integer) as benchmark,
    -- agent site
    cast(agent_site as varchar(5)) as agent_site,
    -- timestamps
    cast(starttime as timestamp) as starttime,
    cast(endtime as timestamp) as endtime,
    now() as insert_time
from cte_callrecords
where rn = 1

-- dbt build --select <model.sql> --vars '{is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
``` 
**create a new file called `stg_outcomes.sql` under the staging directory**
```sql
with cte_outcomes as (
    select *
    from {{ source('staging', 'outcomes') }}
)
select 
    -- identifiers
    cast(sale_id as integer) as sale_id,
    cast(emp_id as integer) as emp_id,
    cast(cust_id as varchar(5)) as cust_id,
    -- outcome
    cast(sale_flag as integer) as sale_flag,
    -- timestamps
    cast(sale_time as timestamp) as sale_time,
    now() as insert_time
from cte_outcomes

-- dbt build --select <model.sql> --vars '{is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
```
**create a new directory under models callded "Core"**  

**create a new file called `schema.yml` under the core directory**
```yml
version: 2

models:
  - name: aca
    description: ""
  - name: vaca
    description: ""
```
**create a new file called `aca.sql` under the core directory**
```sql
with cte_calls as (
    select c.*
    from {{ ref('stg_callrecords') }} c 
), cte_outcomes as (
    select s.*
    from {{ ref('stg_outcomes') }} s 
)
    select 
        c.*, 
        s.sale_id , 
        s.sale_flag , 
        s.sale_time 
    from cte_calls c 
    left join cte_outcomes s 
    on c.emp_id = s.emp_id 
    and c.cust_id = s.cust_id 
    and s.sale_time >= c.starttime and s.sale_time <= c.endtime 

-- dbt build --select <model.sql> --vars '{is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
```
**create a new file called `vaca.sql` under the core directory**
```sql
{{ 
    config(
        materialized='view',
        docs={'node_color': 'purple'}
      ) 
}}

select *
from {{ref('aca')}}

-- dbt build --select <model.sql> --vars '{is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
```
**create a new directory under models called "Reporting"**  

**create a new file called `schema.yml` under the reporting directory**
```yml
version: 2

models:
  - name: portal
    description: ""
  - name: site_analysis
    description: ""
  - name: site_analysis_pivot
    description: ""
```
**create a new file called `portal.sql` under the reporting directory**
```sql
with cte_aca as (
    select 
        *
    from {{ ref('vaca') }} aca
), cte_portal as (
    select
      date_trunc('day',starttime)::date as calldate, 
    	sum(case when benchmark=1 then 1 else 0 end) as on_calls ,
    	sum(case when benchmark=0 then 1 else 0 end) as off_calls ,
    	sum(case when benchmark=1 then sale_flag else 0 end) on_sales,
    	sum(case when benchmark=0 then sale_flag else 0 end) off_sales
    from cte_aca
    group by 1
), cte_incr as (
    select 
    	calldate,
    	on_calls,
    	off_calls,
    	on_calls+off_calls as total_calls,
    	on_sales,
    	off_sales,
    	on_sales+off_sales as total_sales,
    	on_sales::float / on_calls::float as on_spc,
    	off_sales::float / off_calls::float as off_spc,
    	((on_sales::float / on_calls::float)-(off_sales::float / off_calls::float)) as lift,
    	((on_sales::float / on_calls::float)-(off_sales::float / off_calls::float))*on_calls::float as incremental
    from cte_portal
)
select * 
from cte_incr

-- dbt build --select <model.sql> --vars '{is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
```
**create a new file called `site_analysis.sql` under the reporting directory**
```sql
with cte_aca as (
    select 
      *
    from {{ ref('vaca') }} aca
), cte_portal as (
    select
      date_trunc('day',starttime)::date as calldate, 
      agent_site,
    	sum(case when benchmark=1 then 1 else 0 end) as on_calls ,
    	sum(case when benchmark=0 then 1 else 0 end) as off_calls ,
    	sum(case when benchmark=1 then sale_flag else 0 end) on_sales,
    	sum(case when benchmark=0 then sale_flag else 0 end) off_sales
    from cte_aca
    group by 1, 2
), cte_incr as (
    select 
    	calldate,
      agent_site,
    	on_calls,
    	off_calls,
    	on_calls+off_calls as total_calls,
    	on_sales,
    	off_sales,
    	on_sales+off_sales as total_sales,
    	coalesce(on_sales::float / nullif(on_calls::float, 0), 0) as on_spc,
    	coalesce(off_sales::float / nullif(off_calls::float, 0), 0) as off_spc,
    	coalesce(((on_sales::float / nullif(on_calls::float, 0))-(off_sales::float / nullif(off_calls::float, 0))), 0) as lift,
    	coalesce(((on_sales::float / nullif(on_calls::float, 0))-(off_sales::float / nullif(off_calls::float, 0)))*on_calls::float, 0)::float as incremental
    from cte_portal
)
select * 
from cte_incr

-- dbt build --select <model.sql> --vars '{is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
```
**create a new file called `site_analysis_pivot.sql` under the directory folder**
```sql
{% set categories = dbt_utils.get_column_values(ref('site_analysis'), 'calldate') %}

with pivoted_view as (
    select 
        agent_site,
        {{ dbt_utils.pivot(
            'calldate',
            categories,
            agg='array_agg',
            then_value='incremental',
            else_value='NULL') }}
    from {{ ref('site_analysis') }}
    group by 1
)
select 
    agent_site,
    {% for category in categories %}
    (array_remove("{{ category }}",NULL))[1] as "{{ category }}"
    {% if not loop.last %}, {% endif %}
    {%endfor%}
from pivoted_view

-- dbt build --select <model.sql> --vars '{is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
```

**setup `packages.yml` file**
```yml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1

  - package: dbt-labs/codegen
    version: 0.12.1
```

**run dbt commands**
```shell
dbt deps
dbt build
dbt docs generate
dbt docs serve
```

### Demo 2  

**add documentation to `schema.yml` under the staging directory**
```yml
version: 2

sources:
  - name: staging
    database: new_sample_db
    schema: dev

    tables:
      - name: callrecords
        description: >
          Call records data containing a detailed record that is generated for each call that arrives at a peripheral Contact Center environment. 
          The TCD record contains comprehensive information about the call, including customer and agent IDs, start time, end time, and agent site.
          Benchmark data is appended to the call record data by the client and shared in the same CSV file. 
          Data is shared daily as a csv file with 7 columns.
      
      - name: outcomes
        description: >
          Sales records as generated by the point-of-sale system for each sale interaction. The interaction may or may not end in an actual.
          A sale record should always have a unique identifier and should be attributed to an agent through the emp_id, and to a customer through cust_id.
          The sale flag indicates whether the sale interaction ended in a sale or not. This will be used as our binomial metric.
          Data is shared daily as a csv file with 5 columns.

models:
  - name: stg_callrecords
    description: >
      Staging of the call records data where the data is type case to its correct format as illustrated below in the column data types,
      and an insert_time column is created to indicate when the data was staged. 
    columns:
      - name: call_id
        data_type: integer
        description: Primary key for this table, generated by the peripheral switch system

      - name: emp_id
        data_type: integer
        description: The ID of the agent that answered the call 

      - name: cust_id
        data_type: character varying
        description: The ID of the customer who made the call 

      - name: benchmark
        data_type: integer
        description: Afiniti benchmark appended by the client

      - name: starttime
        data_type: timestamp without time zone
        description: Call start time

      - name: endtime
        data_type: timestamp without time zone
        description: Call end time

      - name: agent_site
        data_type: character varying
        description: >
          A code indicating the agent site that received the call
          A = Northeast
          B = Southeast
          C = Midwest     
          D = Southwest
          E = West Coast

      - name: insert_time
        data_type: timestamp without time zone
        description: Data staging time

  - name: stg_outcomes
    description: >
      Staging of the outcomes data where the data is type case to its correct format as illustrated below in the column data types, 
      and an insert_time column is created to indicate when the data was staged. 
    columns:
      - name: sale_id
        data_type: integer
        description: Primary key for this table, generated by point-of-sale system to identify sale

      - name: emp_id
        data_type: integer
        description: The ID of the agent that answered the call 

      - name: cust_id
        data_type: character varying
        description: The ID of the customer who made the call 

      - name: sale_flag
        data_type: integer
        description: The sale flag indicates whether an interaction ended in a sale (1) or not (0). All interactions must have a sale_flag value

      - name: sale_time
        data_type: timestamp without time zone
        description: The timestamp of the sale interaction

      - name: insert_time
        data_type: timestamp without time zone
        description: Data staging time        
```

**add a freshness test to `schema.yml` under the staging directory**
```yml
        freshness:
          warn_after: {count: 24, period: hour}
          # error_after: {count: 48, period: hour}
        loaded_at_field: sale_time::timestamp
```
```shell
dbt source freshness
```
Note: Freshness can be tested on database and tables, with or without a loaded_at_field.

**define a data test: primary key (unique and not_null) to `schema.yml` under the staging directory**
```yml
        data_tests:
          - unique:
              severity: warn
          - not_null:
              severity: error
              # severity: warn
```
```shell
dbt test --select test_type:data
```  
Also, model build will fail (just make sure to set the is_test_var to false)   
```shell
dbt build --select stg_callrecords --vars '{is_test_var: false}'
```

**define a data test: accepted values to `schema.yml` under the staging directory**
```yml
        data_tests:
          - accepted_values:
              values: ['A','B','C','D','E']
              severity: warn
```

**version control with git**  

**run with test_vars set to false, and deploy to prod**
```shell
dbt build --target prod --vars '{is_test_run: false}'
```

***

### git commands  
```shell
echo "# dbt-demo" >> README.md  
git init  
git add README.md  
git commit -m "first commit"  
git branch -M main  
git remote add origin https://github.com/el-grudge/dbt-demo.git  
git push -u origin main  
```  
***

### dbt commands  
```shell
dbt build  
dbt test  
dbt run  
dbt docs generate  
dbt docs serve  
```

### Resources:  
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)  
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers  
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support  
- Find [dbt events](https://events.getdbt.com) near you  
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices  
