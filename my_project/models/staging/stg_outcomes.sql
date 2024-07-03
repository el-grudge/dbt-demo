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