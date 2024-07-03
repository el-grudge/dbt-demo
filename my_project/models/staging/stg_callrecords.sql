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