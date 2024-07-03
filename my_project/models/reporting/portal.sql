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