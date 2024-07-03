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