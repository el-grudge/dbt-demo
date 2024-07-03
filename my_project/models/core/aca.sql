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
    -- join on agent, customer, and sale time between call start and call end
    on c.emp_id = s.emp_id 
    and c.cust_id = s.cust_id 
    and s.sale_time >= c.starttime and s.sale_time <= c.endtime 

-- dbt build --select <model.sql> --vars '{is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}