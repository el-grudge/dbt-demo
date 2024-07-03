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