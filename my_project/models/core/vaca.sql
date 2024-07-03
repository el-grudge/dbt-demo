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