with lesions as (
    select * from {{ ref('stg_lesions') }}
)

select
    body_location,
    diagnosis_code,
    diagnosis_name,
    count(*)                   as case_count,
    round(avg(patient_age), 1) as avg_age
from lesions
group by body_location, diagnosis_code, diagnosis_name
order by case_count desc
