
  
  create view "skin_lesions"."main"."diagnosis_summary__dbt_tmp" as (
    with lesions as (
    select * from "skin_lesions"."main"."stg_lesions"
)

select
    diagnosis_code,
    diagnosis_name,
    count(*)                                                    as total_cases,
    round(count(*) * 100.0 / sum(count(*)) over (), 2)         as pct_of_total,
    round(avg(patient_age), 1)                                  as avg_age,
    count(distinct lesion_id)                                   as unique_lesions
from lesions
group by diagnosis_code, diagnosis_name
order by total_cases desc
  );
