
  
  create view "skin_lesions"."main"."demographics_analysis__dbt_tmp" as (
    with lesions as (
    select * from "skin_lesions"."main"."stg_lesions"
)

select
    age_group,
    diagnosis_code,
    diagnosis_name,
    patient_sex,
    count(*) as case_count
from lesions
group by age_group, diagnosis_code, diagnosis_name, patient_sex
order by age_group, diagnosis_code
  );
