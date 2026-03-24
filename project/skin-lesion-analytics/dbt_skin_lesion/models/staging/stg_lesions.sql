with source as (
    select * from {{ source('raw', 'raw_skin_lesions') }}
),

cleaned as (
    select
        lesion_id,
        image_id,
        dx              as diagnosis_code,
        diagnosis_name,
        dx_type         as diagnosis_method,
        cast(age as integer) as patient_age,
        age_group,
        sex             as patient_sex,
        localization    as body_location
    from source
)

select * from cleaned
