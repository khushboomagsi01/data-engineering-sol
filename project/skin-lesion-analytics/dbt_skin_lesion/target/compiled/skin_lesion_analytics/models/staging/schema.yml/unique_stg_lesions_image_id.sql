
    
    

select
    image_id as unique_field,
    count(*) as n_records

from "skin_lesions"."main"."stg_lesions"
where image_id is not null
group by image_id
having count(*) > 1


