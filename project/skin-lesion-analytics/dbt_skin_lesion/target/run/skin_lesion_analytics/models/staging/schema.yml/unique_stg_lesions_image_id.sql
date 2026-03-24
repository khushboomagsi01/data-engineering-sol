
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    image_id as unique_field,
    count(*) as n_records

from "skin_lesions"."main"."stg_lesions"
where image_id is not null
group by image_id
having count(*) > 1



  
  
      
    ) dbt_internal_test