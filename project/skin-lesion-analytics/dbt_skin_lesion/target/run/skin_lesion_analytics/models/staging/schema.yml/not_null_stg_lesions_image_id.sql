
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select image_id
from "skin_lesions"."main"."stg_lesions"
where image_id is null



  
  
      
    ) dbt_internal_test