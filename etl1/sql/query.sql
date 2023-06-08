/*update*/

update core.etl1_tab1 t1
set t1.name=t2.name,
    t1.age=t2.age,
    t1.source=t2.source,
    t1.main_source = case when t2.source='A' or t2.source ='C' then 1 else 99 end,
    t1.tech_flag = if (t2.source='A', False ,True),
    t1.update_timestamp = current_datetime('Europe/Paris') 
from `stage.etl1_raw` t2 
where t1.id=t2.id
;

/*insert new records*/

insert into core.etl1_tab1 (id, name, age, source, main_source,tech_flag,insertion_timestamp )
(select * from 
    (
      SELECT  id,
              name,
              age,
              source,
              case when source='A' or source ='C' then 1 else 99 end,
              if (source='A', False ,True) as tech_flag,
              current_datetime('Europe/Paris') as insertion_timestamp
      from `stage.etl1_raw`
     ) 
  where not id in (select id from core.etl1_tab1 )
) ;

