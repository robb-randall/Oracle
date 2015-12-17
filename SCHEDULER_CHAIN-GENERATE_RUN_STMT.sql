select
  q'~EXEC DBMS_SCHEDULER.RUN_CHAIN ('~'||chain_name||q'~','~'||start_steps||q'~',NULL);~' run_chain
from
(
  select
    '"'||ascs.owner||'"."'||ascs.chain_name||'"' chain_name,
    listagg (ascs.step_name, ',') within group (order by ascs.step_name) start_steps
  from all_scheduler_chain_steps ascs
  join all_scheduler_chain_rules ascr
    on regexp_like (ascr.action, ascs.step_name)
    and ascs.owner=ascr.owner
    and ascs.chain_name=ascr.chain_name
  where ascs.owner='&chain_owner'
    and ascs.chain_name='&chain_name'
    and upper (ascr.condition)='TRUE'
  group by
    ascs.owner,
    ascs.chain_name
);
