create table flashback_table as
select level lvl
from dual
connect by level <= 2;

select lvl from flashback_table;

select dbms_flashback.get_system_change_number SCN, to_char (current_timestamp, 'DD-MON-RR HH24:MI:SS') TS
from dual;

delete from flashback_table where lvl=1;
commit;

select lvl from flashback_table;
select lvl from flashback_table as of scn 11035532336948;
select lvl from flashback_table as of timestamp to_timestamp ('08-JUL-15 16:43:26', 'DD-MON-RR HH24:MI:SS');

drop table flashback_table purge;

