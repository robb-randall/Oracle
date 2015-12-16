/*
:datapump_type ......... IMPDP or EXPDP
:login_credentials ..... Login credentials "USR/PWD@TNS" or can be null if you want to be prompted
:directory ............. Datapump directory
:dumpfile .............. Dumpfile name (with extension)
:logfile ............... Logfile name without extension (_[%grpno].log is appended)
:additional_params ..... Any additional parameters needed (ex. TABLE_EXISTS_ACTION=REPLACE)
*/

select
  :datapump_type||' '||:login_credentials||' '||
  'DIRECTORY='||:directory||' '||
  'DUMPFILE='||:dumpfile||' '||
  'LOGFILE='||:logfile||'_'||grp||'.log '||
  'TABLES='||trim(leading ',' from tbls)||' '||
  :additional_params
from (
  select
    grp,
    xmlagg(xmlelement("x", ','||owner||'.'||table_name) order by table_name).extract('//text()') tbls
  from (
    select
      mod(rownum, 5) grp, -- Number of groups
      owner,
      table_name
    from dba_tables
    where owner=:owner
    and table_name like nvl(:table_name_like, '%')
  )
  group by grp
);




