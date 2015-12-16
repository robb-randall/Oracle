--select * from bi_prod.lu_project_control where data_center_id=298 and project_id='CU15';
--
--DATA_CENTER_SK=643
--DATA_CENTER_ID=298
--PROJECT_CONTROL_SK=53764
--PROJECT_ID=CU15;

set define off;

SELECT
  xmlagg (xmlelement ("x", to_clob (OWNER||'.'||TABLE_NAME||',')) order by 1).extract('//text()') TABLES,
  xmlagg (xmlelement ("x", to_clob (chr(13)||'QUERY='||OWNER||'.'||TABLE_NAME||':"WHERE ('||COLS||') IN (SELECT '||COLS||' FROM BI_PROD.LU_PROJECT_CONTROL WHERE PROJECT_CONTROL_SK=53764)"')) order by 1).extract('//text()') queries,
  xmlagg (xmlelement ("x", to_clob (chr(13)||'DELETE FROM '||OWNER||'.'||TABLE_NAME||' WHERE ('||COLS||') IN (SELECT '||COLS||' FROM BI_PROD.LU_PROJECT_CONTROL WHERE PROJECT_CONTROL_SK=53764);')) order by 1).extract('//text()') del_stmt
FROM (
  SELECT
    OWNER,
    TABLE_NAME,
    LISTAGG (COLUMN_NAME, ',') WITHIN GROUP (ORDER BY COLUMN_NAME) COLS
  FROM ALL_TAB_COLUMNS ATC
  JOIN ALL_TABLES AT USING (OWNER, TABLE_NAME)
  WHERE OWNER='BI_PROD'
    AND COLUMN_NAME IN (
      'DATA_CENTER_SK',
      'DATA_CENTER_ID',
      'PROJECT_CONTROL_SK',
      'PROJECT_ID'
      )
  GROUP BY
    OWNER,
    TABLE_NAME
)
WHERE COLS LIKE '%PROJECT_CONTROL_SK%'
  OR COLS LIKE '%DATA%PROJECT%'
;

