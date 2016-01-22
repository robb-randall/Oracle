select
  tablespace_name,
  df.datafiles,
  nvl(o.schemas,0) schemas,
  nvl(o.objects,0) objects,
  nvl(round((o.objects_size/df.maxsize_bytes)*100, 2),0)||'%' pct_used_of_max, -- % used of maxsize
  nvl(round((o.objects_size/df.size_bytes)*100, 2),0)||'%' pct_used_in_file, -- % used of objects within the filesize
  round((df.size_bytes/df.maxsize_bytes)*100, 2)||'%' pct_allocated_of_max, -- % of file size of maxsize
  round(df.maxsize_bytes,2) maxsize_bytes,
  round(df.maxsize_bytes/1024/1024,2) maxsize_mb,
  round(df.maxsize_bytes/1024/1024/1024,2) maxsize_gb,
  round(df.size_bytes,2) filesize_bytes,
  round(df.size_bytes/1024/1024,2) filesize_mb,
  round(df.size_bytes/1024/1024/1024,2) filesize_gb,
  nvl(round(o.objects_size,2),0) objects_size_bytes,
  nvl(round(o.objects_size/1024/1024,2),0) objects_size_mb,
  nvl(round(o.objects_size/1024/1024/1024,2),0) objects_size_gb,
  nvl(o.lobindex_bytes,0) lobindex_bytes,
  nvl(o.index_part_bytes,0) index_part_bytes,
  nvl(o.table_subpart_bytes,0) table_subpart_bytes,
  nvl(o.rollback_bytes,0) rollback_bytes,
  nvl(o.table_part_bytes,0) table_part_bytes,
  nvl(o.nested_table_bytes,0) nested_table_bytes,
  nvl(o.lob_part_bytes,0) lob_part_bytes,
  nvl(o.lobsegment_bytes,0) lobsegment_bytes,
  nvl(o.index_bytes,0) index_bytes,
  nvl(o.table_bytes,0) table_bytes,
  nvl(o.type2_undo_bytes,0) type2_undo_bytes,
  nvl(o.cluster_bytes,0) cluster_bytes
from (
  select
    tablespace_name,
    count(1) datafiles,
    sum(bytes) size_bytes,
    sum(maxbytes) maxsize_bytes
  from dba_data_files
  group by tablespace_name
  union all
  select
    tablespace_name,
    count(1) datafiles,
    sum(bytes) size_bytes,
    sum(maxbytes) maxsize_bytes
  from dba_temp_files
  group by tablespace_name
) df
left join (
  select
    a.*,
    (
      LOBINDEX_bytes +
      INDEX_PART_bytes +
      TABLE_SUBPART_bytes +
      ROLLBACK_bytes +
      TABLE_PART_bytes +
      NESTED_TABLE_bytes +
      LOB_PART_bytes +
      LOBSEGMENT_bytes +
      INDEX_bytes +
      TABLE_bytes +
      TYPE2_UNDO_bytes +
      CLUSTER_bytes
    ) objects_size
  from (
    select
      tablespace_name,
      count(distinct owner) schemas,
      SUM (
        LOBINDEX_objects +
        INDEX_PART_objects +
        TABLE_SUBPART_objects +
        ROLLBACK_objects +
        TABLE_PART_objects +
        NESTED_TABLE_objects +
        LOB_PART_objects +
        LOBSEGMENT_objects +
        INDEX_objects +
        TABLE_objects +
        TYPE2_UNDO_objects +
        CLUSTER_objects
      ) objects,
      SUM(nvl(LOBINDEX_BYTES,0)) LOBINDEX_BYTES,
      SUM(nvl(INDEX_PART_BYTES,0)) INDEX_PART_BYTES,
      SUM(nvl(TABLE_SUBPART_BYTES,0)) TABLE_SUBPART_BYTES,
      SUM(nvl(ROLLBACK_BYTES,0)) ROLLBACK_BYTES,
      SUM(nvl(TABLE_PART_BYTES,0)) TABLE_PART_BYTES,
      SUM(nvl(NESTED_TABLE_BYTES,0)) NESTED_TABLE_BYTES,
      SUM(nvl(LOB_PART_BYTES,0)) LOB_PART_BYTES,
      SUM(nvl(LOBSEGMENT_BYTES,0)) LOBSEGMENT_BYTES,
      SUM(nvl(INDEX_BYTES,0)) INDEX_BYTES,
      SUM(nvl(TABLE_BYTES,0)) TABLE_BYTES,
      SUM(nvl(TYPE2_UNDO_BYTES,0)) TYPE2_UNDO_BYTES,
      SUM(nvl(CLUSTER_BYTES,0)) CLUSTER_BYTES
    from (
      select
        tablespace_name,
        segment_type,
        owner,
        bytes
      from dba_segments
    )
    pivot (
      count(1) objects,
      sum(bytes) bytes
      for segment_type in (
        'LOBINDEX' as "LOBINDEX",
        'INDEX PARTITION' as "INDEX_PART",
        'TABLE SUBPARTITION' as "TABLE_SUBPART",
        'ROLLBACK' as "ROLLBACK",
        'TABLE PARTITION' as "TABLE_PART",
        'NESTED TABLE' as "NESTED_TABLE",
        'LOB PARTITION' as "LOB_PART",
        'LOBSEGMENT' as "LOBSEGMENT",
        'INDEX' as "INDEX",
        'TABLE' as "TABLE",
        'TYPE2 UNDO' as "TYPE2_UNDO",
        'CLUSTER' as "CLUSTER"
      )
    )
    group by tablespace_name
  ) a
) o
  using (tablespace_name)
;
