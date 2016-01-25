create or replace package metadata as 

/*******************************************************************************
 *** GET_OBJECT_DDL
 * Returns a CLOB of the DDL for the object specified.
 */
function get_object_ddl
(
  db_link         in user_db_links.db_link%type,
  object_type     in all_objects.object_type%type,
  object_owner    in all_objects.owner%type,
  object_name     in all_objects.object_name%type
) return clob;

/*******************************************************************************
 *** GET_TABLESPACE_DDL
 * Returns a CLOB of the DDL for the tablespace specified.
 */
function get_tablespace_ddl
(
  db_link in user_db_links.db_link%type,
  tablespace_name in user_tablespaces.tablespace_name%type
) return clob;

/*******************************************************************************
 *** GET_USER_DDL
 * Returns a CLOB of the DDL for the user specified.
 */
function get_user_ddl
(
  db_link         in user_db_links.db_link%type,
  username in user_users.username%type
) return clob;

end metadata;
