SELECT current_database() as db_name, main.schema_name, main.table_name, '1_main' as query_name, ROW_TO_JSON(main)::jsonb 
- 'schema_name'
- 'table_name'
AS data FROM (
SELECT 
  table_schema as schema_name,
  table_name as table_name, 
  (
    SELECT 
      td.description 
    FROM 
      pg_catalog.pg_description td 
    WHERE 
      td.objoid = a.attrelid 
      AND td.objsubid = 0
  ) as "table_comment",
  ordinal_position as column_position, 
  column_name as column_name, 
  CASE WHEN ad.description is not null THEN ad.description ELSE '' END as "column_comment", 
  udt_name as datatype, 
  character_maximum_length as datatype_length, 
  CASE WHEN a.attnotnull = false THEN 'NOT NULL' ELSE '' END as is_required, 
  CASE WHEN a.attnum IN(
    SELECT 
      UNNEST(cn.conkey) 
    FROM 
      pg_catalog.pg_constraint cn 
    WHERE 
      cn.conrelid = a.attrelid 
      AND cn.contype LIKE 'p'
  ) THEN 'PK' ELSE '' END as "PK", 
  CASE WHEN a.attnum IN(
    SELECT 
      UNNEST(cn.conkey) 
    FROM 
      pg_catalog.pg_constraint cn 
    WHERE 
      cn.conrelid = a.attrelid 
      AND cn.contype LIKE 'f'
  ) THEN 'FK' ELSE '' END as "FK"
  , CASE WHEN ic.column_default is not null THEN ic.column_default ELSE '' END as default_value
  --, a.*
  FROM pg_catalog.pg_attribute a 
  INNER JOIN pg_catalog.pg_class c ON a.attrelid = c.oid 
  INNER JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid 
  LEFT OUTER JOIN pg_catalog.pg_description ad ON ad.objoid = a.attrelid AND ad.objsubid = a.attnum 
  INNER JOIN information_schema.columns ic ON ic.table_schema = n.nspname
	AND ic.table_name = c.relname AND ic.ordinal_position = a.attnum 
WHERE 
  a.attnum > 0 AND  c.reltype <> 0 
  AND n.nspname <> 'information_schema' AND n.nspname <> 'pg_catalog'
  --AND n.nspname = public' 
ORDER BY 
  n.nspname,
  c.relname, 
  a.attnum) main
 
UNION ALL
  
SELECT current_database() as db_name, fks.schema_name, fks.table_name, '2_fks' as query_name,  ROW_TO_JSON(fks)::jsonb 
- 'schema_name'
- 'table_name'
AS data FROM (
SELECT
    distinct CASE
      WHEN tc.constraint_type = 'FOREIGN KEY'  THEN ccu.column_name 
      WHEN tc.constraint_type = 'PRIMARY KEY' THEN NULL 
	END AS foreign_column_name,
	tc.constraint_catalog,
    tc.table_schema AS schema_name, 
    tc.table_name AS table_name,
	tc.constraint_type,
    tc.constraint_name,
    kcu.column_name,
	kcu.ordinal_position,
    ccu.table_name AS foreign_table_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON kcu.constraint_name = tc.constraint_name
    AND kcu.table_schema = tc.table_schema
JOIN information_schema.constraint_column_usage AS ccu
   ON ccu.constraint_name = tc.constraint_name
LEFT OUTER JOIN information_schema.columns AS c
    ON c.table_schema = tc.table_schema AND c.table_name=kcu.table_name AND c.ordinal_position=kcu.position_in_unique_constraint
WHERE (tc.constraint_type = 'FOREIGN KEY' OR tc.constraint_type = 'PRIMARY KEY')
    --AND tc.table_schema='public'
    --AND tc.table_name='mytable';
	--AND c.table_name NOT LIKE 'from%'
ORDER BY tc.table_schema,tc.table_name,tc.constraint_type desc,kcu.ordinal_position,kcu.column_name,foreign_table_name,foreign_column_name ) fks

UNION ALL

SELECT current_database() as db_name, idx.schema_name, idx.table_name, '3_idx' as query_name,  ROW_TO_JSON(idx)::jsonb 
- 'schema_name'
- 'table_name'
AS data FROM (
SELECT 
  tnsp.nspname AS schema_name, 
  trel.relname AS table_name, 
  irel.relname AS index_name, 
  a.attname AS column_name, 
  1 + Array_position(i.indkey, a.attnum) AS column_position, 
  CASE o.OPTION & 1 WHEN 1 THEN 'DESC' ELSE 'ASC' END AS order
  --,o.OPTION
FROM pg_index AS i
join pg_class AS trel ON trel.oid = i.indrelid
join pg_namespace AS tnsp ON trel.relnamespace = tnsp.oid
join pg_class AS irel ON irel.oid = i.indexrelid
cross join lateral unnest (i.indkey) WITH ordinality AS c (colnum, ordinality)
left join lateral unnest (i.indoption) WITH ordinality AS o (OPTION, ordinality) ON c.ordinality = o.ordinality
join pg_attribute AS a ON trel.oid = a.attrelid AND a.attnum = c.colnum
--WHERE tnsp.nspname='public' -- можно заменить
--AND trel.relname='test1' -- тоже можно заменить
GROUP BY tnsp.nspname, trel.relname, irel.relname, a.attname, array_position(i.indkey, a.attnum), o.OPTION 
ORDER BY schema_name, table_name, index_name, column_position) idx

Order by db_name, schema_name, table_name, query_name