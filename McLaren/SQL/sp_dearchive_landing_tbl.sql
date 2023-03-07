CREATE OR REPLACE PROCEDURE sp_dearchive_landing_tbl_test()
RETURNS character varying(ANY)
LANGUAGE NZPLSQL AS
BEGIN_PROC
DECLARE
da_tbl_cnt bigint;
da_tbl_rec record;
tmp_tbl_column record;
formulate_columns varchar(5000);
req_columns varchar(5000);
tmp_missing_batch record;
formulate_batches varchar(5000);
missing_batch varchar(5000);
missing_btch_cnt bigint;
pk_column varchar(1000);
dst_tbl_cnt_val bigint;
src_tbl_clmn bigint;
src_tbl_cnt_val bigint;
ren_rec_cnt bigint;
log_data varchar(64000);

BEGIN

log_data:= '';
--Metadata table check begin
IF ((SELECT COUNT(1) FROM _v_relation_column WHERE name = 'dearchive_metadata') = 0) THEN
log_data:=log_data||'
"dearchive_metadata" table is missing. Without the metadata table, the dearchive proces cant be run.';
RETURN log_data;
EXIT;
ELSE

SELECT COUNT(1) INTO da_tbl_cnt FROM dearchive_metadata WHERE trim(upper(COALESCE(run_ind,'N'))) = 'Y';
--Number of dearchive tables check begin
IF (da_tbl_cnt = 0) THEN
log_data:=log_data||'
No tables are available/marked for dearchive. Check the run_ind column value in the dearchive_metadata table. A value of Y will dearchive the corresponding table.';
RETURN log_data;
EXIT;
ELSE
log_data:=log_data||'
'||da_tbl_cnt||' table(s) found to be dearchived.';

--Looping through the entire metadata table begin
FOR da_tbl_rec IN SELECT trim(upper(customer)) as customer, trim(upper(source)) as source, trim(lower(lnd_db_name)) as lnd_db_name, 
trim(lower(utl_db_name)) as utl_db_name, trim(lower(src_tbl_name)) as src_tbl_name, trim(lower(src_tbl_pk)) as src_tbl_pk, 
replace(trim(lower(src_tbl_pk)),',','|| '' | '' ||') as modified_pk, trim(lower(COALESCE(dst_tbl_name,'cv_' || trim(lower(src_tbl_name))))) as dst_tbl_name, 
trim(upper(COALESCE(run_ind,'N'))) as run_ind FROM dearchive_metadata WHERE trim(upper(COALESCE(run_ind,'N'))) = 'Y' ORDER BY src_tbl_name LOOP

log_data:=log_data||'
DEARCHIVING TABLE '||da_tbl_rec.src_tbl_name||'...';

formulate_columns:='';
--Looping through the schema table to get column list begin
EXECUTE IMMEDIATE 'CREATE TABLE src_tbl_columns AS SELECT attname, attnum FROM '||da_tbl_rec.lnd_db_name||'.._v_relation_column WHERE name = '''||da_tbl_rec.src_tbl_name||''' ORDER BY attnum;';
FOR tmp_tbl_column IN SELECT * FROM src_tbl_columns ORDER BY attnum LOOP
formulate_columns:=formulate_columns || ', ' || tmp_tbl_column.attname;
--Looping through the schema table to get column list end
END LOOP;
EXECUTE IMMEDIATE 'DROP TABLE src_tbl_columns';
req_columns:=SUBSTR(formulate_columns,2,LENGTH(formulate_columns));
--RAISE NOTICE 'REQ_Columns:%',req_columns;

pk_column:='';
IF (da_tbl_rec.customer = 'BON_SECOURS' and da_tbl_rec.source = 'EPIC' and pk_column != '') THEN
EXECUTE IMMEDIATE 'CREATE TABLE tmp_pk_col AS SELECT DISTINCT REPLACE(trim(lower(pce_cst_src_tbl_pk_col)),''|'','' || '''' | '''' || '') AS pk_col FROM '||da_tbl_rec.lnd_db_name||'..pce_hard_delete WHERE lower(trim(pce_cst_src_tbl_nm)) = '''||da_tbl_rec.src_tbl_name||''';';
SELECT pk_col INTO pk_column FROM tmp_pk_col;
EXECUTE IMMEDIATE 'DROP TABLE tmp_pk_col;';
END IF;
RAISE NOTICE 'PK_COL:%.',pk_column;

dst_tbl_cnt_val:=0;
IF ((SELECT COUNT(1) FROM _v_relation_column WHERE name = da_tbl_rec.dst_tbl_name) > 0) THEN
EXECUTE IMMEDIATE 'CREATE TABLE dst_tbl_cnt AS SELECT COUNT(1) FROM '||da_tbl_rec.dst_tbl_name||';';
SELECT * INTO dst_tbl_cnt_val FROM dst_tbl_cnt;
IF (dst_tbl_cnt_val = 0) THEN
log_data:=log_data||'
'||da_tbl_rec.dst_tbl_name||' table exists with no data. Dropping the table to avoid any DDL discrepancy.';
EXECUTE IMMEDIATE 'DROP TABLE '||da_tbl_rec.dst_tbl_name||';';
END IF;
EXECUTE IMMEDIATE 'DROP TABLE dst_tbl_cnt';
END IF;

EXECUTE IMMEDIATE 'CREATE TABLE src_tbl_clmn_cnt AS SELECT COUNT(1) FROM '||da_tbl_rec.lnd_db_name||'.._v_relation_column WHERE name = '''||da_tbl_rec.src_tbl_name||''';';
SELECT * INTO src_tbl_clmn FROM src_tbl_clmn_cnt;
EXECUTE IMMEDIATE 'DROP TABLE src_tbl_clmn_cnt;';

src_tbl_cnt_val:=0;
IF (src_tbl_clmn > 0) THEN
EXECUTE IMMEDIATE 'CREATE TABLE src_tbl_cnt AS SELECT COUNT(1) FROM '||da_tbl_rec.lnd_db_name||'..'||da_tbl_rec.src_tbl_name||';';
SELECT * INTO src_tbl_cnt_val FROM src_tbl_cnt;
EXECUTE IMMEDIATE 'DROP TABLE src_tbl_cnt;';
END IF;

--Identifying Incremental vs Historical begin
IF ((SELECT COUNT(1) FROM _v_relation_column WHERE name = da_tbl_rec.dst_tbl_name) > 0 and src_tbl_clmn > 0) THEN
--INCREMENTAL

log_data:=log_data||'
'||da_tbl_rec.dst_tbl_name||' table exists. Considering the dearchive process as increment run.';

EXECUTE IMMEDIATE 'CREATE TABLE tmp_missing_batch AS 
SELECT rcrd_btch_audt_id FROM '||da_tbl_rec.lnd_db_name||'..'||da_tbl_rec.src_tbl_name||' 
WHERE 
--rcrd_btch_audt_id IN (
--SELECT bh.batch_id 
--FROM 
--(
--SELECT * FROM '||da_tbl_rec.utl_db_name||'..batch_header 
--WHERE 
--customer = '''||da_tbl_rec.customer||'''
--AND source = '''||da_tbl_rec.source||'''
--AND status = ''COMPLETED''
--AND end_ts IS NOT NULL
--) bh
--INNER JOIN '||da_tbl_rec.utl_db_name||'..batch_detail bd ON bh.batch_id = bd.batch_id AND trim(lower(bd.dst_table_name)) = '''||da_tbl_rec.src_tbl_name||'''
--GROUP BY bh.batch_id) AND 
(
rcrd_isrt_ts > (SELECT max(rcrd_isrt_ts) max_rcrd_isrt_ts FROM '||da_tbl_rec.dst_tbl_name||')
OR
rcrd_btch_audt_id > (SELECT max(rcrd_btch_audt_id) max_rcrd_btch_audt_id FROM '||da_tbl_rec.dst_tbl_name||')
)
GROUP BY rcrd_btch_audt_id;';

SELECT COUNT(1) INTO missing_btch_cnt FROM tmp_missing_batch;

IF (missing_btch_cnt > 0) THEN

formulate_batches:='';
FOR tmp_missing_batch IN SELECT * FROM tmp_missing_batch LOOP
formulate_batches:=formulate_batches || ', ' || tmp_missing_batch.rcrd_btch_audt_id;
END LOOP;
missing_batch:=SUBSTR(formulate_batches,2,LENGTH(formulate_batches));
EXECUTE IMMEDIATE 'DROP TABLE tmp_missing_batch;';

log_data:=log_data||'
Missing_batch(s) for the table:'||da_tbl_rec.dst_tbl_name||' include: '||missing_batch;

--DELETE PREVIOUS DATA TO MAKE WAY FOR UPDATES
EXECUTE IMMEDIATE 'DELETE FROM '||da_tbl_rec.dst_tbl_name||'
WHERE ('||da_tbl_rec.src_tbl_pk||') IN 
(SELECT '||da_tbl_rec.src_tbl_pk||' FROM '||da_tbl_rec.lnd_db_name||'..'||da_tbl_rec.src_tbl_name||' WHERE rcrd_btch_audt_id IN ('||missing_batch||') GROUP BY '||da_tbl_rec.src_tbl_pk||');';
log_data:=log_data||'
Deleted matching data.';

--INSERT LATEST RECORDS IF THERE ARE MULTIPLE BATCHES
EXECUTE IMMEDIATE 'INSERT INTO '||da_tbl_rec.dst_tbl_name||'
('||req_columns||')
SELECT '||req_columns||' FROM '||da_tbl_rec.lnd_db_name||'..'||da_tbl_rec.src_tbl_name||' A JOIN (select distinct '||da_tbl_rec.src_tbl_pk||',rcrd_btch_audt_id FROM 
(SELECT *, ROW_NUMBER() OVER(PARTITION BY '||da_tbl_rec.src_tbl_pk||' ORDER BY rcrd_isrt_ts DESC, rcrd_btch_audt_id DESC) AS rn FROM '||da_tbl_rec.lnd_db_name||'..'||da_tbl_rec.src_tbl_name||' 
WHERE rcrd_btch_audt_id IN ('||missing_batch||')) src
WHERE src.rn = 1)B USING ('||da_tbl_rec.src_tbl_pk||',rcrd_btch_audt_id);';
log_data:=log_data||'
Inserted Missing data.';

IF (da_tbl_rec.customer = 'BON_SECOURS' and da_tbl_rec.source = 'EPIC' and pk_column != '') THEN
--HARD DELETE DATA REMOVAL
EXECUTE IMMEDIATE 'DELETE FROM '||da_tbl_rec.dst_tbl_name||'
WHERE ('||pk_column||') IN
(SELECT pce_cst_src_tbl_pk FROM '||da_tbl_rec.lnd_db_name||'..pce_hard_delete WHERE lower(trim(pce_cst_src_tbl_nm)) = '''||da_tbl_rec.src_tbl_name||''' and rcrd_btch_audt_id IN ('||missing_batch||'));';
END IF;

ELSE
log_data:=log_data||'
There are no missing batch(s) in the dearchive table. No action required.';
EXECUTE IMMEDIATE 'DROP TABLE tmp_missing_batch;';
END IF;

--Generate Statistics
--EXECUTE IMMEDIATE 'GENERATE STATISTICS ON '||da_tbl_rec.dst_tbl_name||';';

ELSIF ((SELECT COUNT(1) FROM _v_relation_column WHERE name = da_tbl_rec.dst_tbl_name) = 0 and src_tbl_clmn > 0 and src_tbl_cnt_val > 0) THEN
--HISTORICAL

log_data:=log_data||'
'||da_tbl_rec.dst_tbl_name||' table does not exist. Considering the dearchive process as historical run.';

--Create dearchive table
EXECUTE IMMEDIATE 'CREATE TABLE '||da_tbl_rec.dst_tbl_name||' AS 
SELECT '||req_columns||' FROM '||da_tbl_rec.lnd_db_name||'..'||da_tbl_rec.src_tbl_name||' A JOIN (SELECT DISTINCT '||da_tbl_rec.src_tbl_pk||',rcrd_btch_audt_id FROM
(SELECT *, ROW_NUMBER() OVER(PARTITION BY '||da_tbl_rec.src_tbl_pk||' ORDER BY rcrd_isrt_ts DESC, rcrd_btch_audt_id DESC) AS rn FROM '||da_tbl_rec.lnd_db_name||'..'||da_tbl_rec.src_tbl_name||') src
WHERE src.rn = 1)B USING ('||da_tbl_rec.src_tbl_pk||',rcrd_btch_audt_id) DISTRIBUTE ON ('||da_tbl_rec.src_tbl_pk||');';
log_data:=log_data||'
CREATE TABLE '||da_tbl_rec.dst_tbl_name||' SUCCESSFUL';

IF (da_tbl_rec.customer = 'BON_SECOURS' and da_tbl_rec.source = 'EPIC' and pk_column != '') THEN
--HARD DELETE DATA REMOVAL
EXECUTE IMMEDIATE 'DELETE FROM '||da_tbl_rec.dst_tbl_name||'
WHERE ('||pk_column||') IN
(SELECT pce_cst_src_tbl_pk FROM '||da_tbl_rec.lnd_db_name||'..pce_hard_delete WHERE lower(trim(pce_cst_src_tbl_nm)) = '''||da_tbl_rec.src_tbl_name||''');';
log_data:=log_data||'
Hard Delete records removed.';

EXECUTE IMMEDIATE 'CREATE TABLE renewed_rec AS 
SELECT '||da_tbl_rec.src_tbl_pk||' FROM '||da_tbl_rec.lnd_db_name||'..'||da_tbl_rec.src_tbl_name||' src 
INNER JOIN '||da_tbl_rec.lnd_db_name||'..pce_hard_delete phd ON '||da_tbl_rec.modified_pk||' = phd.pce_cst_src_tbl_pk
WHERE phd.pce_cst_src_tbl_nm = UPPER('''||da_tbl_rec.src_tbl_name||''')
AND src.rcrd_isrt_ts > phd.rcrd_isrt_ts
;';

SELECT COUNT(1) INTO ren_rec_cnt FROM renewed_rec;

IF (ren_rec_cnt > 0) THEN
EXECUTE IMMEDIATE 'INSERT INTO '||da_tbl_rec.dst_tbl_name||'
('||req_columns||')
SELECT '||req_columns||' FROM 
(SELECT *, ROW_NUMBER() OVER(PARTITION BY '||da_tbl_rec.src_tbl_pk||' ORDER BY rcrd_isrt_ts DESC, rcrd_btch_audt_id DESC) AS rn FROM '||da_tbl_rec.lnd_db_name||'..'||da_tbl_rec.src_tbl_name||'
WHERE ('||da_tbl_rec.src_tbl_pk||') IN (SELECT '||da_tbl_rec.src_tbl_pk||' FROM renewed_rec)) src
WHERE src.rn = 1;';
log_data:=log_data||'
Renewed records inserted back.';
END IF;
EXECUTE IMMEDIATE 'DROP TABLE renewed_rec;';
END IF;

ELSIF (src_tbl_clmn > 0 and src_tbl_cnt_val = 0) THEN
log_data:=log_data||'
The source table to be dearchived does not have any data. Skipping the table: '||da_tbl_rec.src_tbl_name||'.';

ELSE
log_data:=log_data||'
ERROR: '||da_tbl_rec.src_tbl_name||' table to be dearchived does not exist. Check the table name and retry.';

--Identifying Incremental vs Historical end
END IF;
--Loopoing through the entire metadata table end
COMMIT;
END LOOP;
--Number of dearchive tables check end
END IF;
--Metadata table check end
RETURN log_data;
END IF;
END;

END_PROC;


