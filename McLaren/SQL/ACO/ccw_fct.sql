\set ON_ERROR_STOP ON;
truncate stg_ccw_fct;

insert into stg_ccw_fct
SELECT mbr_id_num, 'Acquired Hypothyroidism' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(big_gp_cnt) big_gp_cnt,
    SUM(ltl_gp_cnt) ltl_gp_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm 
   	,max(svc_to_dt) last_coded_dt
	,CASE WHEN rsk_pool_nm IN ('IP', 'SNF', 'HHA') THEN 1 END as big_gp_cnt
	,CASE WHEN rsk_pool_nm IN ('OP', 'Part B') THEN 1 END ltl_gp_cnt
    FROM clm_dgns_fct cdf
	INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Acquired Hypothyroidism'
    WHERE rsk_pool_nm IN ('IP', 'SNF', 'HHA','OP', 'Part B')
	group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(big_gp_cnt) > 0 OR SUM(ltl_gp_cnt) > 1
)a;


INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Acute Myocardial Infarction' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm 
	,max(svc_to_dt) last_coded_dt
	,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
    FROM clm_dgns_fct cdf
	INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Acute Myocardial Infarction'
    WHERE rsk_pool_nm ='IP' and icd_pos_num in (1,2)
	group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0
)a;


INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Alzheimer''s Disease' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(all_cnt) all_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm 
	,max(svc_to_dt) last_coded_dt
	,CASE WHEN rsk_pool_nm IN ('IP', 'SNF', 'HHA', 'OP', 'Part B') THEN 1 END as all_cnt
    FROM clm_dgns_fct cdf
	INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Alzheimer''s Disease'
    WHERE rsk_pool_nm IN ('IP', 'SNF', 'HHA', 'OP', 'Part B')
	group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(all_cnt) > 0
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Alzheimer''s Disease and Related Disorders or Senile Dementia' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(all_cnt) all_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm 
	,max(svc_to_dt) last_coded_dt
	,CASE WHEN rsk_pool_nm IN ('IP', 'SNF', 'HHA', 'OP', 'Part B') THEN 1 END as all_cnt
    FROM clm_dgns_fct cdf
	INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Alzheimer''s Disease and Related Disorders or Senile Dementia'
    WHERE rsk_pool_nm IN ('IP', 'SNF', 'HHA', 'OP', 'Part B')
	group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(all_cnt) > 0
)a;


INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Anemia' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(all_cnt) all_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm 
	,max(svc_to_dt) last_coded_dt
	,CASE WHEN rsk_pool_nm IN ('IP', 'SNF', 'HHA', 'OP', 'Part B') THEN 1 END as all_cnt
    FROM clm_dgns_fct cdf
	INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Anemia'
    WHERE rsk_pool_nm IN ('IP', 'SNF', 'HHA', 'OP', 'Part B')
	group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(all_cnt) > 0
)a;


INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Asthma' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(big_gp_cnt) big_gp_cnt,
    SUM(ltl_gp_cnt) ltl_gp_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP', 'SNF', 'HHA') THEN 1 END as big_gp_cnt
        ,CASE WHEN rsk_pool_nm IN ('OP', 'Part B') THEN 1 END ltl_gp_cnt
    FROM clm_dgns_fct cdf
        INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Asthma'
    WHERE rsk_pool_nm IN ('IP', 'SNF', 'HHA','OP', 'Part B')
        group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(big_gp_cnt) > 0 OR SUM(ltl_gp_cnt) > 1
)a;


INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Atrial Fibrillation' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(big_gp_cnt) big_gp_cnt,
    SUM(ltl_gp_cnt) ltl_gp_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as big_gp_cnt
        ,CASE WHEN rsk_pool_nm IN ('OP', 'Part B') THEN 1 END ltl_gp_cnt
    FROM clm_dgns_fct cdf
        INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Atrial Fibrillation'
    WHERE rsk_pool_nm IN ('IP', 'OP', 'Part B') and icd_pos_num in (1,2)
        group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(big_gp_cnt) > 0 OR SUM(ltl_gp_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Benign Prostatic Hyperplasia' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(big_gp_cnt) big_gp_cnt,
    SUM(ltl_gp_cnt) ltl_gp_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP', 'SNF', 'HHA') THEN 1 END as big_gp_cnt
        ,CASE WHEN rsk_pool_nm IN ('OP', 'Part B') THEN 1 END ltl_gp_cnt
    FROM clm_dgns_fct cdf
      	INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Benign Prostatic Hyperplasia'
		LEFT JOIN 
		(SELECT clm_id, 'X' excl_ind
    		FROM clm_dgns_fct cdf
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Benign Prostatic Hyperplasia' and opr_type_nm = 'NOT IN'
		group by clm_id)z on cdf.clm_id=z.clm_id
        where z.excl_ind is null 
		group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(big_gp_cnt) > 0 OR SUM(ltl_gp_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Cataract' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ltl_gp_cnt) ltl_gp_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('OP', 'Part B') THEN 1 END ltl_gp_cnt
    FROM clm_dgns_fct cdf
      	INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Cataract'
	WHERE rsk_pool_nm IN ('OP', 'Part B') and icd_pos_num=1
	group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ltl_gp_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Chronic Kidney Disease' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(big_gp_cnt) big_gp_cnt,
    SUM(ltl_gp_cnt) ltl_gp_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP', 'SNF', 'HHA') THEN 1 END as big_gp_cnt
        ,CASE WHEN rsk_pool_nm IN ('OP', 'Part B') THEN 1 END ltl_gp_cnt
    FROM clm_dgns_fct cdf
      	INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Chronic Kidney Disease'
        where rsk_pool_nm in ('IP', 'SNF', 'HHA','OP', 'Part B')  
		group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(big_gp_cnt) > 0 OR SUM(ltl_gp_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Chronic Obstructive Pulmonary Disease and Bronchiectasis' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(big_gp_cnt) big_gp_cnt,
    SUM(ltl_gp_cnt) ltl_gp_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP', 'SNF', 'HHA') THEN 1 END as big_gp_cnt
        ,CASE WHEN rsk_pool_nm IN ('OP', 'Part B') THEN 1 END ltl_gp_cnt
    FROM clm_dgns_fct cdf
      	INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Chronic Obstructive Pulmonary Disease and Bronchiectasis'
        where rsk_pool_nm in ('IP', 'SNF', 'HHA','OP', 'Part B')  
		group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(big_gp_cnt) > 0 OR SUM(ltl_gp_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Depression' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(all_cnt) all_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP', 'SNF', 'HHA', 'OP', 'Part B') THEN 1 END all_cnt
    FROM clm_dgns_fct cdf
        INNER JOIN pln_mbr_dim using (pln_mbr_sk)
        INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
        INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Depression'
        WHERE rsk_pool_nm IN ('IP', 'SNF', 'HHA', 'OP', 'Part B') and icd_pos_num=1
        group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(all_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Diabetes' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(big_gp_cnt) big_gp_cnt,
    SUM(ltl_gp_cnt) ltl_gp_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP', 'SNF', 'HHA') THEN 1 END as big_gp_cnt
        ,CASE WHEN rsk_pool_nm IN ('OP', 'Part B') THEN 1 END ltl_gp_cnt
    FROM clm_dgns_fct cdf
      	INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Diabetes'
        where rsk_pool_nm in ('IP', 'SNF', 'HHA','OP', 'Part B')  
		group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(big_gp_cnt) > 0 OR SUM(ltl_gp_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Glaucoma' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(all_cnt) all_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('Part B') THEN 1 END all_cnt
    FROM clm_dgns_fct cdf
        INNER JOIN pln_mbr_dim using (pln_mbr_sk)
        INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
        INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Glaucoma'
        WHERE rsk_pool_nm IN ('Part B') and icd_pos_num=1
        group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(all_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Heart Failure' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(all_cnt) all_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP', 'OP', 'Part B') THEN 1 END all_cnt
    FROM clm_dgns_fct cdf
        INNER JOIN pln_mbr_dim using (pln_mbr_sk)
        INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
        INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Heart Failure'
        WHERE rsk_pool_nm IN ('IP', 'OP', 'Part B') 
        group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(all_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Hip/Pelvic Fracture' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(all_cnt) all_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP', 'SNF') THEN 1 END all_cnt
    FROM clm_dgns_fct cdf
        INNER JOIN pln_mbr_dim using (pln_mbr_sk)
        INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
        INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Hip/Pelvic Fracture'
        WHERE rsk_pool_nm IN ('IP', 'SNF') 
        group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(all_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Hyperlipidemia' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(big_gp_cnt) big_gp_cnt,
    SUM(ltl_gp_cnt) ltl_gp_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP', 'SNF', 'HHA') THEN 1 END as big_gp_cnt
        ,CASE WHEN rsk_pool_nm IN ('OP', 'Part B') THEN 1 END ltl_gp_cnt
    FROM clm_dgns_fct cdf
      	INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Hyperlipidemia'
        where rsk_pool_nm in ('IP', 'SNF', 'HHA','OP', 'Part B')  
		group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(big_gp_cnt) > 0 OR SUM(ltl_gp_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Hypertension' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(big_gp_cnt) big_gp_cnt,
    SUM(ltl_gp_cnt) ltl_gp_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP', 'SNF', 'HHA') THEN 1 END as big_gp_cnt
        ,CASE WHEN rsk_pool_nm IN ('OP', 'Part B') THEN 1 END ltl_gp_cnt
    FROM clm_dgns_fct cdf
      	INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Hypertension'
        where rsk_pool_nm in ('IP', 'SNF', 'HHA','OP', 'Part B')  
		group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(big_gp_cnt) > 0 OR SUM(ltl_gp_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Ischemic Heart Disease' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(all_cnt) all_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP', 'SNF', 'HHA', 'OP', 'Part B') THEN 1 END all_cnt
    FROM clm_dgns_fct cdf
        INNER JOIN pln_mbr_dim using (pln_mbr_sk)
        INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
        INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Ischemic Heart Disease'
        WHERE rsk_pool_nm IN ('IP', 'SNF', 'HHA', 'OP', 'Part B') 
        group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(all_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Osteoporosis' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(big_gp_cnt) big_gp_cnt,
    SUM(ltl_gp_cnt) ltl_gp_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP', 'SNF', 'HHA') THEN 1 END as big_gp_cnt
        ,CASE WHEN rsk_pool_nm IN ('OP', 'Part B') THEN 1 END ltl_gp_cnt
    FROM clm_dgns_fct cdf
      	INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Osteoporosis'
        where rsk_pool_nm in ('IP', 'SNF', 'HHA','OP', 'Part B')  
		group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(big_gp_cnt) > 0 OR SUM(ltl_gp_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'RA/OA (Rheumatoid Arthritis/ Osteoarthritis)' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(all_cnt) all_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP', 'SNF', 'HHA', 'OP', 'Part B') THEN 1 END all_cnt
    FROM clm_dgns_fct cdf
        INNER JOIN pln_mbr_dim using (pln_mbr_sk)
        INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
        INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'RA/OA (Rheumatoid Arthritis/ Osteoarthritis)'
        WHERE rsk_pool_nm IN ('IP', 'SNF', 'HHA', 'OP', 'Part B') 
        group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(all_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Stroke / Transient Ischemic Attack' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(big_gp_cnt) big_gp_cnt,
    SUM(ltl_gp_cnt) ltl_gp_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as big_gp_cnt
        ,CASE WHEN rsk_pool_nm IN ('OP', 'Part B') THEN 1 END ltl_gp_cnt
    FROM clm_dgns_fct cdf
        INNER JOIN pln_mbr_dim using (pln_mbr_sk)
        INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
        INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Stroke / Transient Ischemic Attack'
                LEFT JOIN
                (SELECT clm_id, 'X' excl_ind
                FROM clm_dgns_fct cdf
        INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
        INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Stroke / Transient Ischemic Attack' and opr_type_nm = 'NOT IN'
                group by clm_id)z on cdf.clm_id=z.clm_id
        where z.excl_ind is null
                group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(big_gp_cnt) > 0 OR SUM(ltl_gp_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Female / Male Breast Cancer' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(big_gp_cnt) big_gp_cnt,
    SUM(ltl_gp_cnt) ltl_gp_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP', 'SNF') THEN 1 END as big_gp_cnt
        ,CASE WHEN rsk_pool_nm IN ('OP', 'Part B') THEN 1 END ltl_gp_cnt
    FROM clm_dgns_fct cdf
      	INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Female / Male Breast Cancer'
        where rsk_pool_nm in ('IP', 'SNF','OP', 'Part B')  
		group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(big_gp_cnt) > 0 OR SUM(ltl_gp_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Colorectal Cancer' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(big_gp_cnt) big_gp_cnt,
    SUM(ltl_gp_cnt) ltl_gp_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP', 'SNF') THEN 1 END as big_gp_cnt
        ,CASE WHEN rsk_pool_nm IN ('OP', 'Part B') THEN 1 END ltl_gp_cnt
    FROM clm_dgns_fct cdf
      	INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Colorectal Cancer'
        where rsk_pool_nm in ('IP', 'SNF','OP', 'Part B')  
		group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(big_gp_cnt) > 0 OR SUM(ltl_gp_cnt) > 1
)a;


INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Prostate Cancer' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(big_gp_cnt) big_gp_cnt,
    SUM(ltl_gp_cnt) ltl_gp_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP', 'SNF') THEN 1 END as big_gp_cnt
        ,CASE WHEN rsk_pool_nm IN ('OP', 'Part B') THEN 1 END ltl_gp_cnt
    FROM clm_dgns_fct cdf
      	INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Prostate Cancer'
        where rsk_pool_nm in ('IP', 'SNF','OP', 'Part B')  
		group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(big_gp_cnt) > 0 OR SUM(ltl_gp_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Lung Cancer' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(big_gp_cnt) big_gp_cnt,
    SUM(ltl_gp_cnt) ltl_gp_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP', 'SNF') THEN 1 END as big_gp_cnt
        ,CASE WHEN rsk_pool_nm IN ('OP', 'Part B') THEN 1 END ltl_gp_cnt
    FROM clm_dgns_fct cdf
      	INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Lung Cancer'
        where rsk_pool_nm in ('IP', 'SNF','OP', 'Part B')  
		group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(big_gp_cnt) > 0 OR SUM(ltl_gp_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Endometrial Cancer' cohrt_nm, 'Chronic Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(big_gp_cnt) big_gp_cnt,
    SUM(ltl_gp_cnt) ltl_gp_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP', 'SNF') THEN 1 END as big_gp_cnt
        ,CASE WHEN rsk_pool_nm IN ('OP', 'Part B') THEN 1 END ltl_gp_cnt
    FROM clm_dgns_fct cdf
      	INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      	INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      	INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Endometrial Cancer'
        where rsk_pool_nm in ('IP', 'SNF','OP', 'Part B')  
		group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(big_gp_cnt) > 0 OR SUM(ltl_gp_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'ADHD, Conduct Disorders, and Hyperkinetic Syndrome' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT   mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'ADHD, Conduct Disorders, and Hyperkinetic Syndrome'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Alcohol Use Disorders' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
        ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
        INNER JOIN pln_mbr_dim using (pln_mbr_sk)
        INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
        INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Alcohol Use Disorders'
		group by 1,2,3
        UNION
   	SELECT mbr_id_num,cpf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
        ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
	FROM clm_pcd_fct cpf
		INNER JOIN pln_mbr_dim using (pln_mbr_sk)
        INNER JOIN icd_pcd_dim ipd ON cpf.icd_pcd_sk = ipd.icd_pcd_sk
        INNER JOIN val_set_dim vsd ON ipd.icd_pcd_alt_cd = vsd.cd and vsd.cohrt_id = 'Alcohol Use Disorders'
        group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Anxiety Disorders' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT  mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Anxiety Disorders'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Autism Spectrum Disorders' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT  mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Autism Spectrum Disorders'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Bipolar Disorder' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT  mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Bipolar Disorder'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Cerebral Palsy' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT  mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Cerebral Palsy'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Cystic Fibrosis and Other Metabolic Developmental Disorders' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT  mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Cystic Fibrosis and Other Metabolic Developmental Disorders'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Depressive Disorders' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Depressive Disorders'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Drug Use Disorders' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
        ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
        INNER JOIN pln_mbr_dim using (pln_mbr_sk)
        INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
        INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Drug Use Disorders' and cd_dmn_nm='ICD Diagnosis'
		group by 1,2,3
        UNION
   	SELECT mbr_id_num,cpf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
        ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
	FROM clm_pcd_fct cpf
		INNER JOIN pln_mbr_dim using (pln_mbr_sk)
        INNER JOIN icd_pcd_dim ipd ON cpf.icd_pcd_sk = ipd.icd_pcd_sk
        INNER JOIN val_set_dim vsd ON ipd.icd_pcd_alt_cd = vsd.cd and vsd.cohrt_id = 'Drug Use Disorders' and cd_dmn_nm='ICD Procedure'
        group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Epilepsy' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Epilepsy'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Fibromyalgia, Chronic Pain and Fatigue' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Fibromyalgia'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Human Immunodeficiency Virus and/or Acquired Immunodeficiency Syndrome (HIV/AIDS)' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
        ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
        INNER JOIN pln_mbr_dim using (pln_mbr_sk)
        INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
        INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Human Immunodeficiency Virus and/or Acquired Immunodeficiency Syndrome (HIV/AIDS)' and cd_dmn_nm='ICD Diagnosis'
		group by 1,2,3
        UNION
   	SELECT mbr_id_num,clf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
        ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
	FROM clm_line_fct clf
        INNER JOIN val_set_dim vsd ON clf.ms_drg_cd = vsd.cd and vsd.cohrt_id = 'Human Immunodeficiency Virus and/or Acquired Immunodeficiency Syndrome (HIV/AIDS)' AND vsd.cd_dmn_nm = 'MS DRG Codes'
        group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Intellectual Disabilities and Related Conditions' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Intellectual Disabilities and Related Conditions'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;


INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Learning Disabilities' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Learning Disabilities'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Leukemias and Lymphomas' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Leukemias and Lymphomas'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Liver Disease, Cirrhosis and Other Liver Conditions (except Viral Hepatitis)' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,cdf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
        ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
        INNER JOIN pln_mbr_dim using (pln_mbr_sk)
        INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
        INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Liver Disease, Cirrhosis and Other Liver Conditions (except Viral Hepatitis)' and cd_dmn_nm='ICD Diagnosis'
		group by 1,2,3
        UNION
   	SELECT mbr_id_num,cpf.clm_id,rsk_pool_nm
        ,max(svc_to_dt) last_coded_dt
        ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
        ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
        FROM clm_pcd_fct cpf
        INNER JOIN pln_mbr_dim using (pln_mbr_sk)
        INNER JOIN icd_pcd_dim ipd ON cpf.icd_pcd_sk = ipd.icd_pcd_sk
        INNER JOIN val_set_dim vsd ON ipd.icd_pcd_alt_cd = vsd.cd and vsd.cohrt_id = 'Liver Disease, Cirrhosis and Other Liver Conditions (except Viral Hepatitis)' AND vsd.cd_dmn_nm = 'ICD Procedure'
        group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Migraine and Chronic Headache' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Migraine and Chronic Headache'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Mobility Impairments' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Mobility Impairments'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Multiple Sclerosis and Transverse Myelitis' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Multiple Sclerosis and Transverse Myelitis'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Muscular Dystrophy' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Muscular Dystrophy'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Obesity' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Obesity'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;


INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Other Developmental Delays' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Other Developmental Delays'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Peripheral Vascular Disease (PVD)' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Peripheral Vascular Disease (PVD)'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;


INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Personality Disorders' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Personality Disorders'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Post-Traumatic Stress Disorder (PTSD)' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Post-Traumatic Stress Disorder (PTSD)'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Pressure and Chronic Ulcers' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Pressure and Chronic Ulcers'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;


INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Schizophrenia' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Schizophrenia'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;


INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Schizophrenia and Other Psychotic Disorders' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Schizophrenia and Other Psychotic Disorders'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Sensory - Blindness and Visual Impairment' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Sensory - Blindness and Visual Impairment'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;


INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Sensory - Deafness and Hearing Impairment' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Sensory - Deafness and Hearing Impairment'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Spina Bifida and Other Congenital Anomalies of the Nervous System' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Spina Bifida and Other Congenital Anomalies of the Nervous System'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Spinal Cord Injury' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Spinal Cord Injury'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;


INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Tobacco Use' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Tobacco Use' and vsd.cd_dmn_nm='ICD Diagnosis'
      group by 1,2,3
	UNION
	SELECT 	mbr_id_num,clm_id,rsk_pool_nm
		 	,max(svc_to_dt) last_coded_dt
			,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
	FROM clm_line_fct
	INNER JOIN hcpcs_dim hd using (hcpcs_sk)
	INNER JOIN val_set_dim vsd ON hd.hcpcs_cd = vsd.cd and vsd.cohrt_id = 'Tobacco Use' and vsd.cd_dmn_nm='HCPCS Code'
	group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;


INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Traumatic Brain Injury and Nonpsychotic Mental Disorders due to Brain Damage' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Traumatic Brain Injury and Nonpsychotic Mental Disorders due to Brain Damage'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;


INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Hepatitis A' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Hepatitis A'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;


INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Hepatitis B (acute or unspecified)' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Hepatitis B (acute or unspecified)'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;


INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Hepatitis B (chronic)' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Hepatitis B (chronic)'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Hepatitis C (acute)' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Hepatitis C (acute)'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Hepatitis C (chronic)' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Hepatitis C (chronic)'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Hepatitis C (unspecified)' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Hepatitis C (unspecified)'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Hepatitis D' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Hepatitis D'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

INSERT INTO stg_ccw_fct
SELECT mbr_id_num, 'Hepatitis E' cohrt_nm, 'Other Chronic/Potentially Disabling Condition' as ccw_type, last_coded_dt
  FROM (
  SELECT
    mbr_id_num,
    SUM(ip_cnt) ip_cnt,
    SUM(oth_cnt) oth_cnt,
    max(last_coded_dt) last_coded_dt
  FROM (
    SELECT mbr_id_num,clm_id,rsk_pool_nm
            ,max(svc_to_dt) last_coded_dt
            ,CASE WHEN rsk_pool_nm IN ('IP') THEN 1 END as ip_cnt
            ,CASE WHEN rsk_pool_nm NOT IN ('IP') THEN 1 END oth_cnt
    FROM clm_dgns_fct cdf
      INNER JOIN pln_mbr_dim using (pln_mbr_sk)
      INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
      INNER JOIN val_set_dim vsd ON dd.dgns_cd = vsd.cd and vsd.cohrt_id = 'Hepatitis E'
      group by 1,2,3
  )z GROUP BY mbr_id_num
having SUM(ip_cnt) > 0 OR SUM(oth_cnt) > 1
)a;

DROP table ccw_fct;
CREATE TABLE ccw_fct AS
 with clm_ds as
 (
select a.mbr_id_num, paid_amt_12_mn, elig_sts, care_manage_status_ind from 
(select  mbr_id_num, SUM(CASE WHEN add_months(svc_to_dt, 12) > (SELECT val FROM dt_meta WHERE descr = 'roll_yr_end') THEN clf.paid_amt ELSE 0 END) paid_amt_12_mn from clm_line_fct_ds clf group by mbr_id_num)a join 
(select  mbr_id_num, elig_sts, care_manage_status_ind, svc_to_dt from 
(select mbr_id_num, elig_sts, care_manage_status_ind, svc_to_dt, ROW_NUMBER() OVER(PARTITION BY mbr_id_num order by svc_to_dt desc) as rn from clm_line_fct_ds) src where src.rn=1
)b on a.mbr_id_num=b.mbr_id_num
)

SELECT cf.*, pmd.frst_nm, pmd.last_nm, pmd.brth_dt, pmd.cms_hcc_scor_num, pmd.gnd_cd, pd.npi, pd.pvdr_frst_nm, pd.pvdr_lgl_last_nm, inpd.rgon, clf.paid_amt_12_mn, clf.elig_sts, clf.care_manage_status_ind
FROM stg_ccw_fct cf
  INNER JOIN pln_mbr_dim pmd ON cf.mbr_id_num = pmd.mbr_id_num
  INNER JOIN bene_pcp_attr bcs ON pmd.pln_mbr_sk = bcs.pln_mbr_sk
  INNER JOIN pvdr_dim pd ON bcs.bill_pvdr_sk = pd.pvdr_sk
  INNER JOIN clm_ds clf ON pmd.mbr_id_num = clf.mbr_id_num
  INNER JOIN in_ntw_pvdr_dim inpd ON bcs.bill_pvdr_sk = inpd.in_ntw_pvdr_sk;

\unset ON_ERROR_STOP
