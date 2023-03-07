DROP TABLE tmp_sub_encntr_fct IF EXISTS;
create table tmp_sub_encntr_fct as
 (SELECT distinct pf.patient_id,
 case when vd.cd is null then vdd.cd else vd.cd end as pcd_cd_sub,
 case when pcd_cd_sub is null then pf.icd_code else pcd_cd_sub  end as pcd_cd,
 case when vd.cd_descr is null then vdd.cd_descr else vd.cd_descr end as pcd_descr_sub,
 case when pcd_descr_sub is null then pd.icd_pcd_descr else pcd_descr_sub  end as pcd_descr,
 case when pcd_descr_sub is not null then 1 else 2 end as pcd_row
 FROM  intermediate_stage_encntr_pcd_fct pf
 left join pce_qe16_prd..icd_pcd_cd_dim pd on pf.icd_code = pd.icd_pcd_cd and PF.icd_type='P'
 left join val_set_dim vd on pf.icd_code = vd.cd and vd.cohrt_id in ('GASTROENTEROSTOMY_PCD','HYSTERECTOMY_PCD','LOBECTOMY_PCD','NEPHRECTOMY_PCD','PROSTATECTOMEY_PCD','ROBOTIC_ASSISTED_PROCEDURES')
 AND PF.icd_type='P'
 left join val_set_dim vdd on pf.icd_code = vdd.cd and vdd.cohrt_id =  'ROBOTIC_ASSISTED_PROCEDURES' and pf.icd_type='S');

DROP TABLE tmp_postop_sep IF EXISTS;
 create table tmp_postop_sep as
(  SELECT distinct ef.encntr_num, 1 as postop_sep_ind
  FROM  intermediate_stage_encntr_anl_fct ef
  inner join  intermediate_stage_encntr_dgns_fct ed on ef.encntr_num = ed.patient_id
  inner join  intermediate_stage_encntr_pcd_fct ep on ef.encntr_num = ep.patient_id
  inner join ahrq_val_set_dim avd on replace(ed.icd_code,'.','')=avd.cd  and avd.cohrt_id='SEPTI2D' and  ed.diagnosistype not in ( 'Primary','Principal','Admission','Admission Diagnosis','Admitting')
  inner join pce_qe16_prd..ms_drg_dim mdd on ef.ms_drg_cd = mdd.ms_drg_cd
  inner join ahrq_val_set_dim mvd on mdd.ms_drg_cd= mvd.cd and mvd.cohrt_id='SURGI2R'
  inner join ahrq_val_set_dim pvd on ep.icd_code = pvd.cd and pvd.cohrt_id = 'ORPROC'
  where mdd.ms_drg_mdc_cd<>'14'  and ef.in_or_out_patient_ind =  'I ' );

DROP TABLE TMP_ORPROC IF EXISTS;
  CREATE TABLE TMP_ORPROC AS
(SELECT distinct ep.patient_id, 1 as orproc_ind
FROM  intermediate_stage_encntr_pcd_fct ep
inner join ahrq_val_set_dim vd on ep.icd_code = vd.cd and vd.cohrt_id = 'ORPROC');

DROP TABLE TMP_infectid IF EXISTS;
CREATE TABLE TMP_infectid AS
(

SELECT distinct ef.ENCNTR_NUM, 1 as postop_infectid_ind
FROM  intermediate_stage_encntr_anl_fct EF
INNER JOIN  intermediate_stage_encntr_dgns_fct ED ON ef.encntr_num = ed.patient_id
INNER JOIN  intermediate_stage_encntr_pcd_fct ep ON EF.ENCNTR_NUM = EP.PATIENT_ID
inner join ahrq_val_set_dim vd on ep.icd_code = vd.cd and vd.cohrt_id = 'ORPROC'
INNER JOIN ahrq_val_set_dim VDD ON replace(ed.icd_code,'.','') = vdd.cd and vdd.cohrt_id=  'INFECID'

);

 --select 'processing table: encntr_msr_fct ' as table_processing;
DROP TABLE encntr_msr_fct if exists;
 create table encntr_msr_fct as
 (select q.* from
 (select distinct ef.encntr_num,
 tsef.pcd_cd,
 tsef.pcd_descr,
 row_number() over (partition by ef.encntr_num order by tsef.pcd_row asc ) as row_num,
max(case when tlcf.lung_cancer_scrn_ind is null then 0 else tlcf.lung_cancer_scrn_ind end) as lung_cancer_scrn_ind ,
 max(case when tref.encntr_num is null then 0 else 1 end) as robotic_srgy_ind,
 max(case when tpsep.encntr_num is null then 0 else 1 end) as postop_sep_ind,
  max(case when torproc.patient_id is null then 0 else 1 end) as orproc_ind,
   max(case when tinf.encntr_num is null then 0 else 1 end) as postop_infectid_ind

 from  intermediate_stage_encntr_anl_fct ef
 left join tmp_sub_encntr_fct tsef on ef.encntr_num= tsef.patient_id
 left join tmp_lung_cancer_fct tlcf on ef.encntr_num = tlcf.encntr_num
 left join tmp_robo_encntr_fct tref on ef.encntr_num = tref.encntr_num
 left join tmp_postop_sep  tpsep on ef.encntr_num=tpsep.encntr_num
 left join TMP_ORPROC torproc on ef.encntr_num= torproc.patient_id
 left join tmp_infectid tinf on ef.encntr_num = tinf.encntr_num
 where ef.encntr_num is not null
group by ef.encntr_num,tsef.pcd_cd,tsef.pcd_descr,tsef.pcd_row

 ) as q
 where q.row_num=1) ;

DROP TABLE  intermediate_stage_encntr_anl_fct_new IF EXISTS;
 CREATE TABLE intermediate_stage_encntr_anl_fct_new AS
 Select ef.*, em.lung_cancer_scrn_ind, em.robotic_srgy_ind ,em.postop_sep_ind,em.orproc_ind,em.postop_infectid_ind
 FROM  intermediate_stage_encntr_anl_fct ef
 LEFT JOIN encntr_msr_fct em on ef.encntr_num = em.encntr_num;