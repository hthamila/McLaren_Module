\set ON_ERROR_STOP ON;

SELECT 'processing table: intermediate_stage_phys_scheduling_fct' AS table_processing;
DROP TABLE intermediate_stage_phys_scheduling_fct IF EXISTS;
CREATE TABLE intermediate_stage_phys_scheduling_fct AS
(
SELECT
    sch.scheventid,
    sch.scheduleseq,
    sch.schapptid,
    sch.appointmentsubfacility,
    sch.appointmentlocation,
    sch.appointmentprovidercount,
    sch.appointmentproviderresource,
    sch.appointmentproviderroledescription,
    sch.appointmentprovidernamefull,
    sch.appointmentprovidercredential,
    sch.appointmentprovidernpi,
    sch.referringprovidernamefull,
    sch.referringprovidercredential,
    sch.referringprovidernpi,
    sch.orderingprovidernamefull,
    sch.orderingprovidercredential,
    sch.orderingprovidernpi,
    sch.appointmenttype,
    sch.eventcreatedate,
    sch.eventcreatetime,
    sch.appointmentcreatedate,
    sch.appointmentcreatetime,
    sch.appointmentdate,
    sch.appointmenttime,
    sch.appointmentduration,
    sch.reschedulecount,
    sch.rescheduleperformdate,
    sch.rescheduleperformtime,
    sch.reschedulereason,
    sch.cancelcount,
    sch.cancelperformdate,
    sch.cancelperformtime,
    sch.cancelreason,
    sch.actionwithreasoncount,
    sch.lastactionwithreasonaction,
    sch.lastactionwithreasonperformdate,
    sch.lastactionwithreasonperformtime,
    sch.lastactionwithreasonreason,
    sch.lastactionwithreasonreasonmeaning,
    sch.checkincount,
    sch.checkinperformdate,
    sch.checkinperformtime,
    sch.checkoutcount,
    sch.checkoutperformdate,
    sch.checkoutperformtime,
    sch.personid,
    sch.mrn,
    sch.clinicalencounterid,
    sch.fin,
    sch.encountertype,
    sch.encounterstatus,
    sch.eventstatus,
    sch.schedulestatus,
    sch.appointmentstatus,
    sch.eventrecursequencenumber,
    pd.ptnt_frst_nm,
    pd.ptnt_mid_nm,
    pd.ptnt_lst_nm,
    pd.patient_name,
    spl_refrg.practitioner_name AS referring_pvdr_pract_nm,
    spl_refrg.practitioner_spclty_description AS referring_pvdr_pract_spclty_descr,
    spl_refrg.practitioner_sub_spclty_nm AS referring_pvdr_pract_sub_spclty_nm,
    spl_appt.practitioner_name AS appt_pvdr_pract_nm,
    spl_appt.practitioner_spclty_description AS appt_pvdr_pract_spclty_descr,
    spl_appt.practitioner_sub_spclty_nm AS appt_pvdr_pract_sub_spclty_nm,
    spc_ordr.practitioner_name AS ordr_pvdr_pract_nm,
    spc_ordr.practitioner_spclty_description AS ordr_pvdr_pract_spclty_descr,
    spc_ordr.practitioner_sub_spclty_nm AS ordr_pvdr_pract_sub_spclty_nm
FROM
    pce_qe16_oper_prd_zoom..cv_physician_scheduling sch
    -------------------------------------------------------------------
    LEFT OUTER JOIN
    (
        SELECT
            medical_record_number,
            ptnt_frst_nm,
            ptnt_mid_nm,
            ptnt_lst_nm,
            CASE
                WHEN COALESCE(ptnt_lst_nm,'') = '' AND COALESCE(ptnt_frst_nm,'') = '' THEN NULL
                WHEN COALESCE(ptnt_frst_nm,'') = '' THEN ptnt_lst_nm
                WHEN COALESCE(ptnt_lst_nm,'') = '' THEN ptnt_frst_nm
                ELSE ptnt_lst_nm || ', ' || ptnt_frst_nm
            END AS patient_name,
            ROW_NUMBER() OVER (PARTITION BY medical_record_number ORDER BY COALESCE(dschrg_ts, adm_ts) DESC, upd_dt DESC) AS rn_pd
        FROM
            prd_encntr_anl_fct
    ) pd
    ON
        CAST(sch.mrn AS VARCHAR(50)) = CAST(pd.medical_record_number AS VARCHAR(50)) AND
        pd.rn_pd = 1
    -------------------------------------------------------------------
    LEFT OUTER JOIN
    (
        SELECT
            DISTINCT npi,
            practitioner_name,
            practitioner_spclty_description,
            practitioner_sub_spclty_nm
        FROM
            phy_npi_spclty_dim
    ) spl_refrg
    ON
        spl_refrg.npi = sch.referringprovidernpi
    -------------------------------------------------------------------
    LEFT OUTER JOIN
    (
        SELECT
            DISTINCT npi,
            practitioner_name,
            practitioner_spclty_description,
            practitioner_sub_spclty_nm
        FROM
            phy_npi_spclty_dim
    ) spl_appt
    ON
        spl_appt.npi = sch.appointmentprovidernpi
    -------------------------------------------------------------------
    LEFT OUTER JOIN
    (
        SELECT
            DISTINCT npi,
            practitioner_name,
            practitioner_spclty_description,
            practitioner_sub_spclty_nm
        FROM
            phy_npi_spclty_dim
    )  spc_ordr
    ON
        spc_ordr.npi = sch.orderingprovidernpi
    -------------------------------------------------------------------
)
DISTRIBUTE ON (schapptid, appointmentsubfacility);


SELECT 'processing table: intermediate_phys_scheduling_fct_prev' AS table_processing;
DROP TABLE intermediate_phys_scheduling_fct_prev IF EXISTS;
ALTER TABLE intermediate_phys_scheduling_fct RENAME TO intermediate_phys_scheduling_fct_prev;
ALTER TABLE intermediate_stage_phys_scheduling_fct RENAME TO intermediate_phys_scheduling_fct;


select 'processing table: prd_phys_scheduling_fct' as table_processing;
DROP TABLE prd_phys_scheduling_fct IF EXISTS;
CREATE TABLE prd_phys_scheduling_fct AS SELECT *,now() as rcrd_isrt_ts FROM intermediate_phys_scheduling_fct;

\unset ON_ERROR_STOP

