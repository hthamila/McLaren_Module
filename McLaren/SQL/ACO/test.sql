create or replace view cv_nyu_dgns_ccs_dim as
select  d.dgns_cd, d.dgns_alt_cd, d.dgns_icd_ver, d.dgns_descr,
                dccs.ccs_dgns_cgy_cd,
        dccs.ccs_dgns_cgy_descr,
        dccs.ccs_dgns_lvl_2_descr,
                cci.chronic_cdtn_ind,
                alc_rel_pct, drug_rel_pct, ed_care_needed_not_prvntable_pct,
                ed_care_needed_prvntable_avoidable_pct, injry_rel_pct, non_emrgnt_rel_pct, psychology_rel_pct,
                treatable_emrgnt_ptnt_care_pct, unclsfd_pct
        from dgns_dim d
                left join dgns_ccs_dim dccs on d.dgns_alt_cd=REPLACE(dccs.dgns_cd, '.', '')
                        and d.dgns_icd_ver=dccs.dgns_cd_ver
                left join dgns_ccs_chronic_cdtn_dim cci on d.dgns_alt_cd=REPLACE(cci.dgns_cd, '.', '')
                        and d.dgns_icd_ver=cci.dgns_cd_ver
                left join nyu_ed_algr_dim nyu on d.dgns_alt_cd=nyu.icd_diagonsis_cd
                        and d.dgns_icd_ver=nyu.dgns_icd_ver
;
create or replace view cv_pcd_ccs_dim as
select p.icd_pcd_cd, p.icd_pcd_descr, p.icd_ver, icd_pcd_3_dgt_cd, icd_pcd_3_dgt_descr, icd_pcd_4_dgt_cd, icd_pcd_4_dgt_descr, icd_pcd_ccs_cgy_cd, icd_pcd_ccs_cgy_descr, icd_pcd_ccs_lvl_2_descr
        from icd_pcd_dim p
          left join icd_pcd_ccs_dim ccs
                on p.icd_pcd_cd=ccs.icd_pcd_cd
                and p.icd_ver=ccs.icd_pcd_cd_ver;
