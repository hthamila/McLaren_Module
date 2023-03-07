--############################################################--
-- This table is a transpose of data from survey demographics --
-- Created two temp tables because of Netezza inability of max record length --
--############################################################--

drop table cv_survey_demographics_tr1 if exists;
create temp table cv_survey_demographics_tr1 as
select survey_id, 
MAX(CASE WHEN VARNAME='29ALASKA' THEN VALUE END) AS ALASKA,
MAX(CASE WHEN VARNAME='29ASIAN' THEN VALUE END) AS ASIAN,
MAX(CASE WHEN VARNAME='29BLACK' THEN VALUE END) AS BLACK,
MAX(CASE WHEN VARNAME='29HAWAII' THEN VALUE END) AS HAWAII,
MAX(CASE WHEN VARNAME='29WHITE' THEN VALUE END) AS WHITE,
MAX(CASE WHEN VARNAME='ADJSAMP' THEN VALUE END) AS ADJSAMP,
MAX(CASE WHEN VARNAME='AGE' THEN VALUE END) AS AGE_1,
MAX(CASE WHEN VARNAME='AGERG' THEN VALUE END) AS AGERG,
MAX(CASE WHEN VARNAME='APPTWAIT' THEN VALUE END) AS APPTWAIT,
MAX(CASE WHEN VARNAME='APPTWRG' THEN VALUE END) AS APPTWRG,
MAX(CASE WHEN VARNAME='ATAQSANS' THEN VALUE END) AS ATAQSANS,
MAX(CASE WHEN VARNAME='BARCODE' THEN VALUE END) AS BARCODE,
MAX(CASE WHEN VARNAME='CHECK' THEN VALUE END) AS CHECK_1,
MAX(CASE WHEN VARNAME='CMPTNAME' THEN VALUE END) AS CMPTNAME,
MAX(CASE WHEN VARNAME='CMPTPHON' THEN VALUE END) AS CMPTPHON,
MAX(CASE WHEN VARNAME='CMS_RPT' THEN VALUE END) AS CMS_RPT,
MAX(CASE WHEN VARNAME='COMPSTAT' THEN VALUE END) AS COMPSTAT,
MAX(CASE WHEN VARNAME='CORECOMP' THEN VALUE END) AS CORECOMP,
MAX(CASE WHEN VARNAME='CSURVEY' THEN VALUE END) AS CSURVEY,
MAX(CASE WHEN VARNAME='CTYPE' THEN VALUE END) AS CTYPE,
MAX(CASE WHEN VARNAME='D11529_1' THEN VALUE END) AS D11529_1,
MAX(CASE WHEN VARNAME='D1193_01' THEN VALUE END) AS D1193_01,
MAX(CASE WHEN VARNAME='D1411_01' THEN VALUE END) AS D1411_01,
MAX(CASE WHEN VARNAME='D311_01' THEN VALUE END) AS D311_01,
MAX(CASE WHEN VARNAME='DAYS' THEN VALUE END) AS DAYS_1,
MAX(CASE WHEN VARNAME='DAYSRG' THEN VALUE END) AS DAYSRG,
MAX(CASE WHEN VARNAME='DAYWEEK' THEN VALUE END) AS DAYWEEK,
MAX(CASE WHEN VARNAME='DISDD' THEN VALUE END) AS DISDD,
MAX(CASE WHEN VARNAME='DISMM' THEN VALUE END) AS DISMM,
MAX(CASE WHEN VARNAME='DISTRIB' THEN VALUE END) AS DISTRIB,
MAX(CASE WHEN VARNAME='DISYR' THEN VALUE END) AS DISYR,
MAX(CASE WHEN VARNAME='DRG' THEN VALUE END) AS DRG,
MAX(CASE WHEN VARNAME='DVC_TP_E' THEN VALUE END) AS DVC_TP_E,
MAX(CASE WHEN VARNAME='DVC_TP_S' THEN VALUE END) AS DVC_TP_S,
MAX(CASE WHEN VARNAME='EDTIME' THEN VALUE END) AS EDTIME,
MAX(CASE WHEN VARNAME='EDTIMRG' THEN VALUE END) AS EDTIMRG,
MAX(CASE WHEN VARNAME='ER' THEN VALUE END) AS ER,
MAX(CASE WHEN VARNAME='FILLING' THEN VALUE END) AS FILLING,
MAX(CASE WHEN VARNAME='FSTAY' THEN VALUE END) AS FSTAY,
MAX(CASE WHEN VARNAME='FVISIT' THEN VALUE END) AS FVISIT,
MAX(CASE WHEN VARNAME='HCAHPS' THEN VALUE END) AS HCAHPS,
MAX(CASE WHEN VARNAME='HCWCLHND' THEN VALUE END) AS HCWCLHND,
MAX(CASE WHEN VARNAME='HEALTH' THEN VALUE END) AS HEALTH,
MAX(CASE WHEN VARNAME='IDBRACEL' THEN VALUE END) AS IDBRACEL,
MAX(CASE WHEN VARNAME='INLIMITS' THEN VALUE END) AS INLIMITS,
MAX(CASE WHEN VARNAME='INSUR' THEN VALUE END) AS INSUR,
MAX(CASE WHEN VARNAME='IT_PAYOR' THEN VALUE END) AS IT_PAYOR,
MAX(CASE WHEN VARNAME='ITACCTNO' THEN VALUE END) AS ITACCTNO,
MAX(CASE WHEN VARNAME='ITADD2' THEN VALUE END) AS ITADD2,
MAX(CASE WHEN VARNAME='ITADMDAT' THEN VALUE END) AS ITADMDAT,
MAX(CASE WHEN VARNAME='ITADMSRC' THEN VALUE END) AS ITADMSRC,
MAX(CASE WHEN VARNAME='ITADMTI3' THEN VALUE END) AS ITADMTI3,
MAX(CASE WHEN VARNAME='ITADMTIM' THEN VALUE END) AS ITADMTIM,
MAX(CASE WHEN VARNAME='ITAGE' THEN VALUE END) AS ITAGE,
MAX(CASE WHEN VARNAME='ITANMD' THEN VALUE END) AS ITANMD,
MAX(CASE WHEN VARNAME='ITATDMDC' THEN VALUE END) AS ITATDMDC,
MAX(CASE WHEN VARNAME='ITATNMCD' THEN VALUE END) AS ITATNMCD,
MAX(CASE WHEN VARNAME='ITATNMDN' THEN VALUE END) AS ITATNMDN,
MAX(CASE WHEN VARNAME='ITATTMD' THEN VALUE END) AS ITATTMD,
MAX(CASE WHEN VARNAME='ITATTMDC' THEN VALUE END) AS ITATTMDC,
MAX(CASE WHEN VARNAME='ITATTMDN' THEN VALUE END) AS ITATTMDN,
MAX(CASE WHEN VARNAME='ITATTN_M' THEN VALUE END) AS ITATTN_M,
MAX(CASE WHEN VARNAME='ITATTNDN' THEN VALUE END) AS ITATTNDN,
MAX(CASE WHEN VARNAME='ITATTNM' THEN VALUE END) AS ITATTNM,
MAX(CASE WHEN VARNAME='ITATTNM2' THEN VALUE END) AS ITATTNM2,
MAX(CASE WHEN VARNAME='ITBED' THEN VALUE END) AS ITBED,
MAX(CASE WHEN VARNAME='ITCITY' THEN VALUE END) AS ITCITY,
MAX(CASE WHEN VARNAME='ITCLINCC' THEN VALUE END) AS ITCLINCC,
MAX(CASE WHEN VARNAME='ITCLINCD' THEN VALUE END) AS ITCLINCD,
MAX(CASE WHEN VARNAME='ITCLINIC' THEN VALUE END) AS ITCLINIC,
MAX(CASE WHEN VARNAME='ITCMS_AG' THEN VALUE END) AS ITCMS_AG,
MAX(CASE WHEN VARNAME='ITCMS_SL' THEN VALUE END) AS ITCMS_SL,
MAX(CASE WHEN VARNAME='ITCOMBOA' THEN VALUE END) AS ITCOMBOA,
MAX(CASE WHEN VARNAME='ITCOMBOB' THEN VALUE END) AS ITCOMBOB,
MAX(CASE WHEN VARNAME='ITCPT_CA' THEN VALUE END) AS ITCPT_CA,
MAX(CASE WHEN VARNAME='ITDCG_DA' THEN VALUE END) AS ITDCG_DA,
MAX(CASE WHEN VARNAME='ITDCG_ST' THEN VALUE END) AS ITDCG_ST,
MAX(CASE WHEN VARNAME='ITDCGDA' THEN VALUE END) AS ITDCGDA,
MAX(CASE WHEN VARNAME='ITDCGDAT' THEN VALUE END) AS ITDCGDAT,
MAX(CASE WHEN VARNAME='ITDCGSTA' THEN VALUE END) AS ITDCGSTA,
MAX(CASE WHEN VARNAME='ITDCGTIM' THEN VALUE END) AS ITDCGTIM,
MAX(CASE WHEN VARNAME='ITDECEAS' THEN VALUE END) AS ITDECEAS,
MAX(CASE WHEN VARNAME='ITDEGREE' THEN VALUE END) AS ITDEGREE,
MAX(CASE WHEN VARNAME='ITDEPT' THEN VALUE END) AS ITDEPT,
MAX(CASE WHEN VARNAME='ITDEPTCD' THEN VALUE END) AS ITDEPTCD,
MAX(CASE WHEN VARNAME='ITDIA_C' THEN VALUE END) AS ITDIA_C,
MAX(CASE WHEN VARNAME='ITDIAG_A' THEN VALUE END) AS ITDIAG_A,
MAX(CASE WHEN VARNAME='ITDIAG_B' THEN VALUE END) AS ITDIAG_B,
MAX(CASE WHEN VARNAME='ITDIAG_C' THEN VALUE END) AS ITDIAG_C,
MAX(CASE WHEN VARNAME='ITDIAG_D' THEN VALUE END) AS ITDIAG_D,
MAX(CASE WHEN VARNAME='ITDIAG_E' THEN VALUE END) AS ITDIAG_E,
MAX(CASE WHEN VARNAME='ITDIAG_F' THEN VALUE END) AS ITDIAG_F,
MAX(CASE WHEN VARNAME='ITDIAGA' THEN VALUE END) AS ITDIAGA,
MAX(CASE WHEN VARNAME='ITDIAGC' THEN VALUE END) AS ITDIAGC,
MAX(CASE WHEN VARNAME='ITDIAGCD' THEN VALUE END) AS ITDIAGCD,
MAX(CASE WHEN VARNAME='ITDIAGD' THEN VALUE END) AS ITDIAGD,
MAX(CASE WHEN VARNAME='ITDISDAT' THEN VALUE END) AS ITDISDAT,
MAX(CASE WHEN VARNAME='ITDOB' THEN VALUE END) AS ITDOB,
MAX(CASE WHEN VARNAME='ITDRG' THEN VALUE END) AS ITDRG,
MAX(CASE WHEN VARNAME='ITE_FLAG' THEN VALUE END) AS ITE_FLAG,
MAX(CASE WHEN VARNAME='ITER_ADM' THEN VALUE END) AS ITER_ADM,
MAX(CASE WHEN VARNAME='ITEX_FLA' THEN VALUE END) AS ITEX_FLA,
MAX(CASE WHEN VARNAME='ITFAST_T' THEN VALUE END) AS ITFAST_T,
MAX(CASE WHEN VARNAME='ITFASTTR' THEN VALUE END) AS ITFASTTR,
MAX(CASE WHEN VARNAME='ITHCAHPS' THEN VALUE END) AS ITHCAHPS,
MAX(CASE WHEN VARNAME='ITINPT_X' THEN VALUE END) AS ITINPT_X,
MAX(CASE WHEN VARNAME='ITLANG' THEN VALUE END) AS ITLANG,
MAX(CASE WHEN VARNAME='ITLANGUA' THEN VALUE END) AS ITLANGUA,
MAX(CASE WHEN VARNAME='ITLOC_CD' THEN VALUE END) AS ITLOC_CD,
MAX(CASE WHEN VARNAME='ITLOCATI' THEN VALUE END) AS ITLOCATI,
MAX(CASE WHEN VARNAME='ITLOCCD' THEN VALUE END) AS ITLOCCD,
MAX(CASE WHEN VARNAME='ITLOS' THEN VALUE END) AS ITLOS,
MAX(CASE WHEN VARNAME='ITMD_F' THEN VALUE END) AS ITMD_F,
MAX(CASE WHEN VARNAME='ITMD_L' THEN VALUE END) AS ITMD_L,
MAX(CASE WHEN VARNAME='ITMD_M' THEN VALUE END) AS ITMD_M,
MAX(CASE WHEN VARNAME='ITMD_NAM' THEN VALUE END) AS ITMD_NAM,
MAX(CASE WHEN VARNAME='ITMD_TYP' THEN VALUE END) AS ITMD_TYP,
MAX(CASE WHEN VARNAME='ITMDCD' THEN VALUE END) AS ITMDCD,
MAX(CASE WHEN VARNAME='ITMDNAM' THEN VALUE END) AS ITMDNAM,
MAX(CASE WHEN VARNAME='ITMDNAME' THEN VALUE END) AS ITMDNAME,
MAX(CASE WHEN VARNAME='ITMDNO' THEN VALUE END) AS ITMDNO,
MAX(CASE WHEN VARNAME='ITMDSPE' THEN VALUE END) AS ITMDSPE,
MAX(CASE WHEN VARNAME='ITMDSPEC' THEN VALUE END) AS ITMDSPEC,
MAX(CASE WHEN VARNAME='ITMEDICA' THEN VALUE END) AS ITMEDICA,
MAX(CASE WHEN VARNAME='ITMEDREC' THEN VALUE END) AS ITMEDREC,
MAX(CASE WHEN VARNAME='ITMOBILE' THEN VALUE END) AS ITMOBILE,
MAX(CASE WHEN VARNAME='ITNEWB_F' THEN VALUE END) AS ITNEWB_F,
MAX(CASE WHEN VARNAME='ITNEWBFL' THEN VALUE END) AS ITNEWBFL,
MAX(CASE WHEN VARNAME='ITNO_PUB' THEN VALUE END) AS ITNO_PUB,
MAX(CASE WHEN VARNAME='ITNPI' THEN VALUE END) AS ITNPI,
MAX(CASE WHEN VARNAME='ITOTHER' THEN VALUE END) AS ITOTHER,
MAX(CASE WHEN VARNAME='ITP_LOOK' THEN VALUE END) AS ITP_LOOK,
MAX(CASE WHEN VARNAME='ITPARENT' THEN VALUE END) AS ITPARENT,
MAX(CASE WHEN VARNAME='ITPAT_AG' THEN VALUE END) AS ITPAT_AG,
MAX(CASE WHEN VARNAME='ITPATNO' THEN VALUE END) AS ITPATNO,
MAX(CASE WHEN VARNAME='ITPATTYP' THEN VALUE END) AS ITPATTYP,
MAX(CASE WHEN VARNAME='ITPAYCD' THEN VALUE END) AS ITPAYCD,
MAX(CASE WHEN VARNAME='ITPAYOR' THEN VALUE END) AS ITPAYOR,
MAX(CASE WHEN VARNAME='ITPAYOR1' THEN VALUE END) AS ITPAYOR1,
MAX(CASE WHEN VARNAME='ITPAYORC' THEN VALUE END) AS ITPAYORC,
MAX(CASE WHEN VARNAME='ITPHONE' THEN VALUE END) AS ITPHONE,
MAX(CASE WHEN VARNAME='ITPROCCD' THEN VALUE END) AS ITPROCCD,
MAX(CASE WHEN VARNAME='ITPROV' THEN VALUE END) AS ITPROV,
MAX(CASE WHEN VARNAME='ITRACE' THEN VALUE END) AS ITRACE,
MAX(CASE WHEN VARNAME='ITREFMDN' THEN VALUE END) AS ITREFMDN,
MAX(CASE WHEN VARNAME='ITROOM' THEN VALUE END) AS ITROOM,
MAX(CASE WHEN VARNAME='ITSERV' THEN VALUE END) AS ITSERV,
MAX(CASE WHEN VARNAME='ITSERV_T' THEN VALUE END) AS ITSERV_T,
MAX(CASE WHEN VARNAME='ITSERVIC' THEN VALUE END) AS ITSERVIC,
MAX(CASE WHEN VARNAME='ITSERVTY' THEN VALUE END) AS ITSERVTY,
MAX(CASE WHEN VARNAME='ITSEX' THEN VALUE END) AS ITSEX,
MAX(CASE WHEN VARNAME='ITSITE_C' THEN VALUE END) AS ITSITE_C
from pce_qe16_pressganey_prd_zoom..cv_survey_demographics 
group by survey_id ;

drop table cv_survey_demographics_tr2 if exists;
create temp table cv_survey_demographics_tr2 as
select survey_id, 
MAX(CASE WHEN VARNAME='ITSITE_I' THEN VALUE END) AS ITSITE_I,
MAX(CASE WHEN VARNAME='ITSITECD' THEN VALUE END) AS ITSITECD,
MAX(CASE WHEN VARNAME='ITSPECCD' THEN VALUE END) AS ITSPECCD,
MAX(CASE WHEN VARNAME='ITST_REG' THEN VALUE END) AS ITST_REG,
MAX(CASE WHEN VARNAME='ITSTATE' THEN VALUE END) AS ITSTATE,
MAX(CASE WHEN VARNAME='ITSUFFIX' THEN VALUE END) AS ITSUFFIX,
MAX(CASE WHEN VARNAME='ITSVC_CD' THEN VALUE END) AS ITSVC_CD,
MAX(CASE WHEN VARNAME='ITSVC1' THEN VALUE END) AS ITSVC1,
MAX(CASE WHEN VARNAME='ITSVCCD' THEN VALUE END) AS ITSVCCD,
MAX(CASE WHEN VARNAME='ITSVCDAT' THEN VALUE END) AS ITSVCDAT,
MAX(CASE WHEN VARNAME='ITUNIQ' THEN VALUE END) AS ITUNIQ,
MAX(CASE WHEN VARNAME='ITUNIQUE' THEN VALUE END) AS ITUNIQUE,
MAX(CASE WHEN VARNAME='ITUNIT' THEN VALUE END) AS ITUNIT,
MAX(CASE WHEN VARNAME='ITUPIN' THEN VALUE END) AS ITUPIN,
MAX(CASE WHEN VARNAME='ITZIP' THEN VALUE END) AS ITZIP,
MAX(CASE WHEN VARNAME='LANGUAGE' THEN VALUE END) AS LANGUAGE_1,
MAX(CASE WHEN VARNAME='LEADVISI' THEN VALUE END) AS LEADVISI,
MAX(CASE WHEN VARNAME='LSUPPORT' THEN VALUE END) AS LSUPPORT,
MAX(CASE WHEN VARNAME='MATE' THEN VALUE END) AS MATE,
MAX(CASE WHEN VARNAME='OCCUR' THEN VALUE END) AS OCCUR,
MAX(CASE WHEN VARNAME='ORGAN' THEN VALUE END) AS ORGAN,
MAX(CASE WHEN VARNAME='OSC_38' THEN VALUE END) AS OSC_38,
MAX(CASE WHEN VARNAME='OTHER' THEN VALUE END) AS OTHER,
MAX(CASE WHEN VARNAME='PAGELINK' THEN VALUE END) AS PAGELINK,
MAX(CASE WHEN VARNAME='PROVID' THEN VALUE END) AS PROVID,
MAX(CASE WHEN VARNAME='REASON' THEN VALUE END) AS REASON,
MAX(CASE WHEN VARNAME='RICRIT' THEN VALUE END) AS RICRIT,
MAX(CASE WHEN VARNAME='RIGHTS' THEN VALUE END) AS RIGHTS,
MAX(CASE WHEN VARNAME='SAFESECU' THEN VALUE END) AS SAFESECU,
MAX(CASE WHEN VARNAME='SERVICE' THEN VALUE END) AS SERVICE,
MAX(CASE WHEN VARNAME='SEX' THEN VALUE END) AS SEX,
MAX(CASE WHEN VARNAME='SHIFT' THEN VALUE END) AS SHIFT,
MAX(CASE WHEN VARNAME='SHIFT12' THEN VALUE END) AS SHIFT12,
MAX(CASE WHEN VARNAME='SHIFT8' THEN VALUE END) AS SHIFT8,
MAX(CASE WHEN VARNAME='SIGNSYMP' THEN VALUE END) AS SIGNSYMP,
MAX(CASE WHEN VARNAME='SITEID' THEN VALUE END) AS SITEID,
MAX(CASE WHEN VARNAME='SP' THEN VALUE END) AS SP,
MAX(CASE WHEN VARNAME='SPAN_CMS' THEN VALUE END) AS SPAN_CMS,
MAX(CASE WHEN VARNAME='SPCAT' THEN VALUE END) AS SPCAT,
MAX(CASE WHEN VARNAME='SURGERY' THEN VALUE END) AS SURGERY,
MAX(CASE WHEN VARNAME='TESTWAIT' THEN VALUE END) AS TESTWAIT,
MAX(CASE WHEN VARNAME='TESTWRG' THEN VALUE END) AS TESTWRG,
MAX(CASE WHEN VARNAME='TIME' THEN VALUE END) AS TIME_1,
MAX(CASE WHEN VARNAME='TIMECOMP' THEN VALUE END) AS TIMECOMP,
MAX(CASE WHEN VARNAME='TIMEDAY' THEN VALUE END) AS TIMEDAY,
MAX(CASE WHEN VARNAME='TIMEHR' THEN VALUE END) AS TIMEHR,
MAX(CASE WHEN VARNAME='TIMEMIN' THEN VALUE END) AS TIMEMIN,
MAX(CASE WHEN VARNAME='UNEXPECT' THEN VALUE END) AS UNEXPECT,
MAX(CASE WHEN VARNAME='UNIT' THEN VALUE END) AS UNIT,
MAX(CASE WHEN VARNAME='VISIT' THEN VALUE END) AS VISIT,
MAX(CASE WHEN VARNAME='WAITCP' THEN VALUE END) AS WAITCP,
MAX(CASE WHEN VARNAME='WAITCRG' THEN VALUE END) AS WAITCRG,
MAX(CASE WHEN VARNAME='WAITRM' THEN VALUE END) AS WAITRM,
MAX(CASE WHEN VARNAME='WAITRRG' THEN VALUE END) AS WAITRRG,
MAX(CASE WHEN VARNAME='WHEN' THEN VALUE END) AS WHEN_1,
MAX(CASE WHEN VARNAME='ZIP' THEN VALUE END) AS ZIP,
1 AS srvy_cnt

from pce_qe16_pressganey_prd_zoom..cv_survey_demographics
group by survey_id ;

drop table cv_survey_demographics_tr if exists;
create table cv_survey_demographics_tr as
select * from cv_survey_demographics_tr1 
	inner join cv_survey_demographics_tr2 using (survey_id) 
DISTRIBUTE ON (survey_id);	

