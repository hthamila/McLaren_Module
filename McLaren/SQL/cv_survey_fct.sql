drop table survey_fact if exists;

CREATE TABLE survey_fact
AS
select 
asf.client_id
, asf.survey_id
, asf.service
, asf.disdate
, asf.recdate
, asf.resp_val
, asf.varname
, asf.question_text
, asf.section
, asf.standard
, asf.screening
, asf.top_box_answer
, asf.top_box_scale
, asf.survey_type
, asf.sentiment
from
(select 
pnt.client_id
, pnt.survey_id
, pnt.service
, pnt.disdate
, pnt.recdate
, cahps.value as resp_val
, cahps.varname
, caq.question_text
, caq.section
, caq.standard
, caq.screening
, caq.top_box_answer
, caq.top_box_scale
, 'CAHPS' as survey_type
, '' as sentiment
from 
prmretlp.cv_survey_patient as pnt
inner join prmretlp.cv_survey_cahps as cahps on pnt.survey_id = cahps.survey_id
inner join prmretlp.cv_survey_question_map as caq on cahps.varname = caq.varname and pnt.service = caq.service


UNION ALL

select 
pnt.client_id
, pnt.survey_id
, pnt.service
, pnt.disdate
, pnt.recdate
, anl.value as resp_val
, anl.varname
, aquest.question_text
, aquest.section
, aquest.standard
, aquest.screening
, aquest.top_box_answer
, aquest.top_box_scale
, 'OTHER' as survey_type
, '' as sentiment
from 
prmretlp.cv_survey_patient as pnt
inner join prmretlp.cv_survey_analysis as anl on pnt.survey_id = anl.survey_id
inner join prmretlp.cv_survey_question_map as aquest on anl.varname = aquest.varname and pnt.service = aquest.service


UNION ALL

select 
pnt.client_id
, pnt.survey_id
, pnt.service
, pnt.disdate
, pnt.recdate
, cmnt.varname
, cmnt.value as resp_val
, cquest.question_text
, cquest.section
, cquest.standard
, cquest.screening
, cquest.top_box_answer
, cquest.top_box_scale
, 'SURVEY COMMENTS' as survey_type
, cmnt.sentiment
from 
prmretlp.cv_survey_patient as pnt
inner join prmretlp.cv_survey_comments as cmnt on pnt.survey_id = cmnt.survey_id
inner join prmretlp.cv_survey_question_map as cquest on cmnt.varname = cquest.varname and pnt.service = cquest.service
) as asf
;
