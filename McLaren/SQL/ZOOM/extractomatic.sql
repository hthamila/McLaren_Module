alter table cv_paticd9_10 add column hac_status character varying(25);
alter table cv_paticd9_10 add column clinical_sequence integer;
generate statistics on cv_paticd9_10;

alter table cv_pattrans rename to prior_06302021_cv_pattrans;
alter table cv_patbill rename to prior_06302021_cv_patbill;
alter table stg_patdisch rename to prior_06302021_stg_patdisch;


--Modifying Pat Trans

CREATE TABLE cv_pattrans
(
	rcrd_load_type character(1),
	rcrd_isrt_pcs_nm character varying(255),
	rcrd_isrt_ts timestamp,
	rcrd_udt_pcs_nm character varying(255),
	rcrd_udt_ts timestamp,
	rcrd_src_isrt_id character varying(255),
	rcrd_src_isrt_ts timestamp,
	rcrd_src_udt_id character varying(255),
	rcrd_src_udt_ts timestamp,
	rcrd_src_file_nm character varying(255),
	rcrd_btch_audt_id integer,
	rcrd_pce_cst_nm character varying(255),
	rcrd_pce_cst_src_nm character varying(255),
	facility character varying(25),
	account character varying(25),
	department character varying(25),
	transcode character varying(25),
	receiveddate timestamp,
	postdate date,
	transcodedesc character varying(75),
	transtype character varying(25),
	amount double precision,
	covered double precision,
	noncovered double precision,
	deductible double precision,
	coinsurance double precision,
	subaccount character varying(50),
	revenuecode character varying(4),
	cpt4code character varying(7),
	modifier1 character varying(2),
	modifier2 character varying(2),
	modifier3 character varying(2),
	modifier4 character varying(2),
	payorplancode character varying(10),
	remitid character varying(50),
	extracteddate character varying(50),
	sourcesystem character varying(25),
	invoiceid character varying(25),
	transaction_primary_key character varying(25),
	transaction_posting_primary_key character varying(25),
	gl_activity_datetime character varying(255),
	gl_interface_datetime character varying(255),
	transaction_subtype character varying(255),
	transaction_reason character varying(255),
	debit_or_credit_ind character varying(255),
	remittance_batch_id character varying(255),
	claim_status character varying(255),
	patient_responsibility numeric(19,8),
	charge_group_bo_description character varying(255),
	charge_group_billtype character varying(255),
	charge_group_status character varying(50),
	bill_format character varying(10),
	financial_class character varying(50),
	benefit_order_id character varying(25),
	benefit_order_health_plan_reltn character varying(25),
	reclass_indicator character varying(1)

)
DISTRIBUTE ON (facility, account);

insert into cv_pattrans
(
rcrd_load_type
       , rcrd_isrt_pcs_nm
       , rcrd_isrt_ts
       , rcrd_udt_pcs_nm
       , rcrd_udt_ts
       , rcrd_src_isrt_id
       , rcrd_src_isrt_ts
       , rcrd_src_udt_id
       , rcrd_src_udt_ts
       , rcrd_src_file_nm
       , rcrd_btch_audt_id
       , rcrd_pce_cst_nm
       , rcrd_pce_cst_src_nm
       , facility
       , account
       , department
       , transcode
       , receiveddate
       , postdate
       , transcodedesc
       , transtype
       , amount
       , covered
       , noncovered
       , deductible
       , coinsurance
       , subaccount
       , revenuecode
       , cpt4code
       , modifier1
       , modifier2
       , modifier3
       , modifier4
       , payorplancode
       , remitid
       , extracteddate
       , sourcesystem
       , invoiceid

)
SELECT rcrd_load_type
       , rcrd_isrt_pcs_nm
       , rcrd_isrt_ts
       , rcrd_udt_pcs_nm
       , rcrd_udt_ts
       , rcrd_src_isrt_id
       , rcrd_src_isrt_ts
       , rcrd_src_udt_id
       , rcrd_src_udt_ts
       , rcrd_src_file_nm
       , rcrd_btch_audt_id
       , rcrd_pce_cst_nm
       , rcrd_pce_cst_src_nm
       , facility
       , account
       , department
       , transcode
       , receiveddate
       , postdate
       , transcodedesc
       , transtype
       , amount
       , covered
       , noncovered
       , deductible
       , coinsurance
       , subaccount
       , revenuecode
       , cpt4code
       , modifier1
       , modifier2
       , modifier3
       , modifier4
       , payorplancode
       , remitid
       , extracteddate
       , sourcesystem
       , invoiceid

  FROM pce_qe16_oper_prd_zoom..prior_06302021_cv_pattrans;


CREATE TABLE cv_patbill
(
	rcrd_load_type character(1) NOT NULL,
	rcrd_isrt_pcs_nm character varying(255) NOT NULL,
	rcrd_isrt_ts timestamp NOT NULL,
	rcrd_udt_pcs_nm character varying(255),
	rcrd_udt_ts timestamp,
	rcrd_src_isrt_id character varying(255),
	rcrd_src_isrt_ts timestamp,
	rcrd_src_udt_id character varying(255),
	rcrd_src_udt_ts timestamp,
	rcrd_src_file_nm character varying(255) NOT NULL,
	rcrd_btch_audt_id integer NOT NULL,
	rcrd_pce_cst_nm character varying(255) NOT NULL,
	rcrd_pce_cst_src_nm character varying(255) NOT NULL,
	company_id national character varying(25),
	patient_id national character varying(25),
	service_date character varying(25),
	charge_code national character varying(25),
	quantity bigint,
	total_charge numeric(19,4),
	total_variable_cost numeric(19,4),
	total_fixed_cost numeric(19,4),
	cpt_code national character varying(8),
	revenue_code character(4),
	ordering_practitioner_code national character varying(50),
	cpt_modifier_1 national character varying(25),
	cpt_modifier_2 national character varying(25),
	cpt_modifier_3 national character varying(25),
	cpt_modifier_4 national character varying(25),
	dept national character varying(25),
	postdate character varying(25),
	unitcharge numeric(19,4),
	invoiceid national character varying(25),
	performingphysician national character varying(50),
	cpt4full national character varying(25),
	subaccount national character varying(25),
	chargecodedesc national character varying(75),
	financialclass national character varying(25),
	payorplancode national character varying(25),
	updatedate character varying(25),
	sourcesystem national character varying(25),
	raw_chargcode national character varying(25),
	organization character varying(75),
	billitemid character varying(25),
	chargeitemid character varying(25),
	ndc character varying(25),
	ndcunits integer,
	ndcunitofmeasure character varying(30),
	financial_charge_primary_key character varying(25),
	hcpcs character varying(5),
	charge_type character varying(25),
	professional_charge_indicator character varying(1),
	diagnosis_1 character varying(10),
	diagnosis_2 character varying(10),
	diagnosis_3 character varying(10),
	diagnosis_4 character varying(10),
	diagnosis_5 character varying(10),
	service_location_facility character varying(50),
	service_location_building character varying(50),
	service_location_nurse_unit character varying(50),
	service_location_room character varying(50),
	service_location_bed character varying(50),
	accommodation character varying(50),
	late_charge_status character varying(50),
	activity_type character varying(50),
	service_resource character varying(250),
	manual_indicator character varying(1),
	charge_status character varying(50),
	price_schedule character varying(50),
	tier_group character varying(50),
	provider_speciality character varying(50),
	activity_datetime timestamp,
	service_datetime timestamp,
	adjusted_datetime timestamp,
	credited_datetime timestamp,
	cancelled_datetime timestamp,
	gl_activity_datetime timestamp,
	gl_interface_datetime timestamp,
	work_rvu numeric(19,8),
	charge_problem character varying(1),
	hcpcs_desc character varying(255),
	encounter_location_facility character varying(50),
	encounter_location_building character varying(50),
	encounter_location_nurse_unit character varying(50),
	encounter_location_room character varying(50),
	encounter_location_bed character varying(50),
	ndc_description character varying(255),
	reclass_indicator character varying(1)
)
DISTRIBUTE ON (company_id, patient_id);

insert into cv_patbill
(
rcrd_load_type
       , rcrd_isrt_pcs_nm
       , rcrd_isrt_ts
       , rcrd_udt_pcs_nm
       , rcrd_udt_ts
       , rcrd_src_isrt_id
       , rcrd_src_isrt_ts
       , rcrd_src_udt_id
       , rcrd_src_udt_ts
       , rcrd_src_file_nm
       , rcrd_btch_audt_id
       , rcrd_pce_cst_nm
       , rcrd_pce_cst_src_nm
       , company_id
       , patient_id
       , service_date
       , charge_code
       , quantity
       , total_charge
       , total_variable_cost
       , total_fixed_cost
       , cpt_code
       , revenue_code
       , ordering_practitioner_code
       , cpt_modifier_1
       , cpt_modifier_2
       , cpt_modifier_3
       , cpt_modifier_4
       , dept
       , postdate
       , unitcharge
       , invoiceid
       , performingphysician
       , cpt4full
       , subaccount
       , chargecodedesc
       , financialclass
       , payorplancode
       , updatedate
       , sourcesystem
       , raw_chargcode
       , organization
       , billitemid
       , chargeitemid
       , ndc
       , ndcunits
       , ndcunitofmeasure
)
SELECT rcrd_load_type
       , rcrd_isrt_pcs_nm
       , rcrd_isrt_ts
       , rcrd_udt_pcs_nm
       , rcrd_udt_ts
       , rcrd_src_isrt_id
       , rcrd_src_isrt_ts
       , rcrd_src_udt_id
       , rcrd_src_udt_ts
       , rcrd_src_file_nm
       , rcrd_btch_audt_id
       , rcrd_pce_cst_nm
       , rcrd_pce_cst_src_nm
       , company_id
       , patient_id
       , service_date
       , charge_code
       , quantity
       , total_charge
       , total_variable_cost
       , total_fixed_cost
       , cpt_code
       , revenue_code
       , ordering_practitioner_code
       , cpt_modifier_1
       , cpt_modifier_2
       , cpt_modifier_3
       , cpt_modifier_4
       , dept
       , postdate
       , unitcharge
       , invoiceid
       , performingphysician
       , cpt4full
       , subaccount
       , chargecodedesc
       , financialclass
       , payorplancode
       , updatedate
       , sourcesystem
       , raw_chargcode
       , organization
       , billitemid
       , chargeitemid
       , ndc
       , ndcunits
       , ndcunitofmeasure

  FROM pce_qe16_oper_prd_zoom..prior_06302021_cv_patbill;

CREATE TABLE stg_patdisch
(
	rcrd_load_type character(1),
	rcrd_isrt_pcs_nm character varying(255),
	rcrd_isrt_ts timestamp,
	rcrd_udt_pcs_nm character varying(255),
	rcrd_udt_ts timestamp,
	rcrd_src_isrt_id character varying(255),
	rcrd_src_isrt_ts timestamp,
	rcrd_src_udt_id character varying(255),
	rcrd_src_udt_ts timestamp,
	rcrd_src_file_nm character varying(255),
	rcrd_btch_audt_id integer,
	rcrd_pce_cst_nm character varying(255),
	rcrd_pce_cst_src_nm character varying(255),
	company_id national character varying(25),
	inpatient_outpatient_flag character(2),
	medical_record_number national character varying(20),
	patient_id national character varying(25),
	admissionarrival_date character varying(25),
	discharge_date character varying(25),
	length_of_stay integer,
	msdrg_code national character varying(3),
	apr_code national character varying(5),
	apr_severity_of_illness national character varying(5),
	apr_risk_of_mortality integer,
	patient_type national character varying(50),
	primary_payer_code national character varying(20),
	secondary_payer_code national character varying(20),
	attending_practitioner_code national character varying(25),
	admitting_practitioner_code national character varying(25),
	consulting_practitioner_code_1 national character varying(25),
	consulting_practitioner_code_2 national character varying(12),
	consulting_practitioner_code_3 national character varying(12),
	discharge_total_charges numeric(19,8),
	discharge_variable_cost numeric(19,8),
	discharge_fixed_cost numeric(19,8),
	reimbursement_amount numeric(19,8),
	age_in_years smallint,
	birth_date character varying(25),
	babys_patient_number national character varying(30),
	sex national character varying(10),
	residential_zip_code national character varying(10),
	admission_type_visit_type national character varying(20),
	point_of_origin_for_admission_or_visit national character varying(20),
	discharge_status national character varying(50),
	employer_code national character varying(25),
	state_of_patient_origin national character varying(2),
	county_of_patient_origin national character varying(35),
	race national character varying(50),
	marital_status national character varying(50),
	birth_weight_in_grams numeric(19,8),
	days_on_mechanical_ventilator integer,
	smoker_flag national character varying(1),
	weight_in_lbs numeric(19,8),
	ethnicity_code national character varying(50),
	ed_visit national character varying(1),
	ccn_care_setting national character varying(3),
	patient_hic_number national character varying(12),
	tin national character varying(10),
	admit_time character varying(25),
	discharge_time character varying(25),
	patient_first_name national character varying(25),
	patient_middle_name national character varying(25),
	patient_last_name national character varying(35),
	subfacility national character varying(100),
	accountstatus national character varying(25),
	readmissionflag national character varying(3),
	previousdischargedate character varying(25),
	namesuffix national character varying(15),
	address1 national character varying(35),
	address2 national character varying(35),
	city national character varying(35),
	zipplus4 national character varying(4),
	employercode national character varying(50),
	primaryicd9diagnosiscode national character varying(6),
	primaryicd10diagnosiscode national character varying(8),
	primaryicd9procedurecode national character varying(7),
	primaryicd10procedurecode national character varying(7),
	admitdiagnosiscode national character varying(8),
	admitservice national character varying(25),
	dischargeservice national character varying(50),
	nursingstation national character varying(15),
	financialclass national character varying(50),
	financialclassoriginal national character varying(50),
	tertiarypayorplan national character varying(20),
	quaternarypayorplan national character varying(20),
	finalbillflag national character varying(1),
	finalbilldate character varying(25),
	totaladjustments numeric(19,8),
	accountbalance numeric(19,8),
	expectedpayment numeric(19,8),
	updatedate character varying(25),
	updateid national character varying(25),
	sourcesystem national character varying(25),
	patientphonenumber character varying(14),
	mothersaccount character varying(25),
	mothersname character varying(85),
	patientssn character varying(11),
	fin character varying(25),
	fin_string character varying(255),
	fin_with_recur_sequence character varying(25),
	recur_sequence integer,
	recur_service_month integer,
	recur_service_year integer,
	calculated_balance numeric(19,8),
	clinical_encounter_primary_key character varying(25),
	person_patient_primary_key character varying(25),
	preregistration_date_time timestamp,
	inpatient_admit_date_time timestamp,
	collection_status character varying(50),
	combine_status character varying(50),
	combined_into_encounter_id character varying(25),
	bad_dept_date_time timestamp,
	bad_dept_balance numeric(19,8),
	account_balance_last_change_datetime timestamp,
	zero_balance_datetime timestamp,
	coding_status character varying(50),
	coding_start_datetime timestamp,
	coding_complete_datetime timestamp,
	coding_last_updated_by_username character varying(50),
	coding_last_updated_datetime timestamp,
	coding_source_system character varying(50),
	decimal_length_of_stay numeric(19,8),
	patient_email character varying(100),
	snf_facility character varying(100),
	working_drg character(50)
)
DISTRIBUTE ON (company_id, patient_id);





insert into stg_patdisch
(
rcrd_load_type
       , rcrd_isrt_pcs_nm
       , rcrd_isrt_ts
       , rcrd_udt_pcs_nm
       , rcrd_udt_ts
       , rcrd_src_isrt_id
       , rcrd_src_isrt_ts
       , rcrd_src_udt_id
       , rcrd_src_udt_ts
       , rcrd_src_file_nm
       , rcrd_btch_audt_id
       , rcrd_pce_cst_nm
       , rcrd_pce_cst_src_nm
       , company_id
       , inpatient_outpatient_flag
       , medical_record_number
       , patient_id
       , admissionarrival_date
       , discharge_date
       , length_of_stay
       , msdrg_code
       , apr_code
       , apr_severity_of_illness
       , apr_risk_of_mortality
       , patient_type
       , primary_payer_code
       , secondary_payer_code
       , attending_practitioner_code
       , admitting_practitioner_code
       , consulting_practitioner_code_1
       , consulting_practitioner_code_2
       , consulting_practitioner_code_3
       , discharge_total_charges
       , discharge_variable_cost
       , discharge_fixed_cost
       , reimbursement_amount
       , age_in_years
       , birth_date
       , babys_patient_number
       , sex
       , residential_zip_code
       , admission_type_visit_type
       , point_of_origin_for_admission_or_visit
       , discharge_status
       , employer_code
       , state_of_patient_origin
       , county_of_patient_origin
       , race
       , marital_status
       , birth_weight_in_grams
       , days_on_mechanical_ventilator
       , smoker_flag
       , weight_in_lbs
       , ethnicity_code
       , ed_visit
       , ccn_care_setting
       , patient_hic_number
       , tin
       , admit_time
       , discharge_time
       , patient_first_name
       , patient_middle_name
       , patient_last_name
       , subfacility
       , accountstatus
       , readmissionflag
       , previousdischargedate
       , namesuffix
       , address1
       , address2
       , city
       , zipplus4
       , employercode
       , primaryicd9diagnosiscode
       , primaryicd10diagnosiscode
       , primaryicd9procedurecode
       , primaryicd10procedurecode
       , admitdiagnosiscode
       , admitservice
       , dischargeservice
       , nursingstation
       , financialclass
       , financialclassoriginal
       , tertiarypayorplan
       , quaternarypayorplan
       , finalbillflag
       , finalbilldate
       , totaladjustments
       , accountbalance
       , expectedpayment
       , updatedate
       , updateid
       , sourcesystem
       , patientphonenumber
       , mothersaccount
       , mothersname
       , patientssn
)
SELECT rcrd_load_type
       , rcrd_isrt_pcs_nm
       , rcrd_isrt_ts
       , rcrd_udt_pcs_nm
       , rcrd_udt_ts
       , rcrd_src_isrt_id
       , rcrd_src_isrt_ts
       , rcrd_src_udt_id
       , rcrd_src_udt_ts
       , rcrd_src_file_nm
       , rcrd_btch_audt_id
       , rcrd_pce_cst_nm
       , rcrd_pce_cst_src_nm
       , company_id
       , inpatient_outpatient_flag
       , medical_record_number
       , patient_id
       , admissionarrival_date
       , discharge_date
       , length_of_stay
       , msdrg_code
       , apr_code
       , apr_severity_of_illness
       , apr_risk_of_mortality
       , patient_type
       , primary_payer_code
       , secondary_payer_code
       , attending_practitioner_code
       , admitting_practitioner_code
       , consulting_practitioner_code_1
       , consulting_practitioner_code_2
       , consulting_practitioner_code_3
       , discharge_total_charges
       , discharge_variable_cost
       , discharge_fixed_cost
       , reimbursement_amount
       , age_in_years
       , birth_date
       , babys_patient_number
       , sex
       , residential_zip_code
       , admission_type_visit_type
       , point_of_origin_for_admission_or_visit
       , discharge_status
       , employer_code
       , state_of_patient_origin
       , county_of_patient_origin
       , race
       , marital_status
       , birth_weight_in_grams
       , days_on_mechanical_ventilator
       , smoker_flag
       , weight_in_lbs
       , ethnicity_code
       , ed_visit
       , ccn_care_setting
       , patient_hic_number
       , tin
       , admit_time
       , discharge_time
       , patient_first_name
       , patient_middle_name
       , patient_last_name
       , subfacility
       , accountstatus
       , readmissionflag
       , previousdischargedate
       , namesuffix
       , address1
       , address2
       , city
       , zipplus4
       , employercode
       , primaryicd9diagnosiscode
       , primaryicd10diagnosiscode
       , primaryicd9procedurecode
       , primaryicd10procedurecode
       , admitdiagnosiscode
       , admitservice
       , dischargeservice
       , nursingstation
       , financialclass
       , financialclassoriginal
       , tertiarypayorplan
       , quaternarypayorplan
       , finalbillflag
       , finalbilldate
       , totaladjustments
       , accountbalance
       , expectedpayment
       , updatedate
       , updateid
       , sourcesystem
       , patientphonenumber
       , mothersaccount
       , mothersname
       , patientssn

  FROM pce_qe16_oper_prd_zoom..prior_06302021_stg_patdisch;

