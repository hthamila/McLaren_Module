drop table cv_measureresults if exists;
CREATE TABLE cv_measureresults as
SELECT transaction_id
        ,provider_npi
        ,location_tin
        ,organization_npi
        ,ccn
        ,product_code
        ,batch_id
        ,measure_id
        ,measure_sub_id
        ,measure_period_start
        ,measure_period_end
        ,calculated_for_date
        ,patient_guid
        ,case when is_ipp::varchar(5)='TRUE' then 1 else 0 end  is_ipp
        ,case when is_denom_exclusion::varchar(5)='TRUE' then 1 else 0 end  is_denom_exclusion
        ,case when is_denom_exception::varchar(5)='TRUE' then 1 else 0 end  is_denom_exception
        ,case when is_numerator::varchar(5)='TRUE' then 1 else 0 end  is_numerator
        ,case when is_denominator::varchar(5)='TRUE' then 1 else 0 end  is_denominator
        ,case when is_notmet::varchar(5)='TRUE' then 1 else 0 end  is_notmet
        ,case when msrpopl::varchar(5)='TRUE' then 1 else 0 end  msrpopl
        ,observ
        ,client_id
        ,patient_id
        ,alt_location_id
        ,eligibility_date
        ,numerator_crit_date
        ,parent_premier_entity_clazz
        ,parent_premier_entity_code
        ,premier_entity_clazz
        ,premier_entity_code
	,rcrd_isrt_ts
	,rcrd_btch_audt_id

  FROM pce_qe16_misc_prd_zoom..stg_measureresults
DISTRIBUTE ON (patient_guid, measure_id, measure_period_start, measure_period_end);
