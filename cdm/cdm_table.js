var cdm={"table":
    [
        {"tableName":"person","fieldSet":["gender_concept_id","year_of_birth"]},
        {"tableName":"drug_exposure","fieldSet":["drug_concept_id","drug_exposure_start_date","drug_exposure_end_date","drug_source_value"]},
        {"tableName":"condition_occurrence","fieldSet":["condition_concept_id","condition_start_date","condition_end_date"]},
        {"tableName":"measurement","fieldSet":["measurement_concept_id","measurement_date","value_as_number"]},
        {"tableName":"visit_occurence","fieldSet":["visit_start_date","visit_type_concept_id"]}
    ]
}
cdm = cdm.table;
module.exports ={
    cdm
}
