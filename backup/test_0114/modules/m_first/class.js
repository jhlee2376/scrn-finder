function class_drug(person_id, gender, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, quantity, days_supply, dose_unit_source_value, concept_name, atc){
    var obj = {
        person_id: person_id,
        gender: gender,
        drug_concept_id: drug_concept_id,
        drug_exposure_start_date: drug_exposure_start_date,
        drug_exposure_end_date: drug_exposure_end_date,
        quantity: quantity,
        days_supply: days_supply,
        dose_unit_source_value: dose_unit_source_value,
        concept_name :  concept_name,
        atc: atc
    }
    return obj;
}
function class_measurement(person_id, gender, measurement_concept_id, measurement_date, value_as_number, range_low, range_high, unit_source_value, concept_name){
    var obj = {
        person_id: person_id,
        gender: gender,
        measurement_concept_id: measurement_concept_id,
        measurement_date: measurement_date,
        value_as_number: value_as_number,
        range_low: range_low,
        range_high: range_high,
        unit_source_value: unit_source_value,
        concept_name: concept_name
    }
    return obj;
}
function class_condition(person_id, gender, condition_concept_id, condition_start_date, concept_name, icd10) {
    var obj = {
        person_id: person_id,
        gender: gender,
        condition_concept_id: condition_concept_id,
        condition_start_date: condition_start_date,
        concept_name: concept_name,
        icd10: icd10
    }
    return obj;
}
function class_visit(person_id, gender, visit_concept_id, visit_start_date, visit_end_date, care_site_id) {
    var obj = {
        person_id: person_id,
        gender: gender,
        visit_concept_id: visit_concept_id,
        visit_start_date: visit_start_date,
        visit_end_date: visit_end_date,
        care_site_id:care_site_id
    }
    return obj;
}


function class_person(gender, year) {
    var obj = {
        gender: gender,
        year: year
    }
    return obj
}

function class_distribution_result(name, person_value){
    var obj ={
        name : name,
        person_count: person_value
    }
    return obj;
}

function class_distribution_result_measurement(name, avg, sd, min, max, person_value,low,high){
    var obj = {
        name : name, 
        avg: avg,
        sd: sd, 
        min: min,
        max: max,
        person_count: person_value,
        low:low,
        high:high
    }
    return obj;
}


function class_protocol_condition(id, name, person_id){
    var obj = {
        condition_id : id, 
        condition_name : name, 
        person_list : [person_id]
    }
    return obj;
}

function class_protocol_measurement(id,name,value,person_id,low, high){
    var obj={
        measurement_concept_id : id, 
        measurement_name : name, 
        value_as_number : [value],
        max : value,
        min : value, 
        person_list : [person_id],
        low:low,
        high: high
    }
    return obj;
}

function Lab(person_id, measurement_concept_id, name, alt, glucose1, glucose2, glucose3, glucose4, glucose5, alanine, hba1c){
    var obj ={
        id: person_id,
        concept_id: measurement_concept_id,
        name:name,
        alt: alt,
        glucose1: glucose1,
        glucose2: glucose2,
        glucose3: glucose3,
        glucose4: glucose4,
        glucose5: glucose5,
        alanine: alanine,
        hba1c: hba1c
    }
    return obj;
}

function Atc_code(second, third, forth, fifth){
    
    var obj = [second, third ,forth, fifth ]
    
    return obj
}//Atc_code
function Icd10_code(mid, small, concept_name){
    var obj =  [mid, small, concept_name]
    
    return obj;
}//Icd10_code

module.exports ={
    class_drug,
    class_measurement,
    class_condition,
    class_visit,
    class_person,
    class_distribution_result,
    class_protocol_condition,
    class_distribution_result_measurement,
    class_protocol_measurement,
    Lab,
    Atc_code,Icd10_code
}

