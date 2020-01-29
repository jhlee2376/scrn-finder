const { class_drug, class_measurement, class_condition, class_visit} = require('./class.js');


var sql = require('mssql');
    //설정
var dbConfig = {
    user: 'sa',
    password: 'scrn123!!',
    server: 'localhost',
    port: '1433',
    database: 'SCRN_CLOUD',
    connectionTimeout: 3000000,
    requestTimeout: 3000000,
    pool:{
        idleTimeoutMillis :15000000,
        requestTimeout: 15000000
    }
}

asyncMain(1);
//console.log(class_drug(1,2,3,4,5,6,7));


////////////여기는 함수들이 있습니다////////////////////////
async function asyncMain(protocol_id, res) {
    try {
        let pool = await sql.connect(dbConfig);
       // let result = [];

        //해당 사람 리스트 가져오기.
        // drug, condition. measurement, visit 순으로 가져오기

        let person_list = '';

        let drug_info = [];
        let measurement_info = [];
        let condition_info = [];
        let visit_info =[];

        let result_person = await pool.request()
            .input('input_paramiter', sql.Int, protocol_id)
            .query('SELECT person_id FROM dbo.protocol_result WHERE protocol_id = @input_paramiter')
        person_list = make_list_to_string(result_person);  //console_println(person_list); //log찍기
        
        ////약.
        let result_drug = await pool.request()
            .query('SELECT p1.gender_concept_id, d1.drug_concept_id, d1.drug_exposure_start_date, d1.drug_exposure_end_date, d1.quantity, d1.days_supply, d1.dose_unit_source_value FROM (SELECT person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, quantity, days_supply, dose_unit_source_value FROM BIGDATADB.dbo.DRUG_EXPOSURE WHERE BIGDATADB.dbo.DRUG_EXPOSURE.person_id in('+person_list+')) d1, BIGDATADB.dbo.PERSON p1 WHERE d1.person_id = p1.person_id');
        // durg 결과 내용을 리스트로 만들기.
        drug_info = make_result_drug_to_list(result_drug.recordset);    //console_println(drug_info.length);


        ////Lab
        let result_measurement = await pool.request()
            .query('SELECT p1.gender_concept_id, d1.measurement_concept_id, d1.measurement_date, d1.value_as_number, d1.range_low, d1.range_high, d1.unit_source_value FROM (SELECT person_id, measurement_concept_id, measurement_date, value_as_number, range_low, range_high, unit_source_value FROM BIGDATADB.dbo.MEASUREMENT WHERE BIGDATADB.dbo.MEASUREMENT.person_id in('+person_list+')) d1, BIGDATADB.dbo.PERSON p1 WHERE d1.person_id = p1.person_id');
        measurement_info= make_result_measurement_to_list(result_measurement.recordset);    //console_println(measurement_info.length);

        ////Condition
        let result_condition = await pool.request()
            .query('SELECT p1.gender_concept_id, d1.condition_concept_id, d1.condition_start_date FROM (SELECT person_id, condition_concept_id, condition_start_date FROM BIGDATADB.dbo.CONDITION_OCCURRENCE WHERE BIGDATADB.dbo.CONDITION_OCCURRENCE.person_id in('+person_list+')) d1, BIGDATADB.dbo.PERSON p1 WHERE d1.person_id = p1.person_id');
        condition_info= make_result_condition_to_list(result_condition.recordset);  //console_println(condition_info.length);

        ////Visit
        let result_visit = await pool.request()
        .query('SELECT p1.gender_concept_id, d1.visit_concept_id, d1.visit_start_date, d1.visit_end_date, d1.care_site_id FROM (SELECT person_id, visit_concept_id, visit_start_date, visit_end_date,care_site_id FROM BIGDATADB.dbo.VISIT_OCCURRENCE WHERE BIGDATADB.dbo.VISIT_OCCURRENCE.person_id in('+person_list+')) d1, BIGDATADB.dbo.PERSON p1 WHERE d1.person_id = p1.person_id');
        visit_info= make_result_visit_to_list(result_visit.recordset); //console_println(visit_info);

        let result ={
            drug_info: drug_info,
            measurement_info: measurement_info,
            condition_info: condition_info,
            visit_info:visit_info
        }; console_println(result);
        //res.send(result);

    }catch(err){
        console.log({error: err});
    }

    // 필요한 내부 함수 부분입니다//
    function make_result_visit_to_list(recordset){
        let result = [];
        for (i = 0; i < recordset.length; i++) {
            result.push(class_visit(
                recordset[i].gender_concept_id,
                recordset[i].visit_concept_id,
                new Date(recordset[i].visit_start_date).format("yyyy-MM-dd"),
                new Date(recordset[i].visit_end_date).format("yyyy-MM-dd"),
                recordset[i].care_site_id
            ))
        }
        return result;
    }

    function make_result_condition_to_list(recordset) {
        let result = [];
        for (i = 0; i < recordset.length; i++) {
            result.push(class_condition(
                recordset[i].gender_concept_id,
                recordset[i].condition_concept_id,
                new Date(recordset[i].condition_start_date).format("yyyy-MM-dd")
            ))
        }

        return result;
    }

    //durg 결과 내용을 리스트로 만들기. 
    function make_result_drug_to_list(recordset){
        let result = [];
        for(i=0;i<recordset.length;i++){
            result.push(class_drug(
                recordset[i].gender_concept_id,
                recordset[i].drug_concept_id,
                new Date(recordset[i].drug_exposure_start_date).format("yyyy-MM-dd"),
                new Date(recordset[i].drug_exposure_end_date).format("yyyy-MM-dd"),
                recordset[i].quantity,
                recordset[i].days_supply,
                recordset[i].dose_unit_source_value
                ))
        }

        return result;
    }

    //measurement_info 결과 내용을 리스트로 만들기.
    function make_result_measurement_to_list(recordset){
        let result = [];
        for(i=0;i<recordset.length;i++){
            result.push(class_measurement(
                recordset[i].gender_concept_id,
                recordset[i].measurement_concept_id,
                new Date(recordset[i].measurement_date).format("yyyy-MM-dd"),
                recordset[i].value_as_number,
                recordset[i].range_low,
                recordset[i].range_high,
                recordset[i].unit_source_value
                ))
        }

        return result;
    }

    //환자번호 목록 리스트를 String 으로 다 붙여 넣는 함수 
    //[1,2,3,4,5] => "1,2,3,4,5"
    function make_list_to_string(obj){
        let result='';
    
        for(i=0;i<obj.recordset.length;i++){
            result += obj.recordset[i].person_id;
            if(i+1 != obj.recordset.length ){
                result += ", ";
            }else{
                result += " ";
            }
        }
       
        // result += `(${result})`
        return result;
    }
}



    // 0 : select, 1: delete, 2: insert, 3: update
async function mssql(protocol_id, condition, dbconfig){
    if(condition==1){
        try{
            let pool = await sql.connect(dbconfig);
            let result1 = await pool.request()
                .input('input_paramiter', sql.Int, protocol_id)
                .query('SELECT * FROM BIGDATADB.dbo.MEASUREMENT WHERE BIGDATADB.dbo.MEASUREMENT.person_id in (SELECT person_id FROM dbo.protocol_result WHERE protocol_id = @input_paramiter')
            
            
            console.dir({result: result1.recordset});
    
        }catch(err){
            console.log({error: err});
        }
    }else{
        try{
    //         SELECT * FROM BIGDATADB.dbo.CONDITION_OCCURRENCE WHERE BIGDATADB.dbo.CONDITION_OCCURRENCE.person_id in (SELECT person_id FROM dbo.protocol_result WHERE protocol_id =3);
	
            let pool = await sql.connect(dbconfig);
            let result1 = await pool.request()
                .input('input_paramiter', sql.Int, protocol_id)
                .query('SELECT * FROM BIGDATADB.dbo.CONDITION_OCCURRENCE WHERE BIGDATADB.dbo.CONDITION_OCCURRENCE.person_id in (SELECT person_id FROM dbo.protocol_result WHERE protocol_id = @input_paramiter')
            
            
            console.dir({result: result1.recordset});
    
        }catch(err){
            console.log({error: err});
        }
    }
    
}

//부수적인 함수

//날짜 형식 바꾸는 함수
Date.prototype.format = function (f) {
    if (!this.valueOf()) return " ";

    var weekName = ["일요일", "월요일", "화요일", "수요일", "목요일", "금요일", "토요일"];
    var d = this;

    return f.replace(/(yyyy|yy|MM|dd|E|hh|mm|ss|a\/p)/gi, function ($1) {
        switch ($1) {
            case "yyyy": return d.getFullYear();
            case "yy": return (d.getFullYear() % 1000).zf(2);
            case "MM": return (d.getMonth() + 1).zf(2);
            case "dd": return d.getDate().zf(2);
            case "E": return weekName[d.getDay()];
            case "HH": return d.getHours().zf(2);
            case "hh": return ((h = d.getHours() % 12) ? h : 12).zf(2);
            case "mm": return d.getMinutes().zf(2);
            case "ss": return d.getSeconds().zf(2);
            case "a/p": return d.getHours() < 12 ? "오전" : "오후";
            default: return $1;
        }
    });
};

String.prototype.string = function (len) { var s = '', i = 0; while (i++ < len) { s += this; } return s; };
String.prototype.zf = function (len) { return "0".string(len - this.length) + this; };
Number.prototype.zf = function (len) { return this.toString().zf(len); };


//로그를 찍어내는 함수.
function console_println(mes){
    console.log(mes);
    console.log();
}