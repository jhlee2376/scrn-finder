const { class_drug, class_measurement, class_condition, class_visit,class_person,class_distribution_result} = require('./class.js');
// 모듈을 추출
var dao = require('./serverSource/dao');

var http = require('http');
var express = require('express');
var cors = require('cors');
const utf8 = require('utf8');
var bodyParser =require('body-parser');
var chalk = require('chalk');

var fs = require('fs');

var sql = require('mssql');

var dbConfig = {
    user: 'sa',
    password: 'scrn123!!',
    server: '127.0.0.1',
    port: '1433',
    database: 'SCRN_CLOUD',
    connectionTimeout: 3000000,
    requestTimeout: 3000000,
    pool:{
        idleTimeoutMillis :15000000,
        requestTimeout: 15000000
    }
}

var CdmDbConfig = {
    user: 'sa',
    password: 'scrn123!!',
    server: '127.0.0.1',
    port: '1433',
    database: 'BIGDATADB',
    connectionTimeout: 3000000,
    requestTimeout: 3000000,
    pool:{
        idleTimeoutMillis :15000000,
        requestTimeout: 15000000
    }
}

//웹 서버를 생성
var app = express();
app.use(cors());
app.use(express.static('public'));
app.use(bodyParser.json());



////////////////CDM 기본 설정
var cdm={"table":
[
    {"tableName":"person","fieldSet":["gender_concept_id","year_of_birth"]},
    {"tableName":"drug_exposure","fieldSet":["drug_concept_id","drug_exposure_start_date","drug_exposure_end_date","drug_source_value"]},
    {"tableName":"condition_occurrence","fieldSet":["condition_concept_id","condition_start_date","condition_end_date"]},
    {"tableName":"measurement","fieldSet":["measurement_concept_id","measurement_date","value_as_number"]},
    {"tableName":"visit_occurence","fieldSet":["visit_start_date","visit_type_concept_id"]}
]}
cdm = cdm.table;

var condition =["<","<=","=",">=",">"];

//SITE
app.get('/test-site-execute/:study_id/:query_id',(req,res)=>{
    util_console_print("----/test-site-execute/:study_id/:query_id----");
    var study_id = Number(req.param('protocol_id'));
    var query_id = Number(req.param('query_id'));
  
    var query ='';

    //let result ;
    asyncMain(study_id, query_id);
    
    ////Async 함수
    async function asyncMain(study_id, query_id) {
        try{
            let pool =  await sql.connect(dbConfig); //scrn_cloud
           
            // 1.실행 버튼 클릭 시 '진행' 상태 변경하기.
            console.log('1.상태 업데이트')
           let result = await pool.request() 
            .input('input_paramiter1', sql.Int, study_id)
            .input('input_paramiter2', sql.Int, query_id)
            .query(`UPDATE query SET query_status = N'In Progress' WHERE study_id = @input_paramiter1 and query_id = @input_paramiter2`)
            //console.log(`UPDATE protocol SET protocol_status = N'InProgress' WHERE protocol_id = @input_paramiter`);
            util_console_print({"1.상태 업데이트": result});
           
            // 2. 조건 쿼리 받아와서 쿼리문 만들기.
            console.log('2. 조건쿼리 받아서 쿼리문 만들기');
            result = await pool.request()
            .input('input_paramiter1', sql.Int, study_id)
            .input('input_paramiter2', sql.Int, query_id)
            .query(`SELECT * FROM criteria_detail WHERE study_id = @input_paramiter1 and query_id = @input_paramiter2`)
            util_console_print({"2. 조건쿼리 받아서 쿼리문 만들기":result});

            var str_or_weak  = asyncMain_2_0_assort_strength(result);  //console.log({result_str_or_weak: str_or_weak}); // 강한조건, 유연조건 나누기. 
            var query_list = asyncMain_2_1_make_query(str_or_weak);
            
            // 3. 만들어진 쿼리문 리스트 각 하나씩 돌려서 결과값 받기. 결과: (의사, 환자, 년도, 성별)
            console.log("----3번시작")
            sql.close();
            pool =  await sql.connect(CdmDbConfig); //scrn_cloud
            var query_result =[];

            for(i=0;i<query_list.length;i++){
                result = await pool.request()
                .query(query_list[0]);
                query_result.push(result.recordset.length);
            }

            // 4. 만들어진 쿼리 리스트 count(*)값 추출
            console.log("----4번시작");
            var list_count = [] ; 
            var buf_query = '';
            for(i=0;i<query_list.length;i++){
                //console.log(query_list[i]);
                buf_query = `SELECT DISTINCT sec.person_id FROM (${query_list[i]}) sec `
                result = await pool.request()
                .query(buf_query);
                list_count.push(result.recordset.length);
            }

            // 5. 마지막 쿼리 조건 조회해서 넣기.
            console.log("----5번시작");
            var result_5 ;
            result = await pool.request()
            .query(query_list[0]);
            result_5 = result;

            

        res.send({resunt:{list_count:list_count, result_5:result_5}});
        sql.close();    
        }catch(err){
            util_console_print({error: err});
            res.send({error:'2.result is 0'});

            sql.close();
        }
       

        function asyncMain_2_0_assort_strength(result){
            var out = {
                strength : [], 
                weak : []
            }
            if(result.rowsAffected[0]==0){
                throw new Error(result);
            }
            rows = result.recordset;
            for(i=0;i<rows.length;i++){
                if(rows[i].strength==1){
                    out.strength.push(rows[i]);
                }else{
                    out.weak.push(rows[i]);
                }
            }
            return out;

        }
        function asyncMain_2_1_make_query(rows_str_weak){
            util_console_print('------asyncMain_2_1_make_query------'); // console.log(rows_str_weak);
            // 강한 조건에서 다시 조건 하나를 넣어준다. 
            // 조건이 넣어지면 다시 선정 제외조건을 나눠서 쿼리문을 만들어준다. 
            // 강한 조건이 끝나면 다시 약한 조건으로 하나씩 쿼리를 만든다. 
            let in_rows=[], ex_rows=[];
            let query_list =[]; 
            let inclusion='' , exclusion='';
           
            //강한조건부터
            let rows = rows_str_weak.strength;          //console.log({rows_count:rows.length});
            for(k=0;k<rows.length;k++){
                //console.log({k:k});
                if(rows[k].criteria_state == 1){
                    in_rows.push(rows[k]);
                }else{
                    ex_rows.push(rows[k]);
                }
                //console.log({in_rows:in_rows.length, ex_rows:ex_rows.length});
                inclusion = make_query_0(in_rows,1); // 선정 1, 제외 0 
                exclusion = make_query_0(ex_rows,0);              //console.log({for: {in:in_rows.length, ex:ex_rows.length}});
                query_list.push(make_query_1(inclusion, exclusion));
            }
            
            //약한조건을 한 개씩 추가해보기
            rows = rows_str_weak.weak;
            //console.log(rows);
            let buf_in_rows =[], buf_ex_rows = [];
            
            for(k=0;k<rows.length;k++){
                //console.log(rows[k]);
                buf_in_rows = JSON.parse(JSON.stringify(in_rows));
                buf_ex_rows = JSON.parse(JSON.stringify(ex_rows));
                if(rows[k].criteria_state ==1){
                    buf_in_rows.push(rows[k]);
                }else{
                    buf_ex_rows.push(rows[k]);
                }
                inclusion = make_query_0(buf_in_rows,1); // 선정 1, 제외 0 
                exclusion = make_query_0(buf_ex_rows,0);              //console.log({for: {in:in_rows.length, ex:ex_rows.length}});
                query_list.push(make_query_1(inclusion, exclusion));
            }

            return query_list;
            
        }
        function make_query_0(rows, in_or_ex) {
            //util_console_print('----make_query_0--in_or_ex: '+in_or_ex+'--')
            var result = {
                person: [],
                drug: [],
                condition: [],
                measurement: []
            }
            var table = 0, field = 0;
            var c_id = 0;
            
            // 카테고리화 문제 때문에 
            // table 값이 6인경우 mtdb에서 해당 id값을 가져와야함 
            // criteria_table 에 텍스트 값을 비교하여 6: 진단, 5: 약을 구분 
            // criteria_detail_attribute , {1,2,3}, {대, 중, 소}
            // SELECT concept_id FROM mtdb.dbo.icd10_snomed WHERE SMALL = "%Gout%" 
            
            var select_table = ['atc_rxnorm_all', 'icd10_snomed']; // -5
            var select_lms = ['LARGE','MID','SMALL']; // -1 

            

            for (i = 0; i < rows.length; i++) {
                table = rows[i].criteria_detail_table;
                field = rows[i].criteria_detail_attribute;
                con = rows[i].criteria_detail_condition;
                val = rows[i].criteria_detail_value;
                title = rows[i].criteria_title;
                
                c_id = rows[i].criteria_id;
                //console.log("SELECT person_id FROM "+ cdm[table].tableName + " WHERE "+cdm[table].fieldSet[field]+" "+condition[con]+ " " + val );
                //condition확인
                //console.log(table);
                switch (Number(table)) {
                    //person
                    case 0:
                        result.person.push("( SELECT person_id, provider_id FROM person WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                        break;
                    //drug
                    case 1:
                        result.drug.push("( SELECT person_id, provider_id FROM drug_exposure WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                        break;
                    //condition
                    case 2:
                        result.condition.push("( SELECT person_id, provider_id FROM condition_occurrence WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                        break;
                    //measurement
                    case 3:
                        result.measurement.push("( SELECT person_id, provider_id FROM measurement WHERE person_id IN (  ( SELECT person_id FROM ( SELECT A.person_id , "+ rows[i].measurement_method+"(A.value_as_number) as value_as_number FROM ( SELECT ROW_NUMBER() OVER(partition by person_id ORDER BY measurement_date DESC ) AS num, person_id, measurement_date, value_as_number FROM dbo.MEASUREMENT WHERE measurement_concept_id = "+ rows[i].criteria_detail_value + " "+ measurement_date(rows[i].date_addition_value)+ " ) A WHERE num < "+ rows[i].measurement_count + " GROUP BY A.person_id ) B "+measurement_min_max(rows[i].value_addition_min, rows[i].value_addition_max) +")))" );
                        break;
                    //mtdb 약
                    case 5:
                        // SELECT concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE SMALL = "%Gout%" 
                        result.drug.push(`( SELECT person_id, provider_id FROM drug_exposure WHERE drug_concept_id IN (SELECT concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE ${select_lms[field-1]} = '%${title}%')`);
                        break;

                    //mtdb 병력
                    case 6:
                        // SELECT concept_id FROM mtdb.dbo.icd10_snomed WHERE SMALL = "%Gout%" 
                        result.condition.push(`( SELECT person_id, provider_id FROM drug_exposure WHERE drug_concept_id IN (SELECT concept_id FROM mtdb.dbo.icd10_snomed WHERE ${select_lms[field-1]} = '%${title}%')`);
                        break;
                }// switch
     
                function measurement_date(value_date){
                    if(value_date==null){
                        return '';
                    }
                    var split_date = value_date.split(" - ");
                    var start_date = split_date[0];
                    var end_date = split_date[1];
                    var start_split = start_date.split("/");
                    var end_split = end_date.split("/");
                    var start_data = start_split[2]+"-"+start_split[1]+"-"+start_split[0];
                    var end_data = end_split[2]+"-"+end_split[1]+"-"+end_split[0];
                    
                    date_start = "and measurement_date >= '"+start_data+"' ";
                    date_end = "and measurement_date <= '"+end_data+"' ";

                    return date_start + date_end
                }// measurement_date 

                function measurement_min_max(min,max,min_con, max_con){
                   var out ='';
                    if(min!=null){
                        out += "B.value_as_number "+ measurement_condition(min_con) + " " +min ;
                    }
                    if(max!=null){
                        if(out.length!=0){
                            out += " and ";
                        }
                        out += "B.value_as_number " + measurement_condition(max_con) + " "+ max;
                    }
                    return out;
                }// measurement_min_max

                function measurement_condition(con){
                    switch(con){
                        case 'more than or equal':
                        return '>=';
                        
                        case 'more then':
                        return '>';
                        
                        case 'equal':
                        return '=';

                        case 'less than or equal':
                        return '<=';
                        
                        case 'less than':
                        return '<';
                    }//switch
                }// measurement_condition
            }
            //최종 쿼리 f_q
            //person
            var tables = ["person", "drug", "condition", "measurement"];
            var f_q = {
                person: "",
                drug: "",
                condition: "",
                measurement: ""
            }
            var where = "";
            var final = "";
            if (in_or_ex == 1) {
                for (i = 0; i < tables.length; i++) {
                    if (result[tables[i]].length != 0) {
                        where = ""
                        for (j = 0; j < result[tables[i]].length; j++) {
                            f_q[tables[i]] += result[tables[i]][j] + " a" + j;
                            if (j + 1 != result[tables[i]].length) {
                                f_q[tables[i]] += ", ";
                            } else {
                                f_q[tables[i]] += " ";
                            }
                        }
                        //where문장
                        for (j = 0; j < result[tables[i]].length - 1; j++) {
                            if (j + 1 != result[tables[i]].length - 1) {
                                where += "a" + j + ".person_id = a" + (j + 1) + ".person_id and ";
                            } else {
                                where += "a" + j + ".person_id = a" + (j + 1) + ".person_id ";
                            }
                        }
    
                        //쿼리문 작성
                        if (result[tables[i]].length == 1) {
                            f_q[tables[i]] = "SELECT DISTINCT a0.person_id, a0.provider_id FROM " + f_q[tables[i]];
                        } else {
                            f_q[tables[i]] = "SELECT DISTINCT a0.person_id, a0.provider_id FROM " + f_q[tables[i]] + " WHERE " + where;
                        }
                    }
    
                }
    
                //최종 추출 idx값 구하기.
                // var tables = ["person","drug","condition","measurement"];
                var idx = 0;
                if (result[tables[2]].length != 0) {
                    idx = 2;
                } else if (result[tables[3]].length != 0) {
                    idx = 3
                } else if (result[tables[1]].length != 0) {
                    idx = 1;
                }
                // condition 내용 유무                console.log("condition유무: " + idx);
    
                //1 = inclusion, 0=exclusion
    
                var buf_tables = [];
                for (i = 0; i < tables.length; i++) {
                    if (f_q[tables[i]].length != 0) {
                        buf_tables.push(tables[i]);
                    }
                }
                // console.log("Inserted tables:");                console.log(buf_tables);                console.log();
    
                idx = buf_tables.indexOf(tables[idx]);
                final += "SELECT b" + idx + ".provider_id, b" + idx + ".person_id, p1.year_of_birth, p1.gender_concept_id FROM "
    
                for (i = 0; i < buf_tables.length; i++) {
                    if (buf_tables.length == 1) {
                        final += "( " + f_q[buf_tables[i]] + " ) b" + i;
                    } else {
                        if (f_q[buf_tables[i]].length != 0) {
    
                            final += "( " + f_q[buf_tables[i]] + " ) b" + i;
                            if (i + 1 != buf_tables.length) {
                                final += ", ";
                            } else {
                                final += " ";
                            }
                           // console.log("--final query--" + buf_tables[i]);                             console.log(final);                            console.log();
                        }
                    }
    
                }
                //final += " WHERE b0.person_id = b1.person_id and b1.person_id = b2.person_id and b2.person_id = b3.person_id and b3.person_id = b1.person_id";
    
                //각 항목에 내용 여부 체크
                var f_where = [];
                for (i = 0; i < 4; i++) {
                    if (f_q[tables[i]].length != 0) {
                        f_where.push(buf_tables.indexOf(tables[i]));
                    }
                }
    
                var where_f = ", person p1 WHERE "
    
                for (i = 0; i < f_where.length - 1; i++) {
                    if (i + 1 != f_where.length - 1) {
                        where_f += "b" + f_where[i] + ".person_id = b" + f_where[i + 1] + ".person_id and ";
                    } else {
                        where_f += "b" + f_where[i] + ".person_id = b" + f_where[i + 1] + ".person_id ";
                    }
                }
               // console.log("f_where.length: " + f_where.length);                 console.log();    
                if(f_where.length !=1){
                    final += where_f + "and b" + f_where[0] + ".person_id = b" + f_where[f_where.length - 1] + ".person_id and b" + idx + ".person_id = p1.person_id";
                }else{
                    final += where_f + " b" + f_where[0] + ".person_id = b" + f_where[f_where.length - 1] + ".person_id and b" + idx + ".person_id = p1.person_id";
                }
                
               // console.log("last final");                 console.log(final);                console.log();
    
            } else {
                // exclusion
    
                for (i = 0; i < tables.length; i++) {
                    //없는 각 테이블 버리기
                    if (result[tables[i]].length != 0) {
                        for (j = 0; j < result[tables[i]].length; j++) {
                            // console.log(i+", "+j)
                            final += result[tables[i]][j] + " UNION "
                        }
                    }
                }
                if (final.length != 0) {
                    final = "SELECT c0.person_id FROM ( " + final.substring(0, final.length - 6) + " ) c0";
                } else {
                    final = "";
                }
    
            }
            //result, f_q, final
           // util_console_print('----make_query_0--end');
            return final;
        }
        function make_query_1(inclusion, exclusion){
            util_console_print('-----make_query_1-----');
            var query;
            if (inclusion.length == 0 || exclusion == 0) {
                query = "SELECT provider_id, A.person_id, A.year_of_birth, A.gender_concept_id FROM ( " + inclusion + " ) A  "
            } else {
                query = "SELECT provider_id, A.person_id, A.year_of_birth, A.gender_concept_id FROM ( " + inclusion + " ) A LEFT JOIN ( " + exclusion + " ) B ON A.person_id = B.person_id WHERE B.person_id IS NULL"
            }
            return query;
        }
    }//async
});///test-site-execute/:study_id/:query_id


//////////Protocol

app.get('/test-protocol-execute/:protocol_id/:user_id', (req,res)=>{
    util_console_print("----test-protocol-execute/:protocol_id/:user_id----");
    var protocol_id = Number(req.param('protocol_id'));
    var query ='';
    
    //let result ;
    asyncMain(protocol_id);
    
    ////Async 함수
    async function asyncMain(protocol_id) {
        try{
            let pool =  await sql.connect(dbConfig); //scrn_cloud
            // 1.프로토콜 실행 버튼 클릭 시 '진행' 상태 변경하기.
            console.log('1.프로토콜 상태 업데이트')
           let result = await pool.request() 
            .input('input_paramiter', sql.Int, protocol_id)
            .query(`UPDATE protocol SET protocol_status = N'InProgress' WHERE protocol_id = @input_paramiter`)
            //console.log(`UPDATE protocol SET protocol_status = N'InProgress' WHERE protocol_id = @input_paramiter`);
            util_console_print({result_update_state: result});
           
            // 2. 조건 쿼리 받아와서 쿼리문 만들기.
            console.log('2. 조건쿼리 받아서 쿼리문 만들기');
            result = await pool.request()
            .input('input_paramiter', sql.Int, protocol_id)
            .query(`SELECT * FROM protocol_criteria_detail WHERE protocol_id = @input_paramiter`)
            util_console_print({result_2_query:result});

            var str_or_weak  = asyncMain_2_0_assort_strength(result);  //console.log({result_str_or_weak: str_or_weak}); // 강한조건, 유연조건 나누기. 
            var query_list = asyncMain_2_1_make_query(str_or_weak);
            
            // 3. 만들어진 쿼리문 리스트 각 하나씩 돌려서 결과값 받기. 결과: (의사, 환자, 년도, 성별)
            console.log("----3번시작")
            sql.close();
            pool =  await sql.connect(CdmDbConfig); //scrn_cloud
            var query_list_result =[];

            for(i=0;i<query_list.length;i++){
                result = await pool.request()
                .query(query_list[0]);
                query_list_result.push(result.recordset.length);
            }

            // 4. 만들어진 쿼리 리스트 count(*)값 추출
            console.log("----4번시작");
            var list_count = [] ; 
            var buf_query = '';
            for(i=0;i<query_list.length;i++){
                //console.log(query_list[i]);
                buf_query = `SELECT DISTINCT sec.person_id FROM (${query_list[i]}) sec `
                result = await pool.request()
                .query(buf_query);
                list_count.push(result.recordset.length);
            }

            // 5. 마지막 쿼리 조건 조회해서 넣기.
            console.log("----5번시작");
            var result_5 ;
            result = await pool.request()
            .query(query_list[query_list.length-1]);
            result_5 = result;

            

        res.send({resunt:{list_count:list_count, result_5:result_5}});
        sql.close();    
        }catch(err){
            util_console_print({error: err});
            res.send({error:'2.result is 0'});

            sql.close();
        }
       

        function asyncMain_2_0_assort_strength(result){
            var out = {
                strength : [], 
                weak : []
            }
            if(result.rowsAffected[0]==0){
                throw new Error(result);
            }
            rows = result.recordset;
            for(i=0;i<rows.length;i++){
                if(rows[i].strength==1){
                    out.strength.push(rows[i]);
                }else{
                    out.weak.push(rows[i]);
                }
            }
            return out;

        }
        function asyncMain_2_1_make_query(rows_str_weak){
            util_console_print('------asyncMain_2_1_make_query------'); // console.log(rows_str_weak);
            // 강한 조건에서 다시 조건 하나를 넣어준다. 
            // 조건이 넣어지면 다시 선정 제외조건을 나눠서 쿼리문을 만들어준다. 
            // 강한 조건이 끝나면 다시 약한 조건으로 하나씩 쿼리를 만든다. 
            let in_rows=[], ex_rows=[];
            let query_list =[]; 
            let inclusion='' , exclusion='';
           
            //강한조건부터
            let rows = rows_str_weak.strength;          //console.log({rows_count:rows.length});
            for(k=0;k<rows.length;k++){
                //console.log({k:k});
                if(rows[k].criteria_state == 1){
                    in_rows.push(rows[k]);
                }else{
                    ex_rows.push(rows[k]);
                }
                //console.log({in_rows:in_rows.length, ex_rows:ex_rows.length});
                inclusion = make_query_0(in_rows,1); // 선정 1, 제외 0 
                exclusion = make_query_0(ex_rows,0);              //console.log({for: {in:in_rows.length, ex:ex_rows.length}});
                query_list.push(make_query_1(inclusion, exclusion));
            }
            
            //약한조건을 한 개씩 추가해보기
            rows = rows_str_weak.weak;
            //console.log(rows);
            let buf_in_rows =[], buf_ex_rows = [];
            
            for(k=0;k<rows.length;k++){
                //console.log(rows[k]);
                buf_in_rows = JSON.parse(JSON.stringify(in_rows));
                buf_ex_rows = JSON.parse(JSON.stringify(ex_rows));
                if(rows[k].criteria_state ==1){
                    buf_in_rows.push(rows[k]);
                }else{
                    buf_ex_rows.push(rows[k]);
                }
                inclusion = make_query_0(buf_in_rows,1); // 선정 1, 제외 0 
                exclusion = make_query_0(buf_ex_rows,0);              //console.log({for: {in:in_rows.length, ex:ex_rows.length}});
                query_list.push(make_query_1(inclusion, exclusion));
            }

            return query_list;
            
        }
        function make_query_0(rows, in_or_ex) {
            //util_console_print('----make_query_0--in_or_ex: '+in_or_ex+'--')
            var result = {
                person: [],
                drug: [],
                condition: [],
                measurement: []
            }
            var table = 0, field = 0;
            var c_id = 0;
            for (i = 0; i < rows.length; i++) {
                table = rows[i].criteria_detail_table;
                field = rows[i].criteria_detail_attribute;
                con = rows[i].criteria_detail_condition;
                val = rows[i].criteria_detail_value;
                
                c_id = rows[i].criteria_id;
                //console.log("SELECT person_id FROM "+ cdm[table].tableName + " WHERE "+cdm[table].fieldSet[field]+" "+condition[con]+ " " + val );
                //condition확인
                //console.log(table);
                switch (Number(table)) {
                    //person
                    case 0:
                        result.person.push("( SELECT person_id, provider_id FROM person WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                        break;
                    //drug
                    case 1:
                        result.drug.push("( SELECT person_id, provider_id FROM drug_exposure WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                        break;
                    //condition
                    case 2:
                        result.condition.push("( SELECT person_id, provider_id FROM condition_occurrence WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                        break;
                    //measurement
                    case 3:
                        result.measurement.push("( SELECT person_id, provider_id FROM measurement WHERE person_id IN (  ( SELECT person_id FROM ( SELECT A.person_id , "+ rows[i].measurement_method+"(A.value_as_number) as value_as_number FROM ( SELECT ROW_NUMBER() OVER(partition by person_id ORDER BY measurement_date DESC ) AS num, person_id, measurement_date, value_as_number FROM dbo.MEASUREMENT WHERE measurement_concept_id = "+ rows[i].criteria_detail_value + " "+ measurement_date(rows[i].date_addition_value)+ " ) A WHERE num < "+ rows[i].measurement_count + " GROUP BY A.person_id ) B "+measurement_min_max(rows[i].value_addition_min, rows[i].value_addition_max) +")))" );
                        break;
                }// switch
     
                function measurement_date(value_date){
                    if(value_date==null){
                        return '';
                    }
                    var split_date = value_date.split(" - ");
                    var start_date = split_date[0];
                    var end_date = split_date[1];
                    var start_split = start_date.split("/");
                    var end_split = end_date.split("/");
                    var start_data = start_split[2]+"-"+start_split[1]+"-"+start_split[0];
                    var end_data = end_split[2]+"-"+end_split[1]+"-"+end_split[0];
                    
                    date_start = "and measurement_date >= '"+start_data+"' ";
                    date_end = "and measurement_date <= '"+end_data+"' ";

                    return date_start + date_end
                }// measurement_date 

                function measurement_min_max(min,max,min_con, max_con){
                   var out ='';
                    if(min!=null){
                        out += "B.value_as_number "+ measurement_condition(min_con) + " " +min ;
                    }
                    if(max!=null){
                        if(out.length!=0){
                            out += " and ";
                        }
                        out += "B.value_as_number " + measurement_condition(max_con) + " "+ max;
                    }
                    return out;
                }// measurement_min_max

                function measurement_condition(con){
                    switch(con){
                        case 'more than or equal':
                        return '>=';
                        
                        case 'more then':
                        return '>';
                        
                        case 'equal':
                        return '=';

                        case 'less than or equal':
                        return '<=';
                        
                        case 'less than':
                        return '<';
                    }//switch
                }// measurement_condition
            }
            //최종 쿼리 f_q
            //person
            var tables = ["person", "drug", "condition", "measurement"];
            var f_q = {
                person: "",
                drug: "",
                condition: "",
                measurement: ""
            }
            var where = "";
            var final = "";
            if (in_or_ex == 1) {
                for (i = 0; i < tables.length; i++) {
                    if (result[tables[i]].length != 0) {
                        where = ""
                        for (j = 0; j < result[tables[i]].length; j++) {
                            f_q[tables[i]] += result[tables[i]][j] + " a" + j;
                            if (j + 1 != result[tables[i]].length) {
                                f_q[tables[i]] += ", ";
                            } else {
                                f_q[tables[i]] += " ";
                            }
                        }
                        //where문장
                        for (j = 0; j < result[tables[i]].length - 1; j++) {
                            if (j + 1 != result[tables[i]].length - 1) {
                                where += "a" + j + ".person_id = a" + (j + 1) + ".person_id and ";
                            } else {
                                where += "a" + j + ".person_id = a" + (j + 1) + ".person_id ";
                            }
                        }
    
                        //쿼리문 작성
                        if (result[tables[i]].length == 1) {
                            f_q[tables[i]] = "SELECT DISTINCT a0.person_id, a0.provider_id FROM " + f_q[tables[i]];
                        } else {
                            f_q[tables[i]] = "SELECT DISTINCT a0.person_id, a0.provider_id FROM " + f_q[tables[i]] + " WHERE " + where;
                        }
                    }
    
                }
    
                //최종 추출 idx값 구하기.
                // var tables = ["person","drug","condition","measurement"];
                var idx = 0;
                if (result[tables[2]].length != 0) {
                    idx = 2;
                } else if (result[tables[3]].length != 0) {
                    idx = 3
                } else if (result[tables[1]].length != 0) {
                    idx = 1;
                }
                // condition 내용 유무                console.log("condition유무: " + idx);
    
                //1 = inclusion, 0=exclusion
    
                var buf_tables = [];
                for (i = 0; i < tables.length; i++) {
                    if (f_q[tables[i]].length != 0) {
                        buf_tables.push(tables[i]);
                    }
                }
                // console.log("Inserted tables:");                console.log(buf_tables);                console.log();
    
                idx = buf_tables.indexOf(tables[idx]);
                final += "SELECT b" + idx + ".provider_id, b" + idx + ".person_id, p1.year_of_birth, p1.gender_concept_id FROM "
    
                for (i = 0; i < buf_tables.length; i++) {
                    if (buf_tables.length == 1) {
                        final += "( " + f_q[buf_tables[i]] + " ) b" + i;
                    } else {
                        if (f_q[buf_tables[i]].length != 0) {
    
                            final += "( " + f_q[buf_tables[i]] + " ) b" + i;
                            if (i + 1 != buf_tables.length) {
                                final += ", ";
                            } else {
                                final += " ";
                            }
                           // console.log("--final query--" + buf_tables[i]);                             console.log(final);                            console.log();
                        }
                    }
    
                }
                //final += " WHERE b0.person_id = b1.person_id and b1.person_id = b2.person_id and b2.person_id = b3.person_id and b3.person_id = b1.person_id";
    
                //각 항목에 내용 여부 체크
                var f_where = [];
                for (i = 0; i < 4; i++) {
                    if (f_q[tables[i]].length != 0) {
                        f_where.push(buf_tables.indexOf(tables[i]));
                    }
                }
    
                var where_f = ", person p1 WHERE "
    
                for (i = 0; i < f_where.length - 1; i++) {
                    if (i + 1 != f_where.length - 1) {
                        where_f += "b" + f_where[i] + ".person_id = b" + f_where[i + 1] + ".person_id and ";
                    } else {
                        where_f += "b" + f_where[i] + ".person_id = b" + f_where[i + 1] + ".person_id ";
                    }
                }
               // console.log("f_where.length: " + f_where.length);                 console.log();    
                if(f_where.length !=1){
                    final += where_f + "and b" + f_where[0] + ".person_id = b" + f_where[f_where.length - 1] + ".person_id and b" + idx + ".person_id = p1.person_id";
                }else{
                    final += where_f + " b" + f_where[0] + ".person_id = b" + f_where[f_where.length - 1] + ".person_id and b" + idx + ".person_id = p1.person_id";
                }
                
               // console.log("last final");                 console.log(final);                console.log();
    
            } else {
                // exclusion
    
                for (i = 0; i < tables.length; i++) {
                    //없는 각 테이블 버리기
                    if (result[tables[i]].length != 0) {
                        for (j = 0; j < result[tables[i]].length; j++) {
                            // console.log(i+", "+j)
                            final += result[tables[i]][j] + " UNION "
                        }
                    }
                }
                if (final.length != 0) {
                    final = "SELECT c0.person_id FROM ( " + final.substring(0, final.length - 6) + " ) c0";
                } else {
                    final = "";
                }
    
            }
            //result, f_q, final
           // util_console_print('----make_query_0--end');
            return final;
        }
        function make_query_1(inclusion, exclusion){
            util_console_print('-----make_query_1-----');
            var query;
            if (inclusion.length == 0 || exclusion == 0) {
                query = "SELECT provider_id, A.person_id, A.year_of_birth, A.gender_concept_id FROM ( " + inclusion + " ) A  "
            } else {
                query = "SELECT provider_id, A.person_id, A.year_of_birth, A.gender_concept_id FROM ( " + inclusion + " ) A LEFT JOIN ( " + exclusion + " ) B ON A.person_id = B.person_id WHERE B.person_id IS NULL"
            }
            return query;
        }
        
    }//async
});//test-site-execute/:study_id/:query_id

//위즈모 이용하기.
app.get('/test-protocol-wijmo/:protocol_id',(req,res)=>{
    util_console_print('/test-protocol-wijmo/:protocol_id')
    //protocol_result에서 해당 프로토콜 아이디로 환자리스트를 추출
    // 환자리스트에 생년, 약내용, 검사내용, 진단내용, 방문내용 뽑기. 
    var protocol_id = Number(req.param('protocol_id'));

    asyncMain(protocol_id);
    
    ////Async 함수
    async function asyncMain(protocol_id) {
        try{
            let pool =  await sql.connect(dbConfig); //scrn_cloud
            // 1.프로토콜 실행 버튼 클릭 시 '진행' 상태 변경하기.
            console.log('1.프로토콜 상태 업데이트')
           let result = await pool.request() 
            .input('input_paramiter', sql.Int, protocol_id)
            .query(`UPDATE protocol SET protocol_status = N'InProgress' WHERE protocol_id = @input_paramiter`)
            //console.log(`UPDATE protocol SET protocol_status = N'InProgress' WHERE protocol_id = @input_paramiter`);
            util_console_print({result_update_state: result});
           
            // 2. 조건 쿼리 받아와서 쿼리문 만들기.
            console.log('2. 조건쿼리 받아서 쿼리문 만들기');
            result = await pool.request()
            .input('input_paramiter', sql.Int, protocol_id)
            .query(`SELECT * FROM protocol_criteria_detail WHERE protocol_id = @input_paramiter`)
            util_console_print({result_2_query:result});

            var str_or_weak  = asyncMain_2_0_assort_strength(result);  //console.log({result_str_or_weak: str_or_weak}); // 강한조건, 유연조건 나누기. 
            var query_list = asyncMain_2_1_make_query(str_or_weak);
            
            // 3. 만들어진 쿼리문 리스트 각 하나씩 돌려서 결과값 받기. 결과: (의사, 환자, 년도, 성별)
            console.log("----3번시작")
            sql.close();
            pool =  await sql.connect(CdmDbConfig); //scrn_cloud
            var query_list_result =[];

            for(i=0;i<query_list.length;i++){
                result = await pool.request()
                .query(query_list[0]);
                query_list_result.push(result.recordset.length);
            }

            // 4. 만들어진 쿼리 리스트 count(*)값 추출
            console.log("----4번시작");
            var list_count = [] ; 
            var buf_query = '';
            for(i=0;i<query_list.length;i++){
                //console.log(query_list[i]);
                buf_query = `SELECT DISTINCT sec.person_id FROM (${query_list[i]}) sec `
                result = await pool.request()
                .query(buf_query);
                list_count.push(result.recordset.length);
            }

            // 5. 마지막 쿼리 조건 조회해서 넣기.
            console.log("----5번시작");
            var result_5 ;
            result = await pool.request()
            .query(query_list[query_list.length-1]);
            result_5 = result;

            

        res.send({resunt:{list_count:list_count, result_5:result_5}});
        sql.close();    
        }catch(err){
            util_console_print({error: err});
            res.send({error:'2.result is 0'});

            sql.close();
        }
       

        function asyncMain_2_0_assort_strength(result){
            var out = {
                strength : [], 
                weak : []
            }
            if(result.rowsAffected[0]==0){
                throw new Error(result);
            }
            rows = result.recordset;
            for(i=0;i<rows.length;i++){
                if(rows[i].strength==1){
                    out.strength.push(rows[i]);
                }else{
                    out.weak.push(rows[i]);
                }
            }
            return out;

        }
        function asyncMain_2_1_make_query(rows_str_weak){
            util_console_print('------asyncMain_2_1_make_query------'); // console.log(rows_str_weak);
            // 강한 조건에서 다시 조건 하나를 넣어준다. 
            // 조건이 넣어지면 다시 선정 제외조건을 나눠서 쿼리문을 만들어준다. 
            // 강한 조건이 끝나면 다시 약한 조건으로 하나씩 쿼리를 만든다. 
            let in_rows=[], ex_rows=[];
            let query_list =[]; 
            let inclusion='' , exclusion='';
           
            //강한조건부터
            let rows = rows_str_weak.strength;          //console.log({rows_count:rows.length});
            for(k=0;k<rows.length;k++){
                //console.log({k:k});
                if(rows[k].criteria_state == 1){
                    in_rows.push(rows[k]);
                }else{
                    ex_rows.push(rows[k]);
                }
                //console.log({in_rows:in_rows.length, ex_rows:ex_rows.length});
                inclusion = make_query_0(in_rows,1); // 선정 1, 제외 0 
                exclusion = make_query_0(ex_rows,0);              //console.log({for: {in:in_rows.length, ex:ex_rows.length}});
                query_list.push(make_query_1(inclusion, exclusion));
            }
            
            //약한조건을 한 개씩 추가해보기
            rows = rows_str_weak.weak;
            //console.log(rows);
            let buf_in_rows =[], buf_ex_rows = [];
            
            for(k=0;k<rows.length;k++){
                //console.log(rows[k]);
                buf_in_rows = JSON.parse(JSON.stringify(in_rows));
                buf_ex_rows = JSON.parse(JSON.stringify(ex_rows));
                if(rows[k].criteria_state ==1){
                    buf_in_rows.push(rows[k]);
                }else{
                    buf_ex_rows.push(rows[k]);
                }
                inclusion = make_query_0(buf_in_rows,1); // 선정 1, 제외 0 
                exclusion = make_query_0(buf_ex_rows,0);              //console.log({for: {in:in_rows.length, ex:ex_rows.length}});
                query_list.push(make_query_1(inclusion, exclusion));
            }

            return query_list;
            
        }
        function make_query_0(rows, in_or_ex) {
            //util_console_print('----make_query_0--in_or_ex: '+in_or_ex+'--')
            var result = {
                person: [],
                drug: [],
                condition: [],
                measurement: []
            }
            var table = 0, field = 0;
            var c_id = 0;
            for (i = 0; i < rows.length; i++) {
                table = rows[i].criteria_detail_table;
                field = rows[i].criteria_detail_attribute;
                con = rows[i].criteria_detail_condition;
                val = rows[i].criteria_detail_value;
                
                c_id = rows[i].criteria_id;
                //console.log("SELECT person_id FROM "+ cdm[table].tableName + " WHERE "+cdm[table].fieldSet[field]+" "+condition[con]+ " " + val );
                //condition확인
                //console.log(table);
                switch (Number(table)) {
                    //person
                    case 0:
                        result.person.push("( SELECT person_id, provider_id FROM person WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                        break;
                    //drug
                    case 1:
                        result.drug.push("( SELECT person_id, provider_id FROM drug_exposure WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                        break;
                    //condition
                    case 2:
                        result.condition.push("( SELECT person_id, provider_id FROM condition_occurrence WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                        break;
                    //measurement
                    case 3:
                        result.measurement.push("( SELECT person_id, provider_id FROM measurement WHERE person_id IN (  ( SELECT person_id FROM ( SELECT A.person_id , "+ rows[i].measurement_method+"(A.value_as_number) as value_as_number FROM ( SELECT ROW_NUMBER() OVER(partition by person_id ORDER BY measurement_date DESC ) AS num, person_id, measurement_date, value_as_number FROM dbo.MEASUREMENT WHERE measurement_concept_id = "+ rows[i].criteria_detail_value + " "+ measurement_date(rows[i].date_addition_value)+ " ) A WHERE num < "+ rows[i].measurement_count + " GROUP BY A.person_id ) B "+measurement_min_max(rows[i].value_addition_min, rows[i].value_addition_max) +")))" );
                        break;
                }// switch
     
                function measurement_date(value_date){
                    if(value_date==null){
                        return '';
                    }
                    var split_date = value_date.split(" - ");
                    var start_date = split_date[0];
                    var end_date = split_date[1];
                    var start_split = start_date.split("/");
                    var end_split = end_date.split("/");
                    var start_data = start_split[2]+"-"+start_split[1]+"-"+start_split[0];
                    var end_data = end_split[2]+"-"+end_split[1]+"-"+end_split[0];
                    
                    date_start = "and measurement_date >= '"+start_data+"' ";
                    date_end = "and measurement_date <= '"+end_data+"' ";

                    return date_start + date_end
                }// measurement_date 

                function measurement_min_max(min,max,min_con, max_con){
                   var out ='';
                    if(min!=null){
                        out += "B.value_as_number "+ measurement_condition(min_con) + " " +min ;
                    }
                    if(max!=null){
                        if(out.length!=0){
                            out += " and ";
                        }
                        out += "B.value_as_number " + measurement_condition(max_con) + " "+ max;
                    }
                    return out;
                }// measurement_min_max

                function measurement_condition(con){
                    switch(con){
                        case 'more than or equal':
                        return '>=';
                        
                        case 'more then':
                        return '>';
                        
                        case 'equal':
                        return '=';

                        case 'less than or equal':
                        return '<=';
                        
                        case 'less than':
                        return '<';
                    }//switch
                }// measurement_condition
            }
            //최종 쿼리 f_q
            //person
            var tables = ["person", "drug", "condition", "measurement"];
            var f_q = {
                person: "",
                drug: "",
                condition: "",
                measurement: ""
            }
            var where = "";
            var final = "";
            if (in_or_ex == 1) {
                for (i = 0; i < tables.length; i++) {
                    if (result[tables[i]].length != 0) {
                        where = ""
                        for (j = 0; j < result[tables[i]].length; j++) {
                            f_q[tables[i]] += result[tables[i]][j] + " a" + j;
                            if (j + 1 != result[tables[i]].length) {
                                f_q[tables[i]] += ", ";
                            } else {
                                f_q[tables[i]] += " ";
                            }
                        }
                        //where문장
                        for (j = 0; j < result[tables[i]].length - 1; j++) {
                            if (j + 1 != result[tables[i]].length - 1) {
                                where += "a" + j + ".person_id = a" + (j + 1) + ".person_id and ";
                            } else {
                                where += "a" + j + ".person_id = a" + (j + 1) + ".person_id ";
                            }
                        }
    
                        //쿼리문 작성
                        if (result[tables[i]].length == 1) {
                            f_q[tables[i]] = "SELECT DISTINCT a0.person_id, a0.provider_id FROM " + f_q[tables[i]];
                        } else {
                            f_q[tables[i]] = "SELECT DISTINCT a0.person_id, a0.provider_id FROM " + f_q[tables[i]] + " WHERE " + where;
                        }
                    }
    
                }
    
                //최종 추출 idx값 구하기.
                // var tables = ["person","drug","condition","measurement"];
                var idx = 0;
                if (result[tables[2]].length != 0) {
                    idx = 2;
                } else if (result[tables[3]].length != 0) {
                    idx = 3
                } else if (result[tables[1]].length != 0) {
                    idx = 1;
                }
                // condition 내용 유무                console.log("condition유무: " + idx);
    
                //1 = inclusion, 0=exclusion
    
                var buf_tables = [];
                for (i = 0; i < tables.length; i++) {
                    if (f_q[tables[i]].length != 0) {
                        buf_tables.push(tables[i]);
                    }
                }
                // console.log("Inserted tables:");                console.log(buf_tables);                console.log();
    
                idx = buf_tables.indexOf(tables[idx]);
                final += "SELECT b" + idx + ".provider_id, b" + idx + ".person_id, p1.year_of_birth, p1.gender_concept_id FROM "
    
                for (i = 0; i < buf_tables.length; i++) {
                    if (buf_tables.length == 1) {
                        final += "( " + f_q[buf_tables[i]] + " ) b" + i;
                    } else {
                        if (f_q[buf_tables[i]].length != 0) {
    
                            final += "( " + f_q[buf_tables[i]] + " ) b" + i;
                            if (i + 1 != buf_tables.length) {
                                final += ", ";
                            } else {
                                final += " ";
                            }
                           // console.log("--final query--" + buf_tables[i]);                             console.log(final);                            console.log();
                        }
                    }
    
                }
                //final += " WHERE b0.person_id = b1.person_id and b1.person_id = b2.person_id and b2.person_id = b3.person_id and b3.person_id = b1.person_id";
    
                //각 항목에 내용 여부 체크
                var f_where = [];
                for (i = 0; i < 4; i++) {
                    if (f_q[tables[i]].length != 0) {
                        f_where.push(buf_tables.indexOf(tables[i]));
                    }
                }
    
                var where_f = ", person p1 WHERE "
    
                for (i = 0; i < f_where.length - 1; i++) {
                    if (i + 1 != f_where.length - 1) {
                        where_f += "b" + f_where[i] + ".person_id = b" + f_where[i + 1] + ".person_id and ";
                    } else {
                        where_f += "b" + f_where[i] + ".person_id = b" + f_where[i + 1] + ".person_id ";
                    }
                }
               // console.log("f_where.length: " + f_where.length);                 console.log();    
                if(f_where.length !=1){
                    final += where_f + "and b" + f_where[0] + ".person_id = b" + f_where[f_where.length - 1] + ".person_id and b" + idx + ".person_id = p1.person_id";
                }else{
                    final += where_f + " b" + f_where[0] + ".person_id = b" + f_where[f_where.length - 1] + ".person_id and b" + idx + ".person_id = p1.person_id";
                }
                
               // console.log("last final");                 console.log(final);                console.log();
    
            } else {
                // exclusion
    
                for (i = 0; i < tables.length; i++) {
                    //없는 각 테이블 버리기
                    if (result[tables[i]].length != 0) {
                        for (j = 0; j < result[tables[i]].length; j++) {
                            // console.log(i+", "+j)
                            final += result[tables[i]][j] + " UNION "
                        }
                    }
                }
                if (final.length != 0) {
                    final = "SELECT c0.person_id FROM ( " + final.substring(0, final.length - 6) + " ) c0";
                } else {
                    final = "";
                }
    
            }
            //result, f_q, final
           // util_console_print('----make_query_0--end');
            return final;
        }
        function make_query_1(inclusion, exclusion){
            util_console_print('-----make_query_1-----');
            var query;
            if (inclusion.length == 0 || exclusion == 0) {
                query = "SELECT provider_id, A.person_id, A.year_of_birth, A.gender_concept_id FROM ( " + inclusion + " ) A  "
            } else {
                query = "SELECT provider_id, A.person_id, A.year_of_birth, A.gender_concept_id FROM ( " + inclusion + " ) A LEFT JOIN ( " + exclusion + " ) B ON A.person_id = B.person_id WHERE B.person_id IS NULL"
            }
            return query;
        }
        
    }//async
});//'/test-protocol-wijmo/:protocol_id'


/////////////////////////////////////////////////////////////////////////////////////////// 서버 실행 부분
app.use((error,req,res,next)=>{
    res.json({message: error.message});
})


// 1092374098123498378241234124124324123412341242142134
// 웹 서버를 실행
http.createServer(app).listen(52273, function(req, res){
    console.log('Server Running at http://127.0.0.1:52273');
    console.log();
});
// 1092374098123498378241234124124324123412341242142134
///////////////////////////////////////////////////////////////////////////////////////////





/// 유용한 함수
function util_console_print(mes){
    console.log(mes);
    console.log();
}