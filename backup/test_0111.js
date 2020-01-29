const {Lab, Atc_code,Icd10_code,
    class_drug, 
    class_measurement, 
    class_condition, class_visit,class_person,class_distribution_result,class_protocol_condition,class_protocol_measurement,class_distribution_result_measurement } = require('./class.js');
const {dbConfig, CdmDbConfig} = require('./source/server_config.js');

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



//웹 서버를 생성
var app = express();
app.use(cors());
app.use(express.static('public'));
app.use(bodyParser.json());



////////////////CDM 기본 설정
// var cdm={"table":
//     [
//         {"tableName":"person","fieldSet":["gender_concept_id","year_of_birth"]},
//         {"tableName":"drug_exposure","fieldSet":["drug_concept_id","drug_exposure_start_date","drug_exposure_end_date","drug_source_value"]},
//         {"tableName":"condition_occurrence","fieldSet":["condition_concept_id","condition_start_date","condition_end_date"]},
//         {"tableName":"measurement","fieldSet":["measurement_concept_id","measurement_date","value_as_number"]},
//         {"tableName":"visit_occurence","fieldSet":["visit_start_date","visit_type_concept_id"]}
//     ]
// }
// cdm = cdm.table;

var condition =["=","=","=","=","="];
// var condition =["<","<=","=",">=",">"];

//test
/////////////////////////////190103/////
 const { cdm } = require('./cdm/cdm_table'); //console.log(cdm);
const site_execute = require('./cdm/site_execute.js');

//////////////////////////////////////

//SITE
app.get('/test-site-execute/:study_id/:query_id',(req,res)=>{
    console.log("-----------------------------------------------start------------------------------------------------------------")
    console.log("-----------------------------------------------start------------------------------------------------------------")
    console.log("-----------------------------------------------start------------------------------------------------------------")
    util_console_print("----/test-site-execute/:study_id/:query_id----");
    var study_id = Number(req.param('study_id'));
    var query_id = Number(req.param('query_id'));
    console.log({study_id:study_id, query_id:query_id});
    var query ='';

    //let result ;
    start();
    

    //
    async function start(){
        try{
            // await asyncMain(study_id, query_id);
            // await asyncMain_protocol(study_id, query_id);
            // await asyncMain_patient(study_id, query_id);
            await count_start(study_id, query_id,res);
           
           
           sql_start = require('mssql');
           let pool_start =  await sql_start.connect(dbConfig); //scrn_cloud
           let result = await pool_start.request() 
            .input('input_paramiter1', sql.Int, study_id)
            .input('input_paramiter2', sql.Int, query_id)
            .query(`UPDATE query SET query_status = N'Done' WHERE study_id = @input_paramiter1 and query_id = @input_paramiter2`)
            //console.log(`UPDATE protocol SET protocol_status = N'InProgress' WHERE protocol_id = @input_paramiter`);
            util_console_print({"마지막_상태 업데이트": result});
            pool_start.close();
            sql_start.close();
            res.send("ok");
        }catch(err){
            console.log({"start_ERROR": err});
        }
        
    }
    ////Async 함수
    async function asyncMain(study_id, query_id) {
        try{
            let pool =  await sql.connect(dbConfig); //scrn_cloud
           //console.log(study_id, query_id);
            // 1.실행 버튼 클릭 시 '진행' 상태 변경하기.
            console.log('1.상태 업데이트_asyncMain')
           let result = await pool.request() 
            .input('input_paramiter1', sql.Int, study_id)
            .input('input_paramiter2', sql.Int, query_id)
            .query(`UPDATE query SET query_status = N'In Progress' WHERE study_id = @input_paramiter1 and query_id = @input_paramiter2`)
            //console.log(`UPDATE protocol SET protocol_status = N'InProgress' WHERE protocol_id = @input_paramiter`);
            util_console_print({"1.상태 업데이트": result});
           
            // 2. 조건 쿼리 받아와서 쿼리문 만들기.
            console.log('2. 조건쿼리 받아서 쿼리문 만들기_asyncMain');
            result = await pool.request()
            .input('input_paramiter1', sql.Int, study_id)
            .input('input_paramiter2', sql.Int, query_id)
            .query(`SELECT * FROM criteria_detail WHERE study_id = @input_paramiter1 and query_id = @input_paramiter2`)
            util_console_print({"2. 조건쿼리 받아서 쿼리문 만들기":result.recordset.length});

            var str_or_weak_or_ud  = asyncMain_2_0_assort_strength(result);  //console.log({result_str_or_weak: str_or_weak_or_ud}); // 강한조건, 유연조건 나누기, User_define
            sql.close();
            //console.log({str_or_weak_or_ud:str_or_weak_or_ud})
            // res.send(str_or_weak_or_ud);
            // return 0
            // out ={inclustion_list [], exclusion_list[]}
            var user_define_list = await asyncMain_2_0_1_list_of_user_define(str_or_weak_or_ud.user_define);
            //console.log({user_define_list:user_define_list});
            var query_list = asyncMain_2_1_make_query(str_or_weak_or_ud);

            // 3. 마지막쿼리 돌려서 결과값 넣기.
            console.log("----3번시작_asyncMain")
            sql.close();
            pool =  await sql.connect(CdmDbConfig); //scrn_cloud
            var query_result =[];
            //console.log({query_list:query_list})
            
            ////////////////////////////////////////
            //User_Define 적용하기
            let buf =''
            let query_3 = `SELECT * FROM (${query_list[query_list.length-1]}) q3 `;
            let out = user_define_list
            if(out.ud_inclusion_person_list.length!=0){
                for(i=0;i<out.ud_inclusion_person_list.length;i++){
                    buf += out.ud_inclusion_person_list[i]
                    if(i+1 != out.ud_inclusion_person_list.length){
                        buf += ", ";
                    }
                }
                query_3 += `WHERE person_id in (${buf}) `;
                
                buf ='';

                if(out.ud_exclusion_person_list.length!=0){
                    for(i=0;i<out.ud_exclusion_person_list.length;i++){
                        buf += out.ud_exclusion_person_list[i]
                        if(i+1 != out.ud_exclusion_person_list.length){
                            buf += ", ";
                        }
                    }
                    query_3 += `and person_id not in (${buf}) `;
                }
            }else{
                if(out.ud_exclusion_person_list.length!=0){
                    for(i=0;i<out.ud_exclusion_person_list.length;i++){
                        buf += out.ud_exclusion_person_list[i]
                        if(i+1 != out.ud_exclusion_person_list.length){
                            buf += ", ";
                        }
                    }
                    query_3 += `WHERE person_id not in (${buf}) `;
                }
            }

            // SQL문 적용하기 
            // 1. 포함 비포함 분류하기. 
            // 2. 포함 비포함에 맞는 sql문 적용하기. 
            let sql_in_ex = {
                in :[],
                ex : []
            }
            if(str_or_weak_or_ud.sql.length!=0){
                //1. 분류하기.
                for(i=0;i<str_or_weak_or_ud.sql.length;i++){
                    if(str_or_weak_or_ud.sql[i].criteria_state=1){
                        sql_in_ex.in.push(str_or_weak_or_ud.sql[i]);
                    }else{
                        sql_in_ex.ex.push(str_or_weak_or_ud.sql[i]);
                    }
                }

                //2. sql문 적용하기
                //inclusion.
                if(sql_in_ex.in.length!=0){
                    if(sql_in_ex.ex.length!=0){
                        query_3 = ` SELECT * FROM (${query_3}) sql WHERE person_id in (${sql_in_ex.in[0].criteria_addition}) and person_id not in (${sql_in_ex.ex[0].criteria_addition}) `;
                    }else{
                        query_3 = ` SELECT * FROM (${query_3}) sql WHERE person_id in (${sql_in_ex.in[0].criteria_addition})`;
                    }
                }else{
                    if(sql_in_ex.ex.length!=0){
                        query_3 = ` SELECT * FROM (${query_3}) sql WHERE person_id not in (${sql_in_ex.ex[0].criteria_addition}) `;
                    }
                }             
            }//if(str_or_weak_or_ud.sql.length!=0)
            ///////////////////////////////////
            
            // 전체 환자수 구하기. 
            query_3_total =` SELECT count(distinct t1.person_id) as cnt FROM (${query_3}) t1`
            result = await pool.request()
            .query(query_3_total);
            let result_total_patients = result.recordset[0].cnt;



            //최종 쿼리 실행
            console.log({"query_3_asyncMain" :query_3});
            result = await pool.request()
            .query(query_3);
            
            //make_query_insert_1_0_
            //4. 기존 쿼리 결과 지우기 및 전체 환자 수 지우기
            //change dbconfig
            sql.close();
            pool =  await sql.connect(dbConfig); //scrn_cloud
            console.log("----4번시작_asyncMain");
            // 전체환자 수 업데이트
            q = `UPDATE query SET query_total_patients =  ${result_total_patients} WHERE study_id = ${study_id} and query_id = ${query_id} ;` //console.log(q)
            await pool.request()
            .query(q); 

            // 실제 쿼리 관련 결과치 이전 값 삭제
            var result_3 = make_query_insert_1(result);
            q = "DELETE FROM doctor_result WHERE study_id = " + study_id + " and qeury_id = " + query_id; //console.log(q)
            result = await pool.request()
            .query(q);
            util_console_print({"4. 기존쿼리결과지우기_asyncMain":result});

            
            
            
            //5. 쿼리 삽입하기.
            console.log("----5번시작_asyncMain");
            console.log({result_3:result_3});
            if(result_3.length!=0){
                var query_5 = make_query_insert_5(result_3);
                result = await pool.request()
                .query(query_5);
            }else{

            }
            
            
            // 6.실행 버튼 클릭 시 '진행' 상태 변경하기.
            console.log('6 .상태 업데이트_생략_asyncMain')
        //    result = await pool.request() 
        //     .input('input_paramiter1', sql.Int, study_id)
        //     .input('input_paramiter2', sql.Int, query_id)
        //     .query(`UPDATE query SET query_status = N'Done' WHERE study_id = @input_paramiter1 and query_id = @input_paramiter2`)
        //     //console.log(`UPDATE protocol SET protocol_status = N'InProgress' WHERE protocol_id = @input_paramiter`);
        //     util_console_print({"6.상태 업데이트": result});

        //res.send({resunt:{result:result}});
        sql.close();    
        }catch(err){
            util_console_print({error: err});
            //res.send({error:'2.result is 0'});

            sql.close();
        }
       

        function asyncMain_2_0_assort_strength(result){
            var out = {
                strength : [], 
                weak : [],
                visit:[], 
                user_define:[], 
                sql:[], 
            }
            if(result.rowsAffected[0]==0){
                throw new Error(result);
            }
            rows = result.recordset;
            for (i = 0; i < rows.length; i++) {
                if (rows[i].criteria_detail_table != 4 && rows[i].criteria_detail_table != 9 && rows[i].criteria_detail_table != 7 ) {
                    if ((rows[i].strength == 1 || rows[i].strength == null)) {
                        out.strength.push(rows[i]);
                    } else {
                        out.weak.push(rows[i]);
                    }
                } else {
                    if (rows[i].criteria_detail_table == 4) {
                        out.visit.push(rows[i]);
                    }
                    if (rows[i].criteria_detail_table == 9) {
                        out.user_define.push(rows[i]);
                    }
                    if( rows[i].criteria_detail_table == 7){
                        out.sql.push(rows[i]);
                    }
                }
            }//for
            return out;

        }
        //User_Define 내용 만들기.
        async function asyncMain_2_0_1_list_of_user_define(rows_user_define){
            console.log("2-1.asyncMain_2_0_1_list_of_user_define");
            let out ={
                ud_inclusion_person_list :[],
                ud_exclusion_person_list : []
            }
            let rows_us;
            if(rows_user_define.length==0){
                return out;
            }else{
                rows_us = JSON.parse(JSON.stringify(rows_user_define));
            }
           // console.log({rows_us:rows_us});

            //console.log({rows_user_define:rows_user_define});
            // rows[] userdefide 관련 내용 ->  값 가지고와서 쿼리문 만들어야함
            // 1. Inclusion, Exclusion 분류
            // 2. 분류된 내용으로 다시 실제 조건 쿼리 만들고 리스트 함수에 각각 넣기. 
            // 3. 리턴

            

            let in_ex ={
                in : [],
                ex : []
            }
            for(i=0;i<rows_us.length;i++){
                //console.log({rows_i : rows_us[i]});
                if(rows_us[i].criteria_state==1){
                    in_ex.in.push(rows_us[i]);
                }else{
                    in_ex.ex.push(rows_us[i]);
                }
            }//for
            //console.log({in_ex:in_ex})
            // inclusion
            console.log('2-2.asyncMain_2_0_1_list_of_user_define');

            try{
                
                let ud_pool = await sql.connect(dbConfig);
                let ud_id =0;
                let ud_result;
                var str_or_weak;
                let query_list_ud
                //Inclusion
                for(i=0;i<in_ex.in.length;i++){
                    ud_id = in_ex.in[i].criteria_detail_value
                    ud_result = await ud_pool.request()
                    .input('input_paramiter1', sql.Int, ud_id)
                    .query(`SELECT * FROM user_criteria_detail WHERE user_define_id = @input_paramiter1`)
                    
                    ud_str_or_weak  = asyncMain_2_0_assort_strength(ud_result); // console.log({result_str_or_weak: str_or_weak_or_ud}); // 강한조건, 유연조건 나누기, User_define
                    ud_query_list = asyncMain_2_1_make_query(ud_str_or_weak,1);
                    //console.log({ud_query_list:ud_query_list})
                    ud_query = `SELECT distinct ud.person_id FROM (${ud_query_list[0]}) ud`

                    ud_pool.close()
                    sql.close();

                    ud_pool = await sql.connect(CdmDbConfig);
                    ud_result = await ud_pool.request()
                    .query(ud_query)
                    ud_pool.close()
                    //console.log({ud_result:ud_result.recordset})
                    ud_rows = ud_result.recordset

                    for(j=0;j<ud_rows.length;j++){
                        out.ud_inclusion_person_list= chk_person_id(out.ud_inclusion_person_list, ud_rows[j].person_id);
                    }     
                }
                ud_pool.close()
                sql.close();

                ud_pool = await sql.connect(dbConfig);
                //Exclusion
                for(i=0;i<in_ex.ex.length;i++){
                    ud_id = in_ex.ex[i].criteria_detail_value
                    ud_result = await ud_pool.request()
                    .input('input_paramiter1', sql.Int, ud_id)
                    .query(`SELECT * FROM user_criteria_detail WHERE user_define_id = @input_paramiter1`)
                    
                    ud_str_or_weak  = asyncMain_2_0_assort_strength(ud_result); // console.log({result_str_or_weak: str_or_weak_or_ud}); // 강한조건, 유연조건 나누기, User_define
                    ud_query_list = asyncMain_2_1_make_query(ud_str_or_weak,1);
                    //console.log({ud_query_list:ud_query_list})
                    ud_query = `SELECT distinct ud.person_id FROM (${ud_query_list[0]}) ud`

                    ud_pool.close()
                    sql.close();

                    ud_pool = await sql.connect(CdmDbConfig);
                    ud_result = await ud_pool.request()
                    .query(ud_query)
                    ud_pool.close()
                    //console.log({ud_result:ud_result.recordset})
                    ud_rows = ud_result.recordset

                    for(j=0;j<ud_rows.length;j++){
                        out.ud_exclusion_person_list= chk_person_id(out.ud_exclusion_person_list, ud_rows[j].person_id);
                    }     
                }

               // console.log({out:out.ud_exclusion_person_list.length});
                ud_pool.close()
                sql.close()

                //선정 - 비선정 구하기 
                let idx =-1;
                for(i=0;i<out.ud_exclusion_person_list.length;i++){
                    idx = out.ud_inclusion_person_list.indexOf(out.ud_exclusion_person_list[i])
                    if(idx!=-1){
                        out.ud_inclusion_person_list = out.ud_inclusion_person_list.splice(idx,1);
                    }
                    idx = -1;
                }
                //console.log({out:out});
                return out;
            }catch(err){
                console.log({'err_2-2.asyncMain_2_0_1_list_of_user_define':err})
                ud_pool.close()
                sql.close()
            }
           function chk_person_id (arr, person_id){
                if(arr.indexOf(person_id)==-1){
                    arr.push(person_id);
                };
                return arr;
           }
        } // asyncMain_2_0_1_list_of_user_define


        function asyncMain_2_1_make_query(rows_str_weak, condition ){
            //util_console_print('------asyncMain_2_1_make_query------'); // console.log(rows_str_weak);
            // 강한 조건에서 다시 조건 하나를 넣어준다. 
            // 조건이 넣어지면 다시 선정 제외조건을 나눠서 쿼리문을 만들어준다. 
            // 강한 조건이 끝나면 다시 약한 조건으로 하나씩 쿼리를 만든다. 
            let in_rows=[], ex_rows=[];
            let query_list =[]; 
            let inclusion='' , exclusion='';
            let visit ='';
            let buf_row;

            // visit 쿼리 만들기
                                // //visit
                                // case 4:
                                // result.visit.push (`( SELECT person_id FROM VISIT_OCCURRENCE WHERE person_id >0 ${val} )`);
                                // break;
            if(rows_str_weak.visit!=null && rows_str_weak.visit.length!=0){
                buf_row = rows_str_weak.visit;
                table = buf_row[0].criteria_detail_table; //0
                field = buf_row[0].criteria_detail_attribute; //0
                con = buf_row[0].criteria_detail_condition;
                val = buf_row[0].criteria_addition;
                title = buf_row[0].criteria_title;
              
                visit = `SELECT DISTINCT person_id FROM VISIT_OCCURRENCE WHERE person_id >0 ${val} `
            }
            //console.log({rows_str_weak:rows_str_weak})

            rows_str_weak = {
                strength : rows_str_weak.strength,
                weak : rows_str_weak.weak
            }
            //console.log({rows_str_weak:rows_str_weak})

            //강한조건부터
            let rows = rows_str_weak.strength;          //console.log({rows_count:rows.length});
            for(k=0;k<rows.length;k++){
                //console.log({k:k});
                if(rows[k].criteria_state == 1 && rows[k].criteria_detail_table !=4){
                    in_rows.push(rows[k]);
                }else if (rows[k].criteria_state != 1 && rows[k].criteria_detail_table !=4){
                    ex_rows.push(rows[k]);
                }
                //console.log({in_rows:in_rows.length, ex_rows:ex_rows.length});
                inclusion = make_query_0(in_rows,1);              // 선정 1, 제외 0 
                exclusion = make_query_0(ex_rows,0);              //console.log({for: {in:in_rows.length, ex:ex_rows.length}});

                //condition =1은 user_define 
                if(inclusion.length != 0 && condition !=1){
                    query_list.push(make_query_1(inclusion, exclusion, visit));
                }else if(k+1 == rows.length){
                    query_list.push(make_query_1(inclusion, exclusion, visit));
                }
                
            }
            
            //약한조건을 한 개씩 추가해보기
            rows = rows_str_weak.weak;
            //console.log(rows);
            let buf_in_rows =[], buf_ex_rows = [];
            
            for(k=0;k<rows.length;k++){
                //console.log(rows[k]);
                buf_in_rows = JSON.parse(JSON.stringify(in_rows));
                buf_ex_rows = JSON.parse(JSON.stringify(ex_rows));
                if(rows[k].criteria_state == 1 && rows[k].criteria_detail_table !=4){
  
                    buf_in_rows.push(rows[k]);
                }else if (rows[k].criteria_state != 1 && rows[k].criteria_detail_table !=4){
                    buf_ex_rows.push(rows[k]);
                }
                inclusion = make_query_0(buf_in_rows,1); // 선정 1, 제외 0 
                exclusion = make_query_0(buf_ex_rows,0);              //console.log({for: {in:in_rows.length, ex:ex_rows.length}});
                

                if(inclusion.length != 0 ){
                    query_list.push(make_query_1(inclusion, exclusion, visit));
                }
            }

            return query_list;
            
        }
        function make_query_0(rows, in_or_ex) {
            //util_console_print('----make_query_0--in_or_ex: '+in_or_ex+'--')
            //console.log(rows);
            var result = {
                person: [],
                drug: [],
                condition: [],
                measurement: [],
                user_define : []
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
            var select_lms_atc = ['FIRST','SECOND','THIRD','FORTH','FIFTH'];
            // var cdm={"table":
            //     [
            //         {"tableName":"person","fieldSet":["gender_concept_id","year_of_birth"]},
            //         {"tableName":"drug_exposure","fieldSet":["drug_concept_id","drug_exposure_start_date","drug_exposure_end_date","drug_source_value"]},
            //         {"tableName":"condition_occurrence","fieldSet":["condition_concept_id","condition_start_date","condition_end_date"]},
            //         {"tableName":"measurement","fieldSet":["measurement_concept_id","measurement_date","value_as_number"]},
            //         {"tableName":"visit_occurence","fieldSet":["visit_start_date","visit_type_concept_id"]}
            //     ]}

            for (i = 0; i < rows.length; i++) {
                table = rows[i].criteria_detail_table; //0
                field = rows[i].criteria_detail_attribute; //0
                con = rows[i].criteria_detail_condition;
                val = rows[i].criteria_detail_value;
                title = rows[i].criteria_title;
                if(rows[i].criteria_addition.length >0 && rows[i].criteria_addition!='null' ){
                    val += " " +rows[i].criteria_addition;
                }
                c_add = rows[i].criteria_addition;

                c_id = rows[i].criteria_id;
                //console.log("SELECT person_id FROM "+ cdm[table].tableName + " WHERE "+cdm[table].fieldSet[field]+" "+condition[con]+ " " + val );
                //condition확인
                //console.log(table);
                //쿼리 메인
                switch (Number(table)) {
                    //person
                    case 0:
                        if(field==0){ //0 젠더
                            result.person.push("( SELECT person_id, provider_id FROM person WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                        }else{
                            result.person.push("( SELECT person_id, provider_id FROM person WHERE person_id  >0 " +rows[i].criteria_addition+ " )");
                        }
                        
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
                        if(rows[i].measurement_count!=''||rows[i].measurement_method!=''){
                            result.measurement.push("( SELECT person_id, provider_id FROM measurement WHERE person_id IN (  ( SELECT person_id FROM ( SELECT A.person_id  "+ measurement_method2( rows[i].measurement_method) +" FROM ( SELECT ROW_NUMBER() OVER(partition by person_id ORDER BY measurement_date DESC ) AS num, person_id, measurement_date, value_as_number FROM dbo.MEASUREMENT WHERE measurement_concept_id = "+ rows[i].criteria_detail_value + " "+ measurement_date(rows[i].date_addition_value)+ " ) A " +measurement_num(rows[i].measurement_count )+ " GROUP BY A.person_id ) B "+measurement_min_max(rows[i].value_addition_min, rows[i].value_addition_max,rows[i].min_condition, rows[i].max_condition) +")))" );
                        }else{
                            result.measurement.push("( SELECT person_id, provider_id FROM measurement WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val +")");
                        }
                        
                        break;

                    //mtdb 약2
                    case 5:
                        //console.log(`'%${title}%'`);
                        // SELECT concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE SMALL = "%Gout%" 
                        result.drug.push(`( SELECT person_id, provider_id FROM drug_exposure WHERE drug_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE ${select_lms_atc[field-1]} LIKE \'%${title}%\') ${c_add})`);
                        //console.log({c_add:rows[i]})
                        
                        
                        break;

                    //mtdb 병력
                    case 6:
                        // SELECT concept_id FROM mtdb.dbo.icd10_snomed WHERE SMALL = "%Gout%" 
                        //console.log(`'${title}'`)
                        result.condition.push(`( SELECT person_id, provider_id FROM  CONDITION_OCCURRENCE WHERE condition_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.icd10_snomed WHERE ${select_lms[field-1]} LIKE \'%${title}%\') ${c_add})`);
                        //console.log({c_add:rows[i]})
                        
                      
                        
                        break;

                    // User Define
                    case 9:
                       
                    break;
                }// switch
                
                function measurement_method2(method){
                    if(method.length!=0){
                        return `, ${method}(A.value_as_number) as value_as_number `;
                    }
                    return '';
                }

                function measurement_num(num){
                    if(num.length==0||num==null){
                        return '';
                    }else{
                        return "WHERE num < " + num;
                    }
                }

                function measurement_date(value_date){
                   // console.log("hihi"+value_date.length);
                    if(value_date.length==0){
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
                   //console.log({in:min,max,min_con, max_con});
                    if(min!=''){
                        out += " WHERE B.value_as_number "+ measurement_condition(min_con) + " " +min ;
                    }
                    if(max!=''){
                        if(out.length!=0){
                            out += " and  B.value_as_number " + measurement_condition(max_con) + " "+ max;
                        }else{
                            out += " B.value_as_number " + measurement_condition(max_con) + " "+ max;
                        }
                        
                    }
                    return out;
                }// measurement_min_max

                function measurement_condition(con){
                    switch(con){
                        case 'more than or equal':
                        return '>=';
                        
                        case 'more than':
                        return '>';
                        
                        case 'equal':
                        return '=';

                        case 'less than or equal':
                        return '<=';
                        
                        case 'less than':
                        return '<';
                    }//switch
                }// measurement_condition
            }// for
            //console.log({result:result})
            
            
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
                    }else{
                        //tables_i = tables[i];
                       //console.log({tables_i:result[tables[0]]});
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
                        // let log1 = f_q[tables[i]];
                        // console.log({log1:log1})
                        buf_tables.push(tables[i]);
                    }
                }
                // console.log("Inserted tables:");                console.log(buf_tables);                console.log();
    
                idx = buf_tables.indexOf(tables[idx]);

                //console.log({idx:idx});
                if(idx == -1){
                    return '';
                }
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
        function make_query_1(inclusion, exclusion, visit){
            util_console_print('-----make_query_1-----');
            var query;
            if (inclusion.length == 0 || exclusion == 0) {
                query = "SELECT provider_id, A.person_id, A.year_of_birth, A.gender_concept_id FROM ( " + inclusion + " ) A  "
            } else {
                query = "SELECT provider_id, A.person_id, A.year_of_birth, A.gender_concept_id FROM ( " + inclusion + " ) A LEFT JOIN ( " + exclusion + " ) B ON A.person_id = B.person_id WHERE B.person_id IS NULL"
            }

            // visit 적용
            if(visit.length!=0){
                query = `SELECT DISTINCT F.provider_id, F.person_id, F.year_of_birth, F.gender_concept_id FROM ( ${query} ) F WHERE F.person_id in ( ${visit} )`;
            }
            
          
            return query;
        }
        function make_query_insert_1(result){
            var q_execute = []; var q_cnt = 0; // 1000개 이상일 경우 
            if (result != null) {
                let rows = result.recordset
                //als;jdfl;kasjflkas;lfajskfjasd;lfsa;lflds;af;alksdjflksd;dlskflksd;fjd
                //rows = result_m;
                console.log("query result: " + rows.length);
                //결과값 0 체크
                if (rows.length != 0) {
                    var q_list = [];
                    var chk_num = -1;
                    for (i = 0; i < rows.length; i++) {
                        if (q_list.length == 0) {
                            q_list.push(obj_provider(rows[i].provider_id));
                            //console.log(Math.floor(((new Date().getFullYear() - rows[i].year_of_birth)+1)/10)*10);
                            q_list[0] = obj_provider_update(q_list[0], Math.floor(((new Date().getFullYear() - rows[i].year_of_birth) + 1) / 10) * 10, rows[i].gender_concept_id);
                        } else {
                            //provider_id 가 있는지 검사
                            //console.log(q_list[0].id);
                            for (j = 0; j < q_list.length; j++) {
                                //console.log(q_list[j]);
                                if (q_list[j].id == rows[i].provider_id) {
                                    chk_num = j;
                                    break;
                                }
                            }
                            //없으면 0.
                            if (chk_num == -1) {

                                q_list.push(obj_provider(rows[i].provider_id));
                                q_list[q_list.length - 1] = obj_provider_update(q_list[q_list.length - 1], Math.floor(((new Date().getFullYear() - rows[i].year_of_birth) + 1) / 10) * 10, rows[i].gender_concept_id);
                            } else {
                                q_list.push(obj_provider(rows[i].provider_id));
                                q_list[chk_num] = obj_provider_update(q_list[chk_num], Math.floor(((new Date().getFullYear() - rows[i].year_of_birth) + 1) / 10) * 10, rows[i].gender_concept_id);
                                chk_num = -1;
                            }
                        }
                    }//for i                     //console.log(q_list);                     //res.send(q_list);


                    //make_query_execute2(rows);
                    // --원본--
                    console.log("q_execute start")

                    for (i = 0; i < q_list.length; i++) {
                        if (q_list[i].total > 0) {
                            q_execute.push("(2, " + q_list[i].id + ", " + q_list[i].total + ", " + query_id + ", " + study_id + ", N'" + output_string(q_list[i].male) + "', N'" + output_string(q_list[i].female) + "')");

                        }
                    }
                    //console.log("q_execute");
                    //console.log(q_execute)
                    //res.send(q_execute);
                    return q_execute;
                } else {
                    console.log({ make_query_insert_1_result: 0 })
                    //console.log(err);
                    return q_execute;
                }
            } else {
                console.log(err);
                //res.send(err);
            }
            function output_string(obj) {
                var out = obj.d9 + ", " + obj.d10 + ", " + obj.d20 + ", " + obj.d30 + ", " + obj.d40 + ", " + obj.d50 + ", " + obj.d60 + ", " + obj.d70;
                return out;
            }
        }//make_query_insert_1

        function make_query_insert_5(q_execute){
            var cnt_1000 = 0;
            var query = "";
            var q_final = [];
            //console.log(q_execute);
            if(q_execute.length !=0){
                for (i = 0; i < q_execute.length; i++) {
                    query += q_execute[i];
                    //console.log(q_execute[i]);
                    cnt_1000 += 1;
                    if (cnt_1000 + 1 == 1000) {
                        q_final.push("INSERT INTO doctor_result(corp_id, doctor_id, patient_num, qeury_id, study_id, male_num, female_num) VALUES " + query + " ;");
                        cnt_1000 = 0;
                        query = "";
                    } else {
                        if (i + 1 != q_execute.length) {
                            query += ", ";
                        }
                    }
                }
                q_final.push("INSERT INTO doctor_result(corp_id, doctor_id, patient_num, qeury_id, study_id, male_num, female_num) VALUES " + query + " ;");
                //res.send(q_final);
                console.log("q_final.length: " + q_final.length);
                q_list1 = "";
                for (i = 0; i < q_final.length; i++) {
                    q_list1 += q_final[i];
                }
            }// if(q_execute.length ==0)
            
            return q_list1;
        }//make_query_insert_5

    }//async

    //protocol 함수부분
    async function asyncMain_protocol(study_id, query_id) {
        console.log(`--------protocol_부분----input: ${study_id}, ${query_id}`);
        try{
            let pool =  await sql.connect(dbConfig); //scrn_cloud
           //console.log(study_id, query_id);
            // 1.실행 버튼 클릭 시 '진행' 상태 변경하기.
            console.log('1.상태 업데이트_생략')
          
         
            // 2. 조건 쿼리 받아와서 쿼리문 만들기.
            console.log('2. 조건쿼리 받아서 쿼리문 만들기_asyncMain_protocol');
            let result = await pool.request() 
            .input('input_paramiter1', sql.Int, study_id)
            .input('input_paramiter2', sql.Int, query_id)
            .query(`SELECT * FROM criteria_detail  WHERE study_id = @input_paramiter1 and query_id = @input_paramiter2`)
            util_console_print({"2. 조건쿼리 받아서 쿼리문 만들기_asyncMain_protocol":result.recordset.length});

            var str_or_weak_or_ud  = asyncMain_2_0_assort_strength(result);  //console.log({result_str_or_weak: str_or_weak}); // 강한조건, 유연조건 나누기. 
            sql.close();
            //console.log({str_or_weak_or_ud:str_or_weak_or_ud})
            // res.send(str_or_weak_or_ud);
            // return 0
            // out ={inclustion_list [], exclusion_list[]}
            var user_define_list = await asyncMain_2_0_1_list_of_user_define(str_or_weak_or_ud.user_define);


            var query_list = asyncMain_2_1_make_query(str_or_weak_or_ud);
            //console.log({query_list:query_list});
            // 3. 마지막쿼리 돌려서 결과값 넣기.
            console.log("----3번시작_asyncMain_protocol")
            sql.close();
            pool =  await sql.connect(CdmDbConfig); //scrn_cloud
            var query_result =[];
            console.log({query_list:query_list})
            
              ////////////////////////////////////////
            //User_Define 적용하기
            let buf =''
            let query_3 = `SELECT * FROM (${query_list[query_list.length-1]}) q3 `;
            let out = user_define_list
            if(out.ud_inclusion_person_list.length!=0){
                for(i=0;i<out.ud_inclusion_person_list.length;i++){
                    buf += out.ud_inclusion_person_list[i]
                    if(i+1 != out.ud_inclusion_person_list.length){
                        buf += ", ";
                    }
                }
                query_3 += `WHERE person_id in (${buf}) `;
                
                buf ='';

                if(out.ud_exclusion_person_list.length!=0){
                    for(i=0;i<out.ud_exclusion_person_list.length;i++){
                        buf += out.ud_exclusion_person_list[i]
                        if(i+1 != out.ud_exclusion_person_list.length){
                            buf += ", ";
                        }
                    }
                    query_3 += `and person_id not in (${buf}) `;
                }
            }else{
                if(out.ud_exclusion_person_list.length!=0){
                    for(i=0;i<out.ud_exclusion_person_list.length;i++){
                        buf += out.ud_exclusion_person_list[i]
                        if(i+1 != out.ud_exclusion_person_list.length){
                            buf += ", ";
                        }
                    }
                    query_3 += `WHERE person_id not in (${buf}) `;
                }
            }

            // SQL문 적용하기 
            // 1. 포함 비포함 분류하기. 
            // 2. 포함 비포함에 맞는 sql문 적용하기. 
            let sql_in_ex = {
                in :[],
                ex : []
            }
            if(str_or_weak_or_ud.sql.length!=0){
                //1. 분류하기.
                for(i=0;i<str_or_weak_or_ud.sql.length;i++){
                    if(str_or_weak_or_ud.sql[i].criteria_state=1){
                        sql_in_ex.in.push(str_or_weak_or_ud.sql[i]);
                    }else{
                        sql_in_ex.ex.push(str_or_weak_or_ud.sql[i]);
                    }
                }

                //2. sql문 적용하기
                //inclusion.
                if(sql_in_ex.in.length!=0){
                    if(sql_in_ex.ex.length!=0){
                        query_3 = ` SELECT * FROM (${query_3}) sql WHERE person_id in (${sql_in_ex.in[0].criteria_addition}) and person_id not in (${sql_in_ex.ex[0].criteria_addition}) `;
                    }else{
                        query_3 = ` SELECT * FROM (${query_3}) sql WHERE person_id in (${sql_in_ex.in[0].criteria_addition})`;
                    }
                }else{
                    if(sql_in_ex.ex.length!=0){
                        query_3 = ` SELECT * FROM (${query_3}) sql WHERE person_id not in (${sql_in_ex.ex[0].criteria_addition}) `;
                    }
                }             
            }//if(str_or_weak_or_ud.sql.length!=0)
            ///////////////////////////////////

            
            
            //최종 쿼리 실행 
            console.log({query_3 :query_3});
            result = await pool.request()
            .query(query_3);
            
            //4. 기존 쿼리 결과 지우기
            //change dbconfig
            sql.close();
            pool =  await sql.connect(dbConfig); //scrn_cloud
            console.log("----4번시작_asyncMain_protocol");
            var result_3 = make_query_insert_1(result,study_id, query_id);
            q = `DELETE FROM protocol_result WHERE study_id =  ${study_id} and query_id = ${query_id} ` ;
            //console.log(q)
            result = await pool.request()
            .query(q);
            util_console_print({"4. 기존쿼리결과지우기":result});
            
            //5. 쿼리 삽입하기.
            // protocol_result 환자리스트 넣기
            console.log("----5번시작_asyncMain_protocol");

            var query_5 = make_query_insert_5(result_3);
            //console.log({query_5:query_5});
            result = await pool.request()
            .query(query_5);
            //console.log("end");
            //6. protocol_detail_result
            // protocol_result 환자리스트 넣기
            let person_list = '';

            let drug_info = [];
            let measurement_info = [];
            let condition_info = [];
            let visit_info =[];
            let person_info =[];

            let result_person = await pool.request()
                .input('input_paramiter1', sql.Int, study_id)
                .input('input_paramiter2', sql.Int, query_id)
                .query('SELECT person_id FROM dbo.protocol_result  WHERE study_id = @input_paramiter1 and query_id = @input_paramiter2')
            person_list = make_list_to_string(result_person);  //console.log(person_list); //log찍기
            
            /////BIGDATADB 접속 
            sql.close();

            pool =  await sql.connect(CdmDbConfig); //scrn_cloud
            //// 환자
            let result_person1 = await pool.request()
                .query('SELECT gender_concept_id, year_of_birth FROM dbo.PERSON WHERE person_id in ( '+person_list+' );')
            person_info = make_result_person_to_list(result_person1.recordset);
            

            var newdate = new Date();
            var date_start = `'${newdate.getFullYear()-5}-${pad(newdate.getMonth()+1,2)}-${pad(newdate.getDate(),2)}'`;
           
            ////약.
            q_drug = 'SELECT f1.person_id, f1.gender_concept_id, f1.drug_concept_id, f1.drug_exposure_start_date, f1.drug_exposure_end_date, f1.quantity, f1.days_supply, f1.dose_unit_source_value,c1.concept_name, a1.SECOND as atc2, a1.THIRD as atc3, a1.FORTH as atc4, a1.FIFTH as atc5 FROM (SELECT p1.person_id, p1.gender_concept_id, d1.drug_concept_id, d1.drug_exposure_start_date, d1.drug_exposure_end_date, d1.quantity, d1.days_supply, d1.dose_unit_source_value FROM (SELECT person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, quantity, days_supply, dose_unit_source_value FROM dbo.DRUG_EXPOSURE WHERE dbo.DRUG_EXPOSURE.person_id in('+person_list+')) d1, dbo.PERSON p1 WHERE d1.person_id = p1.person_id and drug_exposure_start_date >= '+date_start+' ) f1, concept c1, mtdb.dbo.atc_rxnorm_all a1 WHERE f1.drug_concept_id = c1.concept_id and f1.drug_concept_id = a1.concept_id'
            console.log({q_drug:q_drug})
            let result_drug = await pool.request()
                .query(q_drug);
            // durg 결과 내용을 리스트로 만들기.
            drug_info = make_result_drug_to_list(result_drug.recordset);    //console_println(drug_info.length);

            //console.log({drug_info:drug_info[0]});            return 0;

            ////Lab
            m_q1 = 'SELECT f1.person_id, f1.gender_concept_id, f1.measurement_concept_id, f1.measurement_date, f1.value_as_number, f1.range_low, f1.range_high, f1.unit_source_value, c1.concept_name FROM (SELECT  d1.person_id, p1.gender_concept_id, d1.measurement_concept_id, d1.measurement_date, d1.value_as_number, d1.range_low, d1.range_high, d1.unit_source_value FROM (SELECT person_id, measurement_concept_id, measurement_date, value_as_number, range_low, range_high, unit_source_value FROM dbo.MEASUREMENT WHERE dbo.MEASUREMENT.person_id in('+person_list+')) d1, dbo.PERSON p1 WHERE d1.person_id = p1.person_id ) f1, concept c1 WHERE f1.measurement_concept_id = c1.concept_id';
            m_q2 = `SELECT DISTINCT v1.person_id, v1.measurement_concept_id, v1.value_as_number, v1.concept_name, v1.range_low, v1.range_high FROM ( ${m_q1} ) v1 WHERE v1.measurement_date = ( SELECT MAX(measurement_date) FROM ( ${ m_q1} ) v2 WHERE v2.person_id = v1.person_id)`
            console.log({m_q2:m_q2})
            let result_measurement = await pool.request()
                .query(m_q2);
            measurement_info= make_result_measurement_to_list(result_measurement.recordset);    //console_println(measurement_info.length);
            console.log({measurement_info:measurement_info[0]});
            ////Condition
            q_con = 'SELECT  f1.person_id, f1.gender_concept_id, f1.condition_concept_id, f1.condition_start_date, c1.concept_name, i1.mid as icd10_mid, i1.small as icd10_small, i1.CONCEPT_NAME as icd10_concept_name  FROM (SELECT d1.person_id, p1.gender_concept_id, d1.condition_concept_id, d1.condition_start_date FROM (SELECT person_id, condition_concept_id, condition_start_date FROM dbo.CONDITION_OCCURRENCE WHERE dbo.CONDITION_OCCURRENCE.person_id in('+person_list+')) d1, dbo.PERSON p1 WHERE d1.person_id = p1.person_id and d1.condition_start_date >= '+ date_start+') f1, concept c1 , mtdb.dbo.icd10_snomed_category i1 WHERE f1.condition_concept_id = c1.concept_id AND f1.condition_concept_id = i1.concept_id';
            console.log({q_protocol_condition:q_con});
            let result_condition = await pool.request()
                .query(q_con);
            condition_info= make_result_condition_to_list(result_condition.recordset);  //console_println(condition_info.length);
            
            ////Visit
            visit_q = 'SELECT p1.person_id, p1.gender_concept_id, d1.visit_concept_id, d1.visit_start_date, d1.visit_end_date, d1.care_site_id, d1.visit_occurrence_id FROM (SELECT person_id, visit_concept_id, visit_start_date, visit_end_date,care_site_id, visit_occurrence_id FROM dbo.VISIT_OCCURRENCE WHERE dbo.VISIT_OCCURRENCE.person_id in( '+person_list+' )) d1, dbo.PERSON p1 WHERE d1.person_id = p1.person_id and d1.visit_start_date >= '+date_start
            last_q = `SELECT DISTINCT v1.person_id, v1.visit_concept_id FROM ( ${visit_q} ) v1 WHERE v1.visit_occurrence_id = ( SELECT MAX(visit_occurrence_id) FROM (${ visit_q }) v2 WHERE v2.person_id  = v1.person_id)`

            //console.log({visit_q:last_q});

            let result_visit = await pool.request()
                .query(last_q);
            visit_info= make_result_visit_to_list(result_visit.recordset); //console_println(visit_info);
            //console.log(result_visit.recordset);
            let result1 ={
                person_info: person_info,
                drug_info: drug_info,
                measurement_info: measurement_info,
                condition_info: condition_info,
                visit_info:visit_info
            }; //console_println(result);

            // 환자통계, 진단통계, 약통계, 랩통계 정리해보자.
            // 각 내용을 JSON으로 받는다.
            // 각 내용을 컬럼에 맞게 디비를 저장해준다.
            let distribution_person         = make_distribution_person(result1.person_info, result1.visit_info);
            let distribution_condition      = make_distribution_condition(result1.condition_info); //console.log({distribution_condition:distribution_condition});
           
            
            let distribution_drug           = make_distribution_drug(result1.drug_info); //console.log({distribution_drug:distribution_drug});           // res.send(distribution_drug);  return 0;
            let distribution_measurement    = make_distribution_measurement(result1.measurement_info);
            ///////
            console.log({distribution_measurement_1:distribution_measurement[1]});

            var sortField = "person_count"


            for(i=0;i<4;i++){
                switch(i){
                    case 0:
                        buf_list = distribution_drug.atc2
                        buf_list.sort(function(a,b){
                            return b[sortField] - a[sortField];
                        });
                        distribution_drug.atc2 = buf_list;
                    break;

                    case 1:
                        buf_list = distribution_drug.atc3
                        buf_list.sort(function(a,b){
                            return b[sortField] - a[sortField];
                        });
                        distribution_drug.atc3 = buf_list;
                    break;

                    case 2:
                        buf_list = distribution_drug.atc4
                        buf_list.sort(function(a,b){
                            return b[sortField] - a[sortField];
                        });
                        distribution_drug.atc4 = buf_list;
                    break;
                    
                    default:
                    
                        buf_list = distribution_drug.atc5
                        buf_list.sort(function(a,b){
                            return b[sortField] - a[sortField];
                        });
                        distribution_drug.atc5 = buf_list;
                    break;
                }
            }

            //console.log({distribution_drug : distribution_drug.length})

            for(i=0;i<3;i++){
                switch(i){
                    case 0:
                        buf_list = distribution_condition.icd10_mid
                        buf_list.sort(function(a,b){
                            return b[sortField] - a[sortField];
                        });
                        distribution_condition.icd10_mid = buf_list;
                break;

                case 1:
                    buf_list = distribution_condition.icd10_small
                    buf_list.sort(function(a,b){
                        return b[sortField] - a[sortField];
                    });
                    distribution_condition.icd10_small = buf_list;
                break;
                
                default:
                    buf_list = distribution_condition.icd10_concept_name
                    buf_list.sort(function(a,b){
                        return b[sortField] - a[sortField];
                    });
                    distribution_condition.icd10_concept_name = buf_list;
                break;

                }             
            }

            distribution_measurement.sort(function(a,b){
                return b[sortField] - a[sortField];
            });
      

            if(distribution_condition.icd10_mid.length >= 100){
                distribution_condition.icd10_mid = distribution_condition.icd10_mid.slice(0,100);
            }
            if(distribution_condition.icd10_small.length >= 100){
                distribution_condition.icd10_small = distribution_condition.icd10_small.slice(0,100);
            }
            if(distribution_condition.icd10_concept_name.length >= 100){
                distribution_condition.icd10_concept_name = distribution_condition.icd10_concept_name.slice(0,100);
            }


            if(distribution_drug.atc2.length >= 100){
                distribution_drug.atc2 = distribution_drug.atc2.slice(0,100);
            }
            if(distribution_drug.atc3.length >= 100){
                distribution_drug.atc3 = distribution_drug.atc3.slice(0,100);
            }
            if(distribution_drug.atc4.length >= 100){
                distribution_drug.atc4 = distribution_drug.atc4.slice(0,100);
            }
            if(distribution_drug.atc5.length >= 100){
                distribution_drug.atc5= distribution_drug.atc5.slice(0,100);
            }

            if(distribution_measurement.length >= 100){
                distribution_measurement = distribution_measurement.slice(0,100);
            }


            ////SCRNDB접속
            sql.close();
            pool =  await sql.connect(dbConfig); //scrn_cloud
            // 삭제
            lp('query_delete_distribution','')
            let query_delete_distribution = await pool.request()
                .input('input_paramiter1', sql.Int, study_id)
                .input('input_paramiter2', sql.Int, query_id)
                .query('DELETE FROM protocol_detail_result WHERE study_id = @input_paramiter1 and query_id = @input_paramiter2')
                console.log(query_delete_distribution);
            
            // 삽입
            lp('query_insert_distribution','')
            let query_insert_distribution = await pool.request()
                .input('study_id',sql.Int, study_id)
                .input('query_id',sql.Int, query_id)
                .input('input_person', sql.NVarChar, JSON.stringify(distribution_person))
                .input('input_condition', sql.NVarChar, JSON.stringify(distribution_condition))
                .input('input_drug', sql.NVarChar, JSON.stringify(distribution_drug))
                .input('input_measurement', sql.NVarChar, JSON.stringify(distribution_measurement))
                .query('INSERT INTO protocol_detail_result(protocol_id, study_id, query_id,person, condition, drug,measurement) VALUES(1, @study_id, @query_id, @input_person, @input_condition, @input_drug, @input_measurement)')
                console.log(query_insert_distribution);



                // 99.실행 버튼 클릭 시 '진행' 상태 변경하기.
            console.log('99.상태 업데이트_생략')
            // result = await pool.request() 
            //  .input('input_paramiter1', sql.Int, protocol_id)
            //  .query(`UPDATE protocol SET protocol_status = N'Done' WHERE protocol_id = @input_paramiter1`)
            //  //console.log(`UPDATE protocol SET protocol_status = N'InProgress' WHERE protocol_id = @input_paramiter`);
            //  util_console_print({"99.상태 업데이트": result});

        //res.send({resunt:{query_insert_distribution:query_insert_distribution}});
        sql.close();    
        }catch(err){
            util_console_print({error: err});
            //res.send({error:'2.result is 0'});

            sql.close();
        }
        // 필요한 내부 함수 부분입니다//
        function make_distribution_person(person, visit){
            var age ={
                range_9 :0,
                range_10 :0,
                range_20 :0,
                range_30 :0,
                range_40 :0,
                range_50 :0,
                range_60 :0,
                range_70 :0
            };   // 9-, 10-19, 20-29, 30~39, 40~49, 50~59, 60~69, 70~
            
            var result_visit={
                emergency :0,
                out: 0,
                in : 0
            }  ;   // [9203: 응급, 9202 :외래, 9201:입원]

            var gender ={
                male :0,
                female :0
            } ;   // [male, female]
            var now_year = new Date().format('yyyy');
            //console.log({gender: person})
            // 나이 및 성별 분포
            for(i=0;i<person.length;i++){
                //gender
               
                if(person[i].gender==8507){
                    gender.male +=1;
                }else{
                    gender.female +=1;
                }

                //age 
                buf_age = now_year - person[i].year;
                if(buf_age <=9 ){
                    age.range_9 +=1;
                }else if (buf_age <=19){
                    age.range_10 +=1;
                }else if (buf_age <=29){
                    age.range_20 +=1;
                }else if (buf_age <=39){
                    age.range_30 +=1;
                }else if (buf_age <=49){
                    age.range_40 +=1;
                }else if (buf_age <=59){
                    age.range_50 +=1;
                }else if (buf_age <=69){
                    age.range_60 +=1;
                }else{
                    age.range_70 +=1;
                }
            }// end for

            // 방문
            for(i=0;i<visit.length;i++){
                switch(Number(visit[i].visit_concept_id)){
                    case(9203):
                    result_visit.emergency +=1;
                        break;
                    case(9202):
                    result_visit.out +=1;
                        break;
                    case(9201):
                    result_visit.in +=1;
                        break;
                }
            }//end for
            
            var result ={
                age : age,
                gender : gender,
                visit : result_visit
            }

            return result;
        } //make_distribution_person
        function make_distribution_condition(condition_info){
            lp("make_distribution_condition","");
            var rows = condition_info;

            var result_condition_name =[];
            var result_condition_value =[];
            var result_condition_id = [];
            var result_condition_person_list ={
                condition_concept_id: 0, 
                list: []
            }
            
            var isNull;
            
            var condition_list =[];


            var result =[];
            console.log(condition_info.length);
            for(i_icd10=0;i_icd10<3;i_icd10++){
                condition_list =[];
                for(i=0;i<rows.length;i++){        
                    //i=0
                    icd10_value = rows[i].icd10[i_icd10];
                    if(icd10_value==''){
                        continue;
                    }else{
                        if(i==0){
                            condition_list.push(class_protocol_condition(icd10_value, icd10_value, rows[i].person_id));
                        }else{
                            isNull = list_chk(icd10_value, condition_list);
                           // console.log({isNull:isNull});
                            if(isNull==-1){
                                condition_list.push(class_protocol_condition(icd10_value,icd10_value, rows[i].person_id));
                            }else{
                               // console.log({isNull:isNull});
                               //console.log({person_id: rows[i].person_id});
                                p_chk = list_chk_person(rows[i].person_id, condition_list[isNull].person_list)
                                if(p_chk!=1){
                                    condition_list[isNull].person_list.push(rows[i].person_id);
                                }
                            }
                        }
                    }
                }// end for i

                //console.log(condition_list[2]);
                // 객체에 넣기
                var chk =0;
                result_buf =[]
                for(i=0;i<condition_list.length;i++){
                    result_buf.push(class_distribution_result(condition_list[i].condition_name, condition_list[i].person_list.length));
                    chk += condition_list[i].condition_value;
                }
                result.push(result_buf);
            }//for i_icd10
            
            result ={
                icd10_mid           : result[0],
                icd10_small         : result[1],
                icd10_concept_name  : result[2]
            }
            //console.log({chk:chk});
            return result;

            function list_chk(id, condition_list){
                var c_out = -1;
                for(ci=0;ci<condition_list.length;ci++){
                   // console.log(ci);
                    if(id == condition_list[ci].condition_id){
                        //console.log('맞아요');
                        c_out = ci;
                        //console.log(c_out);
                    }
                }
                return c_out;
            }//list_chk
            function list_chk_person(person_id, condition_list_person){
                var out = 0;
                //console.log(condition_list_person.length)
                for(pi=0;pi<condition_list_person.length;pi++){
                    if(condition_list_person[pi] == person_id){
                        out = 1;
                    }
                }
                return out;
            }//list_chk_person

        } //make_distribution_condition
        function make_distribution_drug(drug_info){
            lp("make_distribution_drug","");
            var rows = drug_info;

            var result_drug_name =[];
            var result_drug_value =[];
            var result_drug_id =[];

            var isNull;
            
            var result =[];

            var condition_list =[];

            console.log({make_distribution_drug_rows_length:rows.length});
            console.log({make_distribution_drug_rows_0:rows[1]});
            atc_value =''

            for(i_atc=0;i_atc<4;i_atc++){
                condition_list =[]
                for(i=0;i<rows.length;i++){        
                    //i=0
                    atc_value = rows[i].atc[i_atc]
                    if(atc_value ==''){
                        continue;
                    }else{
                        if(i==0){
                            condition_list.push(class_protocol_condition(atc_value, atc_value, rows[i].person_id));
                        }else{
                            isNull = list_chk(atc_value, condition_list);
                            if(isNull==-1){
                                //console.log('add');
                                condition_list.push(class_protocol_condition(atc_value, atc_value, rows[i].person_id));
                            }else{
                                p_chk = list_chk_person(rows[i].person_id, condition_list[isNull].person_list)
                                if(p_chk!=1){
                                    condition_list[isNull].person_list.push(rows[i].person_id);
                                }
                            }
                        }
                    }
                }// end for
                // 객체에 넣기
                var chk=0; // 잘 저장됐는지 검토하기.
                result_buf =[];

                for(i=0;i<condition_list.length;i++){
                    result_buf.push(class_distribution_result(condition_list[i].condition_name, condition_list[i].person_list.length));
                    chk += condition_list[i].person_list.length;
                }//for
                
                result.push(result_buf)
            }//end for

            result ={
                atc2 : result[0],
                atc3 : result[1],
                atc4 : result[2],
                atc5 : result[3],
            }
            //console.log(chk)
            return result;

            function list_chk(id, condition_list){
                var c_out = -1;
                for(ci=0;ci<condition_list.length;ci++){
                   // console.log(ci);
                    if(id == condition_list[ci].condition_id){
                        //console.log('맞아요');
                        c_out = ci;
                        //console.log(c_out);
                    }
                }
                return c_out;
            }//list_chk
            function list_chk_person(person_id, condition_list_person){
                var out = 0;
                //console.log(condition_list_person.length)
                for(pi=0;pi<condition_list_person.length;pi++){
                    if(condition_list_person[pi] == person_id){
                        out = 1;
                    }
                }
                return out;
            }//list_chk_person

        } //make_distribution_drug     
        function make_distribution_measurement(measurement_info){
            lp("make_distribution_measurement","");
            var rows = measurement_info;

            var result_measurement_name =[];
            var result_measurement_value =[];
            var result_measurement_id =[];

            var isNull;
            
            var result =[];

            /////
            var measurement_list =[];
            /////

            // v1.person_id, v1.measurement_concept_id, v1.value_as_number, v1.concept_name
            console.log(rows.length);
            for(i=0;i<rows.length;i++){        
                //i=0
                if(i==0){
                    measurement_list.push(class_protocol_measurement(rows[i].measurement_concept_id, rows[i].concept_name, rows[i].value_as_number, rows[i].person_id, rows[i].range_low, rows[i].range_high));
                }else{
                    isNull = list_chk(rows[i].measurement_concept_id, measurement_list);
                    if(isNull==-1){
                        measurement_list.push(class_protocol_measurement(rows[i].measurement_concept_id, rows[i].concept_name, rows[i].value_as_number, rows[i].person_id,  rows[i].range_low, rows[i].range_high));
                    }else{
                        p_chk = list_chk_person(rows[i].person_id, measurement_list[isNull].person_list)
                        if(p_chk!=1){
                            measurement_list[isNull].person_list.push(rows[i].person_id);
                            measurement_list[isNull].value_as_number.push(rows[i].value_as_number);
                            measurement_list[isNull].max < rows[i].value_as_number ? measurement_list[isNull].max = rows[i].value_as_number :1
                            measurement_list[isNull].min > rows[i].value_as_number ? measurement_list[isNull].min = rows[i].value_as_number :1
                        }
                    }
                }
            }// end for

            // 객체에 넣기
            var chk=0; // 잘 저장됐는지 검토하기.
            // name : name, 
            // avg: avg, 
            // min: min,
            // max: max,
            // person_count: person_value
            console.log({measurement_list_1:measurement_list[1]});
            buf_list =[];
            for(i_last=0;i_last<measurement_list.length;i_last++){
                buf_list = measurement_list[i_last].value_as_number

                avg = Measurement_avg(buf_list);
                sd = Measurement_sd(buf_list, avg);
                min = Number(measurement_list[i_last].min).toFixed(2);
                max = Number(measurement_list[i_last].max).toFixed(2);
                low =  measurement_list[i_last].low;
                high =  measurement_list[i_last].high; 

                result.push(class_distribution_result_measurement(measurement_list[i_last].measurement_name, avg, sd, min, max, measurement_list[i_last].person_list.length, low, high ));
               // chk += result_measurement_value[i];
            }
            //console.log(measurement_list);
            
            return result;

            function list_chk(id, condition_list){
                var c_out = -1;
                for(ci=0;ci<condition_list.length;ci++){
                   // console.log(ci);
                    if(id == condition_list[ci].measurement_concept_id){
                        //console.log('맞아요');
                        c_out = ci;
                        //console.log(c_out);
                    }
                }
                return c_out;
            }//list_chk
            function list_chk_person(person_id, condition_list_person){
                var out = 0;
                //console.log(condition_list_person.length)
                for(pi=0;pi<condition_list_person.length;pi++){
                    if(condition_list_person[pi] == person_id){
                        out = 1;
                    }
                }
                return out;
            }//list_chk_person
            function Measurement_avg(value_list){
                let out =0;
                let cnt =0;
                for(i_avg=0;i_avg<value_list.length;i_avg++){
                    out += value_list[i_avg]
                }
                return Number(out/value_list.length).toFixed(2);
            }
            function Measurement_sd(value_list, avg){
                out =0;
                //sum (x-x')^2
                for(i_sd=0;i_sd<value_list.length;i_sd++){
                    out += Math.pow((value_list[i_sd]-avg),2)
                }
                // 1/n * sum (x-x')^2
                out = Math.sqrt(out/value_list.length).toFixed(2);
                return out;
            }
        } // make_distribution_measurement

        function make_result_person_to_list(recordset){
            let result = [];
            for(i=0;i<recordset.length;i++){
                result.push(class_person(recordset[i].gender_concept_id, recordset[i].year_of_birth));
            }
            return result;
        } 

        function make_result_visit_to_list(recordset){
            let result = [];
            for (i = 0; i < recordset.length; i++) {
                result.push(class_visit(
                    recordset[i].person_id,
                    recordset[i].gender_concept_id,
                    recordset[i].visit_concept_id,
                    new Date(recordset[i].visit_start_date).format("yyyy-MM-dd"),
                    new Date(recordset[i].visit_end_date).format("yyyy-MM-dd"),
                    recordset[i].care_site_id
                ))
            }
            return result;
        }

        // i1.mid as icd10_mid, i1.small as icd10_small, i1.CONCEPT_NAME as icd10_concept_name
        function make_result_condition_to_list(recordset) {
            let result = [];

            for (i = 0; i < recordset.length; i++) {
            
                result.push(class_condition(
                    recordset[i].person_id,
                    recordset[i].gender_concept_id,
                    recordset[i].condition_concept_id,
                    new Date(recordset[i].condition_start_date).format("yyyy-MM-dd"),
                    recordset[i].concept_name, 
                    Icd10_code(recordset[i].icd10_mid, recordset[i].icd10_small, recordset[i].icd10_concept_name)
                ))
            }

            return result;
        }

        //durg 결과 내용을 리스트로 만들기. 
        function make_result_drug_to_list(recordset){
            // a1.SECOND as atc2, a1.THIRD as atc3, a1.FORTH as atc4, a1.FIFTH as atc5 
            let result =[];
            for(i=0;i<recordset.length;i++){
                result.push(class_drug(
                    recordset[i].person_id,
                    recordset[i].gender_concept_id,
                    recordset[i].drug_concept_id,
                    new Date(recordset[i].drug_exposure_start_date).format("yyyy-MM-dd"),
                    new Date(recordset[i].drug_exposure_end_date).format("yyyy-MM-dd"),
                    recordset[i].quantity,
                    recordset[i].days_supply,
                    recordset[i].dose_unit_source_value,
                    recordset[i].concept_name,
                    Atc_code(recordset[i].atc2,recordset[i].atc3,recordset[i].atc4, recordset[i].atc5)
                    ))
            }

            return result;
        }

        //measurement_info 결과 내용을 리스트로 만들기.
        function make_result_measurement_to_list(recordset){
            let result = [];
            for(i=0;i<recordset.length;i++){
                result.push(class_measurement(
                    recordset[i].person_id,
                    recordset[i].gender_concept_id,
                    recordset[i].measurement_concept_id,
                    new Date(recordset[i].measurement_date).format("yyyy-MM-dd"),
                    recordset[i].value_as_number,
                    recordset[i].range_low,
                    recordset[i].range_high,
                    recordset[i].unit_source_value,
                    recordset[i].concept_name
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
        function asyncMain_2_0_assort_strength(result){
            var out = {
                strength : [], 
                weak : [],
                visit:[], 
                user_define:[], 
                sql:[]
            }
            if(result.rowsAffected[0]==0){
                throw new Error(result);
            }
            rows = result.recordset;
            for (i = 0; i < rows.length; i++) {
                if (rows[i].criteria_detail_table != 4 && rows[i].criteria_detail_table != 9 && rows[i].criteria_detail_table != 7 ) {
                    if ((rows[i].strength == 1 || rows[i].strength == null)) {
                        out.strength.push(rows[i]);
                    } else {
                        out.weak.push(rows[i]);
                    }
                } else {
                    if (rows[i].criteria_detail_table == 4) {
                        out.visit.push(rows[i]);
                    }
                    if (rows[i].criteria_detail_table == 9) {
                        out.user_define.push(rows[i]);
                    }
                    if( rows[i].criteria_detail_table == 7){
                        out.sql.push(rows[i]);
                    }
                }
            }//for
            return out;

        }
        async function asyncMain_2_0_1_list_of_user_define(rows_user_define){
            console.log("2-1.asyncMain_2_0_1_list_of_user_define");
            let out ={
                ud_inclusion_person_list :[],
                ud_exclusion_person_list : []
            }
            let rows_us;
            if(rows_user_define.length==0){
                return out;
            }else{
                rows_us = JSON.parse(JSON.stringify(rows_user_define));
            }
            //console.log({rows_us:rows_us});

            //console.log({rows_user_define:rows_user_define});
            // rows[] userdefide 관련 내용 ->  값 가지고와서 쿼리문 만들어야함
            // 1. Inclusion, Exclusion 분류
            // 2. 분류된 내용으로 다시 실제 조건 쿼리 만들고 리스트 함수에 각각 넣기. 
            // 3. 리턴

            let in_ex ={
                in : [],
                ex : []
            }
            for(i=0;i<rows_us.length;i++){
                //console.log({rows_i : rows_us[i]});
                if(rows_us[i].criteria_state==1){
                    in_ex.in.push(rows_us[i]);
                }else{
                    in_ex.ex.push(rows_us[i]);
                }
            }//for
            //console.log({in_ex:in_ex})
            // inclusion
            console.log('2-2.asyncMain_2_0_1_list_of_user_define');

            try{
                
                let ud_pool = await sql.connect(dbConfig);
                let ud_id =0;
                let ud_result;
                var str_or_weak;
                let query_list_ud
                //Inclusion
                for(i=0;i<in_ex.in.length;i++){
                    ud_id = in_ex.in[i].criteria_detail_value
                    ud_result = await ud_pool.request()
                    .input('input_paramiter1', sql.Int, ud_id)
                    .query(`SELECT * FROM user_criteria_detail WHERE user_define_id = @input_paramiter1`)
                    
                    ud_str_or_weak  = asyncMain_2_0_assort_strength(ud_result); // console.log({result_str_or_weak: str_or_weak_or_ud}); // 강한조건, 유연조건 나누기, User_define
                    ud_query_list = asyncMain_2_1_make_query(ud_str_or_weak,1);
                    console.log({ud_query_list:ud_query_list})
                    ud_query = `SELECT distinct ud.person_id FROM (${ud_query_list[0]}) ud`

                    ud_pool.close()
                    sql.close();

                    ud_pool = await sql.connect(CdmDbConfig);
                    ud_result = await ud_pool.request()
                    .query(ud_query)
                    ud_pool.close()
                    //console.log({ud_result:ud_result.recordset})
                    ud_rows = ud_result.recordset

                    for(j=0;j<ud_rows.length;j++){
                        out.ud_inclusion_person_list= chk_person_id(out.ud_inclusion_person_list, ud_rows[j].person_id);
                    }     
                }
                ud_pool.close()
                sql.close();

                ud_pool = await sql.connect(dbConfig);
                //Exclusion
                for(i=0;i<in_ex.ex.length;i++){
                    ud_id = in_ex.ex[i].criteria_detail_value
                    ud_result = await ud_pool.request()
                    .input('input_paramiter1', sql.Int, ud_id)
                    .query(`SELECT * FROM user_criteria_detail WHERE user_define_id = @input_paramiter1`)
                    
                    ud_str_or_weak  = asyncMain_2_0_assort_strength(ud_result); // console.log({result_str_or_weak: str_or_weak_or_ud}); // 강한조건, 유연조건 나누기, User_define
                    ud_query_list = asyncMain_2_1_make_query(ud_str_or_weak,1);
                    //console.log({ud_query_list:ud_query_list})
                    ud_query = `SELECT distinct ud.person_id FROM (${ud_query_list[0]}) ud`

                    ud_pool.close()
                    sql.close();

                    ud_pool = await sql.connect(CdmDbConfig);
                    ud_result = await ud_pool.request()
                    .query(ud_query)
                    ud_pool.close()
                    //console.log({ud_result:ud_result.recordset})
                    ud_rows = ud_result.recordset

                    for(j=0;j<ud_rows.length;j++){
                        out.ud_exclusion_person_list= chk_person_id(out.ud_exclusion_person_list, ud_rows[j].person_id);
                    }     
                }

               // console.log({out:out.ud_exclusion_person_list.length});
                ud_pool.close()
                sql.close()

                //선정 - 비선정 구하기 
                let idx =-1;
                for(i=0;i<out.ud_exclusion_person_list.length;i++){
                    idx = out.ud_inclusion_person_list.indexOf(out.ud_exclusion_person_list[i])
                    if(idx!=-1){
                        out.ud_inclusion_person_list = out.ud_inclusion_person_list.splice(idx,1);
                    }
                    idx = -1;
                }
                //console.log({out:out});
                return out;
            }catch(err){
                console.log({'err_2-2.asyncMain_2_0_1_list_of_user_define':err})
                //ud_pool.close()
                sql.close()
            }
           function chk_person_id (arr, person_id){
                if(arr.indexOf(person_id)==-1){
                    arr.push(person_id);
                };
                return arr;
           }
        } // asyncMain_2_0_1_list_of_user_define

        function asyncMain_2_1_make_query(rows_str_weak, condition){
            util_console_print('------asyncMain_2_1_make_query------'); // console.log(rows_str_weak);
            // 강한 조건에서 다시 조건 하나를 넣어준다. 
            // 조건이 넣어지면 다시 선정 제외조건을 나눠서 쿼리문을 만들어준다. 
            // 강한 조건이 끝나면 다시 약한 조건으로 하나씩 쿼리를 만든다. 
            let in_rows=[], ex_rows=[];
            let query_list =[]; 
            let inclusion='' , exclusion='';
            let visit ='';
            let buf_row;

            // visit 쿼리 만들기
                                // //visit
                                // case 4:
                                // result.visit.push (`( SELECT person_id FROM VISIT_OCCURRENCE WHERE person_id >0 ${val} )`);
                                // break;
            if(rows_str_weak.visit!=null && rows_str_weak.visit.length!=0){
                buf_row = rows_str_weak.visit;
                table = buf_row[0].criteria_detail_table; //0
                field = buf_row[0].criteria_detail_attribute; //0
                con = buf_row[0].criteria_detail_condition;
                val = buf_row[0].criteria_addition;
                title = buf_row[0].criteria_title;
              
                visit = `SELECT distinct person_id FROM VISIT_OCCURRENCE WHERE person_id >0  ${val} `
            }
            //console.log({rows_str_weak:rows_str_weak})
            rows_str_weak = {
                strength : rows_str_weak.strength,
                weak : rows_str_weak.weak
            }
           
            //강한조건부터
            let rows = rows_str_weak.strength;          //console.log({rows_count:rows.length});
            for(k=0;k<rows.length;k++){
                //console.log({k:k});
                if(rows[k].criteria_state == 1 && rows[k].criteria_detail_table !=4){
                    in_rows.push(rows[k]);
                }else if (rows[k].criteria_state != 1 && rows[k].criteria_detail_table !=4){
                    ex_rows.push(rows[k]);
                }
                //console.log({in_rows:in_rows.length, ex_rows:ex_rows.length});
                inclusion = make_query_0(in_rows,1);              // 선정 1, 제외 0 
                exclusion = make_query_0(ex_rows,0);              //console.log({for: {in:in_rows.length, ex:ex_rows.length}});

                //condition =1은 user_define 
                if(inclusion.length != 0 && condition !=1){
                    query_list.push(make_query_1(inclusion, exclusion, visit));
                }else if(k+1 == rows.length){
                    query_list.push(make_query_1(inclusion, exclusion, visit));
                }
                
            }
            
            //약한조건을 한 개씩 추가해보기
            rows = rows_str_weak.weak;
            //console.log(rows);
            let buf_in_rows =[], buf_ex_rows = [];
            
            for(k=0;k<rows.length;k++){
                //console.log(rows[k]);
                buf_in_rows = JSON.parse(JSON.stringify(in_rows));
                buf_ex_rows = JSON.parse(JSON.stringify(ex_rows));
                if(rows[k].criteria_state == 1 && rows[k].criteria_detail_table !=4){
  
                    buf_in_rows.push(rows[k]);
                }else if (rows[k].criteria_state != 1 && rows[k].criteria_detail_table !=4){
                    buf_ex_rows.push(rows[k]);
                }
                inclusion = make_query_0(buf_in_rows,1); // 선정 1, 제외 0 
                exclusion = make_query_0(buf_ex_rows,0);              //console.log({for: {in:in_rows.length, ex:ex_rows.length}});
                

                if(inclusion.length != 0 ){
                    query_list.push(make_query_1(inclusion, exclusion, visit));
                }
            }

            return query_list;
            
        }
        function make_query_0(rows, in_or_ex) {
            //util_console_print('----make_query_0--in_or_ex: '+in_or_ex+'--')
            var result = {
                person: [],
                drug: [],
                condition: [],
                measurement: [],
                user_define : []
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
            var select_lms_atc = ['FIRST','SECOND','THIRD','FORTH','FIFTH'];
            // var cdm={"table":
            //     [
            //         {"tableName":"person","fieldSet":["gender_concept_id","year_of_birth"]},
            //         {"tableName":"drug_exposure","fieldSet":["drug_concept_id","drug_exposure_start_date","drug_exposure_end_date","drug_source_value"]},
            //         {"tableName":"condition_occurrence","fieldSet":["condition_concept_id","condition_start_date","condition_end_date"]},
            //         {"tableName":"measurement","fieldSet":["measurement_concept_id","measurement_date","value_as_number"]},
            //         {"tableName":"visit_occurence","fieldSet":["visit_start_date","visit_type_concept_id"]}
            //     ]}

            for (i = 0; i < rows.length; i++) {
                table = rows[i].criteria_detail_table; //0
                field = rows[i].criteria_detail_attribute; //0
                con = rows[i].criteria_detail_condition;
                val = rows[i].criteria_detail_value;
                title = rows[i].criteria_title;
                if(rows[i].criteria_addition.length >0 && rows[i].criteria_addition!='null'){
                    val += " " +rows[i].criteria_addition;
                }
                c_add = rows[i].criteria_addition;
                c_id = rows[i].criteria_id;
                //console.log("SELECT person_id FROM "+ cdm[table].tableName + " WHERE "+cdm[table].fieldSet[field]+" "+condition[con]+ " " + val );
                //condition확인
                //console.log(table);
                switch (Number(table)) {
                    //person
                    case 0:
                        if(field==0){ //0 젠더
                            result.person.push("( SELECT person_id, provider_id FROM person WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                        }else{
                            result.person.push("( SELECT person_id, provider_id FROM person WHERE person_id  >0 " +rows[i].criteria_addition+ " )");
                        }
                        
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
                    q_m3='';
                    if(rows[i].measurement_count!=''||rows[i].measurement_method!=''){
                        q_m3 = "( SELECT person_id, provider_id FROM measurement WHERE person_id IN (  ( SELECT person_id FROM ( SELECT A.person_id  "+ measurement_method2( rows[i].measurement_method) +" FROM ( SELECT ROW_NUMBER() OVER(partition by person_id ORDER BY measurement_date DESC ) AS num, person_id, measurement_date, value_as_number FROM dbo.MEASUREMENT WHERE measurement_concept_id = "+ rows[i].criteria_detail_value + " "+ measurement_date(rows[i].date_addition_value)+ " ) A " +measurement_num(rows[i].measurement_count )+ " GROUP BY A.person_id ) B "+measurement_min_max(rows[i].value_addition_min, rows[i].value_addition_max,rows[i].min_condition, rows[i].max_condition) +")))"
                        
                    }else{
                        q_m3 = "( SELECT person_id, provider_id FROM measurement WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val +")";
                        
                    }
                    result.measurement.push(q_m3);
                        
                        break;
                    //mtdb 약2
                    case 5:
                        // SELECT concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE SMALL = "%Gout%" 
                        result.drug.push(`( SELECT person_id, provider_id FROM drug_exposure WHERE drug_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE ${select_lms_atc[field-1]} LIKE '%${title}%') ${c_add})`);
                        // console.log({c_add:rows[i]})
                        
                        
                        
                        break;

                    //mtdb 병력
                    case 6:
                        // SELECT concept_id FROM mtdb.dbo.icd10_snomed WHERE SMALL = "%Gout%" 
                        result.condition.push(`( SELECT person_id, provider_id FROM  CONDITION_OCCURRENCE WHERE condition_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.icd10_snomed WHERE ${select_lms[field-1]} LIKE '%${title}%')  ${c_add})`);
                        // console.log({c_add:rows[i]})
                        
                        
                        
                        break;
                }// switch
                
                function measurement_method2(method){
                    if(method.length!=0){
                        return `, ${method}(A.value_as_number) as value_as_number `;
                    }
                    return '';
                }

                function measurement_num(num){
                    if(num.length==0||num==null){
                        return '';
                    }else{
                        return "WHERE num < " + num;
                    }
                }

                function measurement_date(value_date){
                   // console.log("hihi"+value_date.length);
                    if(value_date.length==0){
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
                   //console.log({in:min,max,min_con, max_con});
                    if(min!=''){
                        out += " WHERE B.value_as_number "+ measurement_condition(min_con) + " " +min ;
                    }
                    if(max!=''){
                        if(out.length!=0){
                            out += " and  B.value_as_number " + measurement_condition(max_con) + " "+ max;
                        }else{
                            out += " B.value_as_number " + measurement_condition(max_con) + " "+ max;
                        }
                        
                    }
                    return out;
                }// measurement_min_max

                function measurement_condition(con){
                    switch(con){
                        case 'more than or equal':
                        return '>=';
                        
                        case 'more than':
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
        function make_query_1(inclusion, exclusion,visit){
            //util_console_print('-----make_query_1-----');
            var query;
            if (inclusion.length == 0 || exclusion == 0) {
                query = "SELECT DISTINCT A.person_id FROM ( " + inclusion + " ) A  "
            } else {
                query = "SELECT DISTINCT A.person_id FROM  ( " + inclusion + " ) A LEFT JOIN ( " + exclusion + " ) B ON A.person_id = B.person_id WHERE B.person_id IS NULL"
            }

             // visit 적용
            if(visit.length!=0){
                query = `SELECT F.person_id FROM ( ${query} ) F WHERE F.person_id in ( ${visit} )`;
            }
            return query;
        }
        function make_query_insert_1(result, study_id, query_id){
            console.log({result:result})
            var q_execute = []; var q_cnt = 0; // 1000개 이상일 경우 
            if (result.recordset != null) {

                let rows = result.recordset
                //als;jdfl;kasjflkas;lfajskfjasd;lfsa;lflds;af;alksdjflksd;dlskflksd;fjd
                //rows = result_m;
                console.log("query result: " + rows.length);
                //결과값 0 체크
                if (rows.length != 0) {
                    var q_list = [];
                    var chk_num = -1;
                    
                    //make_query_execute2(rows);
                    // --원본--
                    console.log("q_execute start")

                    for (i = 0; i < rows.length; i++) {
                            q_execute.push("( "+rows[i].person_id+", "+study_id+","+query_id+" )");
                    }
                    //console.log("q_execute");
                    //console.log(q_execute)
                    //res.send(q_execute);
                    return q_execute;
                }else {
                    console.log({ result: 0 })
                }
                //console.log(err);
            } else {
                console.log(err);
                //res.send(err);
            }
        }//make_query_insert_1
        function make_query_insert_5(q_execute){
            var cnt_1000 = 0;
            var query = "";
            var q_final = [];
            //console.log(q_execute);
            for (i = 0; i < q_execute.length; i++) {
                query += q_execute[i];
                //console.log(q_execute[i]);
                cnt_1000 += 1;
                if (cnt_1000 + 1 == 1000) {
                    q_final.push("INSERT INTO protocol_result(person_id, study_id, query_id) VALUES " + query + " ;");
                    cnt_1000 = 0;
                    query = "";
                } else {
                    if (i + 1 != q_execute.length) {
                        query += ", ";
                    }
                }
            }
    
            q_final.push("INSERT INTO protocol_result(person_id, study_id, query_id) VALUES " + query + " ;");
            //res.send(q_final);
            console.log("q_final.length: " + q_final.length);
            q_list1 = "";
            for (i = 0; i < q_final.length; i++) {
                q_list1 += q_final[i];
            }
            return q_list1;
        }//make_query_insert_5
        function pad(n, width) {
            n = n + '';
            return n.length >= width ? n : new Array(width - n.length + 1).join('0') + n;
          }
    }//async

    // patient
    async function asyncMain_patient(study_id, query_id) {
        console.log(`--------patient_부분----input: ${study_id}, ${query_id}`);

        try{
            
            let pool =  await sql.connect(dbConfig); //scrn_cloud
           //console.log(study_id, query_id);
            //0. provider_id 가져오기
            console.log('0.get provider_id  ');
            let result = await pool.request() 
            .input('input_paramiter1', sql.Int, study_id)
            .query(`SELECT invest_num FROM study  WHERE study_id = @input_paramiter1`)
            //.query(`SELECT invest_num FROM patient_study  WHERE study_id = @input_paramiter1`)
            //console.log(`UPDATE protocol SET protocol_status = N'InProgress' WHERE protocol_id = @input_paramiter`);
            provider_id =result.recordset[0].invest_num;
            util_console_print({"1.상태 업데이트": result});            // res.send({resunt:{provider_id:provider_id}}); sql.close();    
   
            // 1.실행 버튼 클릭 시 '진행' 상태 변경하기.
            console.log('1.상태 업데이트_생략')
 
           
            // 2. 조건 쿼리 받아와서 쿼리문 만들기.
            console.log('2. 조건쿼리 받아서 쿼리문 만들기');
            result = await pool.request()
            .input('input_paramiter1', sql.Int, study_id)
            .input('input_paramiter2', sql.Int, query_id)
            .query(`SELECT * FROM criteria_detail WHERE study_id = @input_paramiter1 and query_id = @input_paramiter2`)
            util_console_print({"2. 조건쿼리 받아서 쿼리문 만들기":result.recordset.length});



            var str_or_weak_or_ud  = asyncMain_2_0_assort_strength(result);  //console.log({result_str_or_weak: str_or_weak_or_ud}); // 강한조건, 유연조건 나누기, User_define
            sql.close();
            //console.log({str_or_weak_or_ud:str_or_weak_or_ud})

            // res.send(str_or_weak_or_ud);
            // return 0
            // out ={inclustion_list [], exclusion_list[]}
            var user_define_list = await asyncMain_2_0_1_list_of_user_define(str_or_weak_or_ud.user_define);
            //console.log({user_define_list:user_define_list});
            var query_list = asyncMain_2_1_make_query(str_or_weak_or_ud,0, provider_id);

            // 3. 마지막쿼리 돌려서 결과값 넣기.
            console.log("----3번시작")
            sql.close();
            pool =  await sql.connect(CdmDbConfig); //scrn_cloud
            var query_result =[];
            console.log({query_list:query_list})
            
            ////////////////////////////////////////
            //User_Define 적용하기
            let buf =''
            let query_3 = `SELECT DISTINCT * FROM (${query_list[query_list.length-1]}) q3 `;
            let out = user_define_list
            if(out.ud_inclusion_person_list.length!=0){
                for(i=0;i<out.ud_inclusion_person_list.length;i++){
                    buf += out.ud_inclusion_person_list[i]
                    if(i+1 != out.ud_inclusion_person_list.length){
                        buf += ", ";
                    }
                }
                query_3 += `WHERE person_id in (${buf}) `;
                
                buf ='';

                if(out.ud_exclusion_person_list.length!=0){
                    for(i=0;i<out.ud_exclusion_person_list.length;i++){
                        buf += out.ud_exclusion_person_list[i]
                        if(i+1 != out.ud_exclusion_person_list.length){
                            buf += ", ";
                        }
                    }
                    query_3 += `and person_id not in (${buf}) `;
                }
            }else{
                if(out.ud_exclusion_person_list.length!=0){
                    for(i=0;i<out.ud_exclusion_person_list.length;i++){
                        buf += out.ud_exclusion_person_list[i]
                        if(i+1 != out.ud_exclusion_person_list.length){
                            buf += ", ";
                        }
                    }
                    query_3 += `WHERE person_id not in (${buf}) `;
                }
            }

            // SQL문 적용하기 
            // 1. 포함 비포함 분류하기. 
            // 2. 포함 비포함에 맞는 sql문 적용하기. 
            let sql_in_ex = {
                in :[],
                ex : []
            }
            if(str_or_weak_or_ud.sql.length!=0){
                //1. 분류하기.
                for(i=0;i<str_or_weak_or_ud.sql.length;i++){
                    if(str_or_weak_or_ud.sql[i].criteria_state=1){
                        sql_in_ex.in.push(str_or_weak_or_ud.sql[i]);
                    }else{
                        sql_in_ex.ex.push(str_or_weak_or_ud.sql[i]);
                    }
                }

                //2. sql문 적용하기
                //inclusion.
                if(sql_in_ex.in.length!=0){
                    if(sql_in_ex.ex.length!=0){
                        query_3 = ` SELECT DISTINCT * FROM (${query_3}) sql WHERE person_id in (${sql_in_ex.in[0].criteria_addition}) and person_id not in (${sql_in_ex.ex[0].criteria_addition}) `;
                    }else{
                        query_3 = ` SELECT DISTINCT * FROM (${query_3}) sql WHERE person_id in (${sql_in_ex.in[0].criteria_addition})`;
                    }
                }else{
                    if(sql_in_ex.ex.length!=0){
                        query_3 = ` SELECT DISTINCT * FROM (${query_3}) sql WHERE person_id not in (${sql_in_ex.ex[0].criteria_addition}) `;
                    }
                }             
            }//if(str_or_weak_or_ud.sql.length!=0)
            ///////////////////////////////////

            //최종 쿼리 실행
            console.log({query_3 :query_3});
            result = await pool.request()
            .query(query_3);

            //4. 기존 쿼리 결과 지우기
            //change dbconfig
            sql.close();
            pool =  await sql.connect(dbConfig); //scrn_cloud
            console.log("----4번시작");
            console.log({result:result})
            var result_3 = make_query_insert_1(result,study_id, query_id, provider_id);     //     res.send({resunt:{result_3:result_3}}); // sql.close();  
            
            q = "DELETE FROM patient_result WHERE study_id = " + study_id + " and query_id = " + query_id + ` AND provider_id = ${provider_id} `; //console.log(q)
            console.log({DELETE_QUERY:q});
            result = await pool.request()
            .query(q);
            util_console_print({"4. 기존쿼리결과지우기":result});
            
            //5. 쿼리 삽입하기.
            console.log("-Paitent---5번시작");
            console.log({result_3:result_3})
            var query_5 = make_query_insert_5(result_3);
            console.log({insert_query: query_5});
            result = await pool.request()
            .query(query_5);
            

            console.log('6.상태 업데이트_생략')


        // res.send({resunt:{result:result}});
        sql.close();    
        }catch(err){
            util_console_print({error: err});
            //res.send({error:'2.result is 0'});

            sql.close();
        }
       

        function asyncMain_2_0_assort_strength(result){
            var out = {
                strength : [], 
                weak : [],
                visit:[], 
                user_define:[], 
                sql:[], 
            }
            if(result.rowsAffected[0]==0){
                throw new Error(result);
            }
            rows = result.recordset;
            for (i = 0; i < rows.length; i++) {
                if (rows[i].criteria_detail_table != 4 && rows[i].criteria_detail_table != 9 && rows[i].criteria_detail_table != 7 ) {
                    if ((rows[i].strength == 1 || rows[i].strength == null)) {
                        out.strength.push(rows[i]);
                    } else {
                        out.weak.push(rows[i]);
                    }
                } else {
                    if (rows[i].criteria_detail_table == 4) {
                        out.visit.push(rows[i]);
                    }
                    if (rows[i].criteria_detail_table == 9) {
                        out.user_define.push(rows[i]);
                    }
                    if( rows[i].criteria_detail_table == 7){
                        out.sql.push(rows[i]);
                    }
                }
            }//for
            return out;;

        }
        //User_Define 내용 만들기.
        async function asyncMain_2_0_1_list_of_user_define(rows_user_define){
            console.log("2-1.asyncMain_2_0_1_list_of_user_define");
            let out ={
                ud_inclusion_person_list :[],
                ud_exclusion_person_list : []
            }
            let rows_us;
            if(rows_user_define.length==0){
                return out;
            }else{
                rows_us = JSON.parse(JSON.stringify(rows_user_define));
            }
           // console.log({rows_us:rows_us});

            //console.log({rows_user_define:rows_user_define});
            // rows[] userdefide 관련 내용 ->  값 가지고와서 쿼리문 만들어야함
            // 1. Inclusion, Exclusion 분류
            // 2. 분류된 내용으로 다시 실제 조건 쿼리 만들고 리스트 함수에 각각 넣기. 
            // 3. 리턴

            

            let in_ex ={
                in : [],
                ex : []
            }
            for(i=0;i<rows_us.length;i++){
                //console.log({rows_i : rows_us[i]});
                if(rows_us[i].criteria_state==1){
                    in_ex.in.push(rows_us[i]);
                }else{
                    in_ex.ex.push(rows_us[i]);
                }
            }//for
            //console.log({in_ex:in_ex})
            // inclusion
            console.log('2-2.asyncMain_2_0_1_list_of_user_define');

            try{
                
                let ud_pool = await sql.connect(dbConfig);
                let ud_id =0;
                let ud_result;
                var str_or_weak;
                let query_list_ud
                //Inclusion
                for(i=0;i<in_ex.in.length;i++){
                    ud_id = in_ex.in[i].criteria_detail_value
                    ud_result = await ud_pool.request()
                    .input('input_paramiter1', sql.Int, ud_id)
                    .query(`SELECT * FROM user_criteria_detail WHERE user_define_id = @input_paramiter1`)
                    
                    ud_str_or_weak  = asyncMain_2_0_assort_strength(ud_result); // console.log({result_str_or_weak: str_or_weak_or_ud}); // 강한조건, 유연조건 나누기, User_define
                    ud_query_list = asyncMain_2_1_make_query(ud_str_or_weak,1);
                    //console.log({ud_query_list:ud_query_list})
                    ud_query = `SELECT distinct ud.person_id FROM (${ud_query_list[0]}) ud`

                    ud_pool.close()
                    sql.close();

                    ud_pool = await sql.connect(CdmDbConfig);
                    ud_result = await ud_pool.request()
                    .query(ud_query)
                    ud_pool.close()
                    //console.log({ud_result:ud_result.recordset})
                    ud_rows = ud_result.recordset

                    for(j=0;j<ud_rows.length;j++){
                        out.ud_inclusion_person_list= chk_person_id(out.ud_inclusion_person_list, ud_rows[j].person_id);
                    }     
                }
                ud_pool.close()
                sql.close();

                ud_pool = await sql.connect(dbConfig);
                //Exclusion
                for(i=0;i<in_ex.ex.length;i++){
                    ud_id = in_ex.ex[i].criteria_detail_value
                    ud_result = await ud_pool.request()
                    .input('input_paramiter1', sql.Int, ud_id)
                    .query(`SELECT * FROM user_criteria_detail WHERE user_define_id = @input_paramiter1`)
                    
                    ud_str_or_weak  = asyncMain_2_0_assort_strength(ud_result); // console.log({result_str_or_weak: str_or_weak_or_ud}); // 강한조건, 유연조건 나누기, User_define
                    ud_query_list = asyncMain_2_1_make_query(ud_str_or_weak,1);
                    //console.log({ud_query_list:ud_query_list})
                    ud_query = `SELECT distinct ud.person_id FROM (${ud_query_list[0]}) ud`

                    ud_pool.close()
                    sql.close();

                    ud_pool = await sql.connect(CdmDbConfig);
                    ud_result = await ud_pool.request()
                    .query(ud_query)
                    ud_pool.close()
                    //console.log({ud_result:ud_result.recordset})
                    ud_rows = ud_result.recordset

                    for(j=0;j<ud_rows.length;j++){
                        out.ud_exclusion_person_list= chk_person_id(out.ud_exclusion_person_list, ud_rows[j].person_id);
                    }     
                }

               // console.log({out:out.ud_exclusion_person_list.length});
                ud_pool.close()
                sql.close()

                //선정 - 비선정 구하기 
                let idx =-1;
                for(i=0;i<out.ud_exclusion_person_list.length;i++){
                    idx = out.ud_inclusion_person_list.indexOf(out.ud_exclusion_person_list[i])
                    if(idx!=-1){
                        out.ud_inclusion_person_list = out.ud_inclusion_person_list.splice(idx,1);
                    }
                    idx = -1;
                }
                //console.log({out:out});
                return out;
            }catch(err){
                console.log({'err_2-2.asyncMain_2_0_1_list_of_user_define':err})
                ud_pool.close()
                sql.close()
            }
           function chk_person_id (arr, person_id){
                if(arr.indexOf(person_id)==-1){
                    arr.push(person_id);
                };
                return arr;
           }
        } // asyncMain_2_0_1_list_of_user_define


        function asyncMain_2_1_make_query(rows_str_weak, condition, provider_id){
             //util_console_print('------asyncMain_2_1_make_query------'); // console.log(rows_str_weak);
            // 강한 조건에서 다시 조건 하나를 넣어준다. 
            // 조건이 넣어지면 다시 선정 제외조건을 나눠서 쿼리문을 만들어준다. 
            // 강한 조건이 끝나면 다시 약한 조건으로 하나씩 쿼리를 만든다. 
            let in_rows=[], ex_rows=[];
            let query_list =[]; 
            let inclusion='' , exclusion='';
            let visit ='';
            let buf_row;
           
            // break;
            if(rows_str_weak.visit!=null && rows_str_weak.visit.length!=0){
                buf_row = rows_str_weak.visit;
                table = buf_row[0].criteria_detail_table; //0
                field = buf_row[0].criteria_detail_attribute; //0
                con = buf_row[0].criteria_detail_condition;
                val = buf_row[0].criteria_addition;
                title = buf_row[0].criteria_title;
                
                visit = `SELECT distinct person_id FROM VISIT_OCCURRENCE WHERE 1=1 ${val} `
            }
            //console.log({rows_str_weak:rows_str_weak})

            rows_str_weak = {
                strength : rows_str_weak.strength,
                weak : rows_str_weak.weak
            }

            //강한조건부터
            let rows = rows_str_weak.strength;          //console.log({rows_count:rows.length});
            for(k=0;k<rows.length;k++){
                //console.log({k:k});
                if(rows[k].criteria_state == 1 && rows[k].criteria_detail_table !=4){
                    in_rows.push(rows[k]);
                }else if (rows[k].criteria_state != 1 && rows[k].criteria_detail_table !=4){
                    ex_rows.push(rows[k]);
                }
                //console.log({in_rows:in_rows.length, ex_rows:ex_rows.length});
                inclusion = make_query_0(in_rows,1, provider_id);              // 선정 1, 제외 0 
                exclusion = make_query_0(ex_rows,0, provider_id);              //console.log({for: {in:in_rows.length, ex:ex_rows.length}});

                //condition =1은 user_define 
                if(inclusion.length != 0 && condition !=1){
                    query_list.push(make_query_1(inclusion, exclusion, visit));
                }else if(k+1 == rows.length){
                    query_list.push(make_query_1(inclusion, exclusion, visit));
                }
                
            }
            
            //약한조건을 한 개씩 추가해보기
            rows = rows_str_weak.weak;
            //console.log(rows);
            let buf_in_rows =[], buf_ex_rows = [];
            
            for(k=0;k<rows.length;k++){
                //console.log(rows[k]);
                buf_in_rows = JSON.parse(JSON.stringify(in_rows));
                buf_ex_rows = JSON.parse(JSON.stringify(ex_rows));
                if(rows[k].criteria_state == 1 && rows[k].criteria_detail_table !=4){
  
                    buf_in_rows.push(rows[k]);
                }else if (rows[k].criteria_state != 1 && rows[k].criteria_detail_table !=4){
                    buf_ex_rows.push(rows[k]);
                }
                inclusion = make_query_0(buf_in_rows,1, provider_id); // 선정 1, 제외 0 
                exclusion = make_query_0(buf_ex_rows,0, provider_id);              //console.log({for: {in:in_rows.length, ex:ex_rows.length}});
                

                if(inclusion.length != 0 ){
                    query_list.push(make_query_1(inclusion, exclusion, visit));
                }
            }

            return query_list;
            
        }
        function make_query_0(rows, in_or_ex, provider_id) {
            //util_console_print('----make_query_0--in_or_ex: '+in_or_ex+'--')
            var result = {
                person: [],
                drug: [],
                condition: [],
                measurement: [],
                user_define : []
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
            var select_lms_atc = ['FIRST','SECOND','THIRD','FORTH','FIFTH'];
            // var cdm={"table":
            //     [
            //         {"tableName":"person","fieldSet":["gender_concept_id","year_of_birth"]},
            //         {"tableName":"drug_exposure","fieldSet":["drug_concept_id","drug_exposure_start_date","drug_exposure_end_date","drug_source_value"]},
            //         {"tableName":"condition_occurrence","fieldSet":["condition_concept_id","condition_start_date","condition_end_date"]},
            //         {"tableName":"measurement","fieldSet":["measurement_concept_id","measurement_date","value_as_number"]},
            //         {"tableName":"visit_occurence","fieldSet":["visit_start_date","visit_type_concept_id"]}
            //     ]}

            for (i = 0; i < rows.length; i++) {
                table = rows[i].criteria_detail_table; //0
                field = rows[i].criteria_detail_attribute; //0
                con = rows[i].criteria_detail_condition;
                val = rows[i].criteria_detail_value;
                title = rows[i].criteria_title;
                if(rows[i].criteria_addition.length >0 && rows[i].criteria_addition!='null' ){
                    val += " " +rows[i].criteria_addition;
                }
                c_add = rows[i].criteria_addition;

                c_id = rows[i].criteria_id;
                //console.log("SELECT person_id FROM "+ cdm[table].tableName + " WHERE "+cdm[table].fieldSet[field]+" "+condition[con]+ " " + val );
                //condition확인
                //console.log(table);
                switch (Number(table)) {
                    //person
                    case 0:
                        if(field==0){ //0 젠더
                            result.person.push("( SELECT person_id, provider_id FROM person WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                        }else{
                            result.person.push("( SELECT person_id, provider_id FROM person WHERE person_id  >0 " +rows[i].criteria_addition+ " )");
                        }
                        
                        break;
                    //drug
                    case 1:
                        result.drug.push("( SELECT person_id, provider_id FROM drug_exposure WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                        break;
                    //condition
                    case 2:
                        // result.condition.push("( SELECT person_id, provider_id FROM condition_occurrence WHERE provider_id = "+provider_id+" and " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                        q_condition = "( SELECT person_id, provider_id,  FROM condition_occurrence WHERE "+cdm[table].fieldSet[field] + " " + condition[con] +" "+ val + " )"
                        q_condition = 
                        result.condition.push();
                        break;
                    //measurement
                    case 3:
                    if(rows[i].measurement_count!=''||rows[i].measurement_method!=''){
                        result.measurement.push("( SELECT person_id, provider_id FROM measurement WHERE person_id IN (  ( SELECT person_id FROM ( SELECT A.person_id  "+ measurement_method2( rows[i].measurement_method) +" FROM ( SELECT ROW_NUMBER() OVER(partition by person_id ORDER BY measurement_date DESC ) AS num, person_id, measurement_date, value_as_number FROM dbo.MEASUREMENT WHERE measurement_concept_id = "+ rows[i].criteria_detail_value + " "+ measurement_date(rows[i].date_addition_value)+ " ) A " +measurement_num(rows[i].measurement_count )+ " GROUP BY A.person_id ) B "+measurement_min_max(rows[i].value_addition_min, rows[i].value_addition_max,rows[i].min_condition, rows[i].max_condition) +")))" );
                    }else{
                        result.measurement.push("( SELECT person_id, provider_id FROM measurement WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val +")");
                    }
                        
                        break;
                    //mtdb 약2
                    case 5:
                        // SELECT concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE SMALL = "%Gout%" 
                        result.drug.push(`( SELECT person_id, provider_id FROM drug_exposure WHERE drug_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE ${select_lms_atc[field-1]} LIKE '%${title}%')  ${c_add})`);
                        // console.log({c_add:rows[i]})
             
                        break;

                    //mtdb 병력
                    case 6:
                        // SELECT concept_id FROM mtdb.dbo.icd10_snomed WHERE SMALL = "%Gout%" 
                        result.condition.push(`( SELECT person_id, provider_id FROM  CONDITION_OCCURRENCE WHERE condition_concept_id IN ( SELECT distinct concept_id FROM mtdb.dbo.icd10_snomed WHERE ${select_lms[field-1]} LIKE '%${title}%')  ${c_add})`);
                        // console.log({c_add:rows[i]})
                        
                        
                        
                        break;
                }// switch
                
                function measurement_method2(method){
                    if(method.length!=0){
                        return `, ${method}(A.value_as_number) as value_as_number `;
                    }
                    return '';
                }

                function measurement_num(num){
                    if(num.length==0||num==null){
                        return '';
                    }else{
                        return "WHERE num < " + num;
                    }
                }

                function measurement_date(value_date){
                   // console.log("hihi"+value_date.length);
                    if(value_date.length==0){
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
                   //console.log({in:min,max,min_con, max_con});
                    if(min!=''){
                        out += " WHERE B.value_as_number "+ measurement_condition(min_con) + " " +min ;
                    }
                    if(max!=''){
                        if(out.length!=0){
                            out += " and  B.value_as_number " + measurement_condition(max_con) + " "+ max;
                        }else{
                            out += " B.value_as_number " + measurement_condition(max_con) + " "+ max;
                        }
                        
                    }
                    return out;
                }// measurement_min_max

                function measurement_condition(con){
                    switch(con){
                        case 'more than or equal':
                        return '>=';
                        
                        case 'more than':
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
        function make_query_1(inclusion, exclusion, visit){
            util_console_print('-----make_query_1-----');
            var query;
            if (inclusion.length == 0 || exclusion == 0) {
                query = "SELECT provider_id, A.person_id, A.year_of_birth, A.gender_concept_id FROM ( " + inclusion + " ) A  "
            } else {
                query = "SELECT provider_id, A.person_id, A.year_of_birth, A.gender_concept_id FROM ( " + inclusion + " ) A LEFT JOIN ( " + exclusion + " ) B ON A.person_id = B.person_id WHERE B.person_id IS NULL"
            }
            // visit 적용
            if(visit.length!=0){
                query = `SELECT DISTINCT F.provider_id, F.person_id, F.year_of_birth, F.gender_concept_id FROM ( ${query} ) F WHERE F.person_id in ( ${visit} )`;
            }

            return query;
        }
        function make_query_insert_1(result, study_id, query_id, provider_id){
            var q_execute = []; var q_cnt = 0; // 1000개 이상일 경우 
            if (result != null) {
                let rows = result.recordset
                //als;jdfl;kasjflkas;lfajskfjasd;lfsa;lflds;af;alksdjflksd;dlskflksd;fjd
                //rows = result_m;
                console.log("query result: " + rows.length);
                //결과값 0 체크
                if (rows.length != 0) {
                    var q_list = [];
                    var chk_num = -1;
                    
                    //console.log(q_list);
                    //res.send(q_list);


                    //make_query_execute2(rows);
                    // --원본--
                    console.log("q_execute start")

                    for (i = 0; i < rows.length; i++) {
                            q_execute.push("( " + study_id + ", " + query_id + ", " +rows[i].person_id+ ", " + rows[i].year_of_birth + ", " +rows[i].gender_concept_id + "," + rows[i].provider_id+ ")");
                    }
                    //console.log("q_execute");
                    //console.log(q_execute)
                    //res.send(q_execute);
                    return q_execute;
                } else {
                    console.log({ result: 0 })
                    return q_execute;
                }
                //console.log(err);
            } else {
                console.log(err);
                //res.send(err);
            }

        }//make_query_insert_1
        function make_query_insert_5(q_execute) {
            var cnt_1000 = 0;
            var query = "";
            var q_final = [];
            q_list1 = "";
            //console.log(q_execute);
            if (q_execute.length != 0) {
                for (i = 0; i < q_execute.length; i++) {
                    query += q_execute[i];
                    //console.log(q_execute[i]);
                    cnt_1000 += 1;
                    if (cnt_1000 + 1 == 1000) {
                        q_final.push("INSERT INTO patient_result(study_id, query_id, patient_id, patient_age, patient_gender, provider_id) VALUES " + query + " ;");
                        cnt_1000 = 0;
                        query = "";
                    } else {
                        if (i + 1 != q_execute.length) {
                            query += ", ";
                        }

                    }
                }

                q_final.push("INSERT INTO patient_result(study_id, query_id, patient_id, patient_age, patient_gender, provider_id) VALUES " + query + " ;");

                //res.send(q_final);
                console.log("q_final.length: " + q_final.length);
               for (i = 0; i < q_final.length; i++) {
                    q_list1 += q_final[i];
                }
            }

            return q_list1;
        }//make_query_insert_5

    }//async
    async function count_start(study_id, query_id,res){
        //site 실행
        //console.log(site);
        try{
            await site_execute(study_id, query_id, res);
            //res.send('ok');
        }catch(err){
            console.log({start_err: err});
        }
        
        console.log("-----------------------------------End--------------------------------------------");
        console.log();
    };



});

///wijmo
app.post('/test-protocol-advanced/:study_id/:query_id',(req,res)=>{
    console.log('------------wijmo----------------')
    console.log({req:req.params.protocol_id});
    var study_id = req.params.study_id;
    var query_id = req.params.query_id;
    

    start(study_id,query_id);

    async function start(study_id,query_id){
        try{
            let out={
                person : [], 
                lab : []                
            };
            let pool =  await sql.connect(dbConfig); //scrn_cloud
            let result = await pool.request() 
             .input('input_paramiter1', sql.Int, study_id)
             .input('input_paramiter2', sql.Int, query_id)
             .query(`SELECT DISTINCT person_id FROM protocol_result  WHERE study_id = @input_paramiter1 and query_id = @input_paramiter2`)
            pool.close();
            sql.close();


            if(result !=null){
                let rows = result.recordset;
                var personList = person_set_by_rows_string(rows);
                console.log({personList:personList});
                
                pool =  await sql.connect(CdmDbConfig); //scrn_cloud
                
                console.log("0")
                result = await pool.request() 
                .query(`SELECT person_id, gender_concept_id, year_of_birth FROM person  WHERE person_id in ( ${personList} )`)
                console.log("1");
                if(result != null)
                {
                    rows = result.recordset
                    console.log("pool2_query_result: " + rows.length);
                    var set = person_set_by_rows(rows);
                    out.person = set;
                    // res.send(set);
                    
                }
                result = await pool.request()
                .query(`SELECT MAX(measurement_date) as last_date FROM MEASUREMENT`);
                date = Month_6(result);

                q = `SELECT m1.person_id, m1.measurement_concept_id, m1.measurement_date, m1.value_as_number, c1.concept_name FROM (SELECT person_id, measurement_concept_id, measurement_date, value_as_number FROM MEASUREMENT WHERE person_id IN ( ${personList}) AND measurement_date > ${date} AND measurement_concept_id IN (3013721,4153111,44816672,3036671,3034530,3004077,46236949,40758583,3000963)) m1, concept c1 WHERE m1.measurement_concept_id = c1.concept_id ORDER BY m1.person_id ASC, m1.measurement_date ASC`
                console.log(q);
                result = await pool.request()
                .query(q);
                
                rows = result.recordset

                buf = 0;
                id = 0;
                buf = rows[0].person_id;
                rows[0].measurement_date = new Date(`'${rows[0].measurement_date}'`).format('yyyy-MM-dd');
                rows[0].value_as_number = Number(Number(rows[0].value_as_number).toFixed(2));
                rows[0].person_id = pad(id+1,4)+'환자'
                
                rows[0].ast         = 0,
                rows[0].glucose1    = 0,
                rows[0].glucose2    = 0,
                rows[0].glucose3    = 0,
                rows[0].glucose4    = 0,
                rows[0].glucose5    = 0,
                rows[0].alt         = 0,
                rows[0].hba1c       = 0
                rows[0]= wijmo_lab(rows[0].measurement_concept_id, rows[0].value_as_number, rows[0]);

                for(i=1;i<rows.length;i++){
                    if(buf != rows[i].person_id ){
                        buf = rows[i].person_id
                        id++;
                    }
                    rows[i].person_id = pad(id+1,4)+'환자'
                    rows[i].measurement_date = new Date(`'${rows[i].measurement_date}'`).format('yyyy-MM-dd');
                    rows[i].value_as_number = Number(Number(rows[i].value_as_number).toFixed(2));
                    rows[i].ast         = 0,
                    rows[i].glucose1    = 0,
                    rows[i].glucose2    = 0,
                    rows[i].glucose3    = 0,
                    rows[i].glucose4    = 0,
                    rows[i].glucose5    = 0,
                    rows[i].alt         = 0,
                    rows[i].hba1c       = 0
                    rows[i]= wijmo_lab(rows[i].measurement_concept_id, rows[i].value_as_number, rows[i]);
                }
                out.lab = rows;

                pool.close();
                sql.close();
                res.send(out);
            }else{
                //res.send(0);
                console.log({Wijmo_else:0})
            }
            
        }catch(err){
            console.log({err:err});
            //res.send(0);
            pool.close();
            sql.close();
        }finally{
            //res.send(0);
            
        }
        function wijmo_lab(concept_id,value, lab_buf){
                switch(concept_id){
                   case 3013721:
                        lab_buf.ast = value;
                   break;
                   case 4153111:
                        lab_buf.glucose1 = value;
                   break;

                   case 44816672:
                         lab_buf.glucose2 = value;
                   break;

                   case 3036671:
                         lab_buf.glucose3 = value;
                   break;

                   case 3034530:
                        lab_buf.glucose4 = value;
                   break;

                   case 3004077:
                        lab_buf.glucose5 = value;
                   break;

                   case 46236949:
                         lab_buf.alt = value;
                   break;

                   case 40758583:
                        lab_buf.hba1c = value;
                   break;

                }//switch
                return lab_buf;
        }//wijmo_lab
        function pad(n, width) {
            n = n + '';
            return n.length >= width ? n : new Array(width - n.length + 1).join('0') + n;
        }
        function Month_6(result){
            //console.log(result);
            var getdate = new Date(result.recordset[0].last_date);
            getdate =new Date(new Date(getdate).format('yyyy-MM-dd'));
            console.log({getdate:getdate});
            let c_date = new Date().setDate(getdate.getDate()-180);

            console.log({c_date:new Date(c_date).format('yyyy-MM-dd')});
            c_date = new Date(c_date).format('yyyy-MM-dd');

            console.log(c_date);
            return `'${c_date}'`;
        }

        function person_set_by_rows_string(rows){
            var obj ='';
        
            for(i=0;i<rows.length;i++){
                obj += rows[i].person_id ;
                if(i+1 != rows.length){
                    obj += ", ";
                }
            }
            return obj;
        }
        function person_set_by_rows(rows){
          
            var list =[];
            var now_year = new Date().format('yyyy');
            for(i=0;i<rows.length;i++){
                list.push(make_person_set(i, rows[i].gender_concept_id, (now_year-rows[i].year_of_birth)));
            }
            //console.log({person_set_by_rows:list});
            return list ;
        }
        function make_person_set(person_id, gender, year){
            var gen=''; 
            if(gender==8507){
                gen ="Male";
            }else{
                gen ="Female";
            }
        
            var obj ={
                id : person_id,
                gender : gen,
                age : year
            }
            return obj;
        }
    }// async function start
}) // post protocol 

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

            

        //res.send({resunt:{list_count:list_count, result_5:result_5}});
        sql.close();    
        }catch(err){
            util_console_print({error: err});
           // res.send({error:'2.result is 0'});

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
                        result.person.push("( SELECT DISTINCT person_id, provider_id FROM person WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                        break;
                    //drug
                    case 1:
                        result.drug.push("( SELECT DISTINCT person_id, provider_id FROM drug_exposure WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                        break;
                    //condition
                    case 2:
                        result.condition.push("( SELECT DISTINCT person_id, provider_id FROM condition_occurrence WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                        break;
                    //measurement
                    case 3:
                        result.measurement.push("( SELECT DISTINCT person_id, provider_id FROM measurement WHERE person_id IN (  ( SELECT person_id FROM ( SELECT A.person_id , "+ rows[i].measurement_method+"(A.value_as_number) as value_as_number FROM ( SELECT ROW_NUMBER() OVER(partition by person_id ORDER BY measurement_date DESC ) AS num, person_id, measurement_date, value_as_number FROM dbo.MEASUREMENT WHERE measurement_concept_id = "+ rows[i].criteria_detail_value + " "+ measurement_date(rows[i].date_addition_value)+ " ) A WHERE num < "+ rows[i].measurement_count + " GROUP BY A.person_id ) B "+measurement_min_max(rows[i].value_addition_min, rows[i].value_addition_max) +")))" );
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

///test-site-execute/:study_id/:query_id

// //////////Protocol
// app.get('/test-protocol-execute/:protocol_id/', (req,res)=>{
//     util_console_print("----test-protocol-execute/:protocol_id/:user_id----");
//     var protocol_id = Number(req.param('protocol_id'));
//     var query ='';
    
//     //let result ;
//     asyncMain(protocol_id);
    
//     ////Async 함수
//     async function asyncMain_protocol(protocol_id) {
//         try{
//             let pool =  await sql.connect(dbConfig); //scrn_cloud
//            //console.log(study_id, query_id);
//             // 1.실행 버튼 클릭 시 '진행' 상태 변경하기.
//             console.log('1.상태 업데이트')
//            let result = await pool.request() 
//             .input('input_paramiter1', sql.Int, protocol_id)
//             .query(`UPDATE protocol SET protocol_status = N'In Progress' WHERE protocol_id = @input_paramiter1`)
//             //console.log(`UPDATE protocol SET protocol_status = N'InProgress' WHERE protocol_id = @input_paramiter`);
//             util_console_print({"1.상태 업데이트": result});
           
//             // 2. 조건 쿼리 받아와서 쿼리문 만들기.
//             console.log('2. 조건쿼리 받아서 쿼리문 만들기');
//             result = await pool.request()
//             .input('input_paramiter1', sql.Int, protocol_id)
//             .query(`SELECT * FROM protocol_criteria_detail WHERE protocol_id = @input_paramiter1 `)
//             util_console_print({"2. 조건쿼리 받아서 쿼리문 만들기":result.recordset.length});

//             var str_or_weak_or_ud  = asyncMain_2_0_assort_strength(result);  //console.log({result_str_or_weak: str_or_weak}); // 강한조건, 유연조건 나누기. 
//             sql.close();
//             //console.log({str_or_weak_or_ud:str_or_weak_or_ud})
//             // res.send(str_or_weak_or_ud);
//             // return 0
//             // out ={inclustion_list [], exclusion_list[]}
//             var user_define_list = await asyncMain_2_0_1_list_of_user_define(str_or_weak_or_ud.user_define);


//             var query_list = asyncMain_2_1_make_query(str_or_weak_or_ud);
//             console.log({query_list:query_list});
//             // 3. 마지막쿼리 돌려서 결과값 넣기.
//             console.log("----3번시작")
//             sql.close();
//             pool =  await sql.connect(CdmDbConfig); //scrn_cloud
//             var query_result =[];
//             console.log({query_list:query_list})
            
//               ////////////////////////////////////////
//             //User_Define 적용하기
//             let buf =''
//             let query_3 = `SELECT * FROM (${query_list[query_list.length-1]}) q3 `;
//             let out = user_define_list
//             if(out.ud_inclusion_person_list.length!=0){
//                 for(i=0;i<out.ud_inclusion_person_list.length;i++){
//                     buf += out.ud_inclusion_person_list[i]
//                     if(i+1 != out.ud_inclusion_person_list.length){
//                         buf += ", ";
//                     }
//                 }
//                 query_3 += `WHERE person_id in (${buf}) `;
                
//                 buf ='';

//                 if(out.ud_exclusion_person_list.length!=0){
//                     for(i=0;i<out.ud_exclusion_person_list.length;i++){
//                         buf += out.ud_exclusion_person_list[i]
//                         if(i+1 != out.ud_exclusion_person_list.length){
//                             buf += ", ";
//                         }
//                     }
//                     query_3 += `and person_id not in (${buf}) `;
//                 }
//             }else{
//                 if(out.ud_exclusion_person_list.length!=0){
//                     for(i=0;i<out.ud_exclusion_person_list.length;i++){
//                         buf += out.ud_exclusion_person_list[i]
//                         if(i+1 != out.ud_exclusion_person_list.length){
//                             buf += ", ";
//                         }
//                     }
//                     query_3 += `WHERE person_id not in (${buf}) `;
//                 }
//             }

//             // SQL문 적용하기 
//             // 1. 포함 비포함 분류하기. 
//             // 2. 포함 비포함에 맞는 sql문 적용하기. 
//             let sql_in_ex = {
//                 in :[],
//                 ex : []
//             }
//             if(str_or_weak_or_ud.sql.length!=0){
//                 //1. 분류하기.
//                 for(i=0;i<str_or_weak_or_ud.sql.length;i++){
//                     if(str_or_weak_or_ud.sql[i].criteria_state=1){
//                         sql_in_ex.in.push(str_or_weak_or_ud.sql[i]);
//                     }else{
//                         sql_in_ex.ex.push(str_or_weak_or_ud.sql[i]);
//                     }
//                 }

//                 //2. sql문 적용하기
//                 //inclusion.
//                 if(sql_in_ex.in.length!=0){
//                     if(sql_in_ex.ex.length!=0){
//                         query_3 = ` SELECT * FROM (${query_3}) sql WHERE person_id in (${sql_in_ex.in[0].criteria_addition}) and person_id not in (${sql_in_ex.ex[0].criteria_addition}) `;
//                     }else{
//                         query_3 = ` SELECT * FROM (${query_3}) sql WHERE person_id in (${sql_in_ex.in[0].criteria_addition})`;
//                     }
//                 }else{
//                     if(sql_in_ex.ex.length!=0){
//                         query_3 = ` SELECT * FROM (${query_3}) sql WHERE person_id not in (${sql_in_ex.ex[0].criteria_addition}) `;
//                     }
//                 }             
//             }//if(str_or_weak_or_ud.sql.length!=0)
//             ///////////////////////////////////

            
            
//             //최종 쿼리 실행 
//             console.log({query_3 :query_3});
//             result = await pool.request()
//             .query(query_3);
            
//             //4. 기존 쿼리 결과 지우기
//             //change dbconfig
//             sql.close();
//             pool =  await sql.connect(dbConfig); //scrn_cloud
//             console.log("----4번시작");
//             var result_3 = make_query_insert_1(result,protocol_id);
//             q = "DELETE FROM protocol_result WHERE protocol_id = " + protocol_id ;
//             //console.log(q)
//             result = await pool.request()
//             .query(q);
//             util_console_print({"4. 기존쿼리결과지우기":result});
            
//             //5. 쿼리 삽입하기.
//             // protocol_result 환자리스트 넣기
//             console.log("----5번시작");
//             var query_5 = make_query_insert_5(result_3);
//             result = await pool.request()
//             .query(query_5);

//             //6. protocol_detail_result
//             // protocol_result 환자리스트 넣기
//             let person_list = '';

//             let drug_info = [];
//             let measurement_info = [];
//             let condition_info = [];
//             let visit_info =[];
//             let person_info =[];

//             let result_person = await pool.request()
//                 .input('input_paramiter', sql.Int, protocol_id)
//                 .query('SELECT person_id FROM dbo.protocol_result WHERE protocol_id = @input_paramiter')
//             person_list = make_list_to_string(result_person);  //console_println(person_list); //log찍기
           
//             /////BIGDATADB 접속 
//             sql.close();

//             pool =  await sql.connect(CdmDbConfig); //scrn_cloud
//             //// 환자
//             let result_person1 = await pool.request()
//                 .query('SELECT gender_concept_id, year_of_birth FROM dbo.PERSON WHERE person_id in ( '+person_list+' );')
//             person_info = make_result_person_to_list(result_person1.recordset);
            
//             var newdate = new Date();
//             var date_start = `'${newdate.getFullYear()-5}-${newdate.getMonth()+1}-${newdate.getDate()}'`;
           
//             ////약.
//             let result_drug = await pool.request()
//                 .query('SELECT DISTINCT f1.gender_concept_id, f1.drug_concept_id, f1.drug_exposure_start_date, f1.drug_exposure_end_date, f1.quantity, f1.days_supply, f1.dose_unit_source_value, c1.concept_name FROM (SELECT p1.gender_concept_id, d1.drug_concept_id, d1.drug_exposure_start_date, d1.drug_exposure_end_date, d1.quantity, d1.days_supply, d1.dose_unit_source_value FROM (SELECT person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, quantity, days_supply, dose_unit_source_value FROM dbo.DRUG_EXPOSURE WHERE dbo.DRUG_EXPOSURE.person_id in('+person_list+')) d1, dbo.PERSON p1 WHERE d1.person_id = p1.person_id and drug_exposure_start_date >= '+date_start+' ) f1, concept c1 WHERE f1.drug_concept_id = c1.concept_id ');
//             // durg 결과 내용을 리스트로 만들기.
//             drug_info = make_result_drug_to_list(result_drug.recordset);    //console_println(drug_info.length);

//             ////Lab
//             let result_measurement = await pool.request()
//                 .query('SELECT DISTINCT f1.gender_concept_id, f1.measurement_concept_id, f1.measurement_date, f1.value_as_number, f1.range_low, f1.range_high, f1.unit_source_value, c1.concept_name FROM (SELECT p1.gender_concept_id, d1.measurement_concept_id, d1.measurement_date, d1.value_as_number, d1.range_low, d1.range_high, d1.unit_source_value FROM (SELECT person_id, measurement_concept_id, measurement_date, value_as_number, range_low, range_high, unit_source_value FROM dbo.MEASUREMENT WHERE dbo.MEASUREMENT.person_id in('+person_list+')) d1, dbo.PERSON p1 WHERE d1.person_id = p1.person_id and d1.measurement_date >='+ date_start +') f1, concept c1 WHERE f1.measurement_concept_id = c1.concept_id');
//             measurement_info= make_result_measurement_to_list(result_measurement.recordset);    //console_println(measurement_info.length);

//             ////Condition
//             let result_condition = await pool.request()
//                 .query('SELECT  DISTINCT f1.gender_concept_id, f1.condition_concept_id, f1.condition_start_date, c1.concept_name  FROM (SELECT p1.gender_concept_id, d1.condition_concept_id, d1.condition_start_date FROM (SELECT person_id, condition_concept_id, condition_start_date FROM dbo.CONDITION_OCCURRENCE WHERE dbo.CONDITION_OCCURRENCE.person_id in('+person_list+')) d1, dbo.PERSON p1 WHERE d1.person_id = p1.person_id and d1.condition_start_date >= '+ date_start+') f1, concept c1 WHERE f1.condition_concept_id = c1.concept_id');
//             condition_info= make_result_condition_to_list(result_condition.recordset);  //console_println(condition_info.length);

//             ////Visit
//             let result_visit = await pool.request()
//             .query('SELECT DISTINCT p1.gender_concept_id, d1.visit_concept_id, d1.visit_start_date, d1.visit_end_date, d1.care_site_id FROM (SELECT person_id, visit_concept_id, visit_start_date, visit_end_date,care_site_id FROM dbo.VISIT_OCCURRENCE WHERE dbo.VISIT_OCCURRENCE.person_id in('+person_list+')) d1, dbo.PERSON p1 WHERE d1.person_id = p1.person_id and d1.visit_start_date >= '+date_start);
//             visit_info= make_result_visit_to_list(result_visit.recordset); //console_println(visit_info);

//             let result1 ={
//                 person_info: person_info,
//                 drug_info: drug_info,
//                 measurement_info: measurement_info,
//                 condition_info: condition_info,
//                 visit_info:visit_info
//             }; //console_println(result);

//             // 환자통계, 진단통계, 약통계, 랩통계 정리해보자.
//             // 각 내용을 JSON으로 받는다.
//             // 각 내용을 컬럼에 맞게 디비를 저장해준다.
//             let distribution_person         = make_distribution_person(result1.person_info, result1.visit_info);
//             let distribution_condition      = make_distribution_condition(result1.condition_info);
//             let distribution_drug           = make_distribution_drug(result1.drug_info);
//             let distribution_measurement    = make_distribution_measurement(result1.measurement_info);
//             ///////

//             distribution_condition.sort(function(a,b){
//                 return b[value] - a[value];
//             });
//             distribution_drug.sort(function(a,b){
//                 return b[value] - a[value];
//             });
//             distribution_measurement.sort(function(a,b){
//                 return b[value] - a[value];
//             });
            
//             if(distribution_condition.length >= 100){
//                 distribution_condition = distribution_condition.slice(0,100);
//             }
//             if(distribution_drug.length >= 100){
//                 distribution_drug = distribution_drug.slice(0,100);
//             }
//             if(distribution_measurement.length >= 100){
//                 distribution_measurement = distribution_measurement.slice(0,100);
//             }
            





//             ////SCRNDB접속
//             sql.close();
//             pool =  await sql.connect(dbConfig); //scrn_cloud
//             // 삭제
//             lp('query_delete_distribution',protocol_id)
//             let query_delete_distribution = await pool.request()
//                 .input('protocol_id',sql.Int, protocol_id)
//                 .query('DELETE FROM protocol_detail_result WHERE protocol_id = @protocol_id')
//                 console.log(query_delete_distribution);
            
//             // 삽입
//             lp('query_insert_distribution','')
//             let query_insert_distribution = await pool.request()
//                 .input('protocol_id',sql.Int, protocol_id)
//                 .input('input_person', sql.NVarChar, JSON.stringify(distribution_person))
//                 .input('input_condition', sql.NVarChar, JSON.stringify(distribution_condition))
//                 .input('input_drug', sql.NVarChar, JSON.stringify(distribution_drug))
//                 .input('input_measurement', sql.NVarChar, JSON.stringify(distribution_measurement))
//                 .query('INSERT INTO protocol_detail_result VALUES( @protocol_id, @input_person, @input_condition, @input_drug, @input_measurement)')
//                 console.log(query_insert_distribution);



//                 // 99.실행 버튼 클릭 시 '진행' 상태 변경하기.
//             console.log('99.상태 업데이트')
//             result = await pool.request() 
//              .input('input_paramiter1', sql.Int, protocol_id)
//              .query(`UPDATE protocol SET protocol_status = N'Done' WHERE protocol_id = @input_paramiter1`)
//              //console.log(`UPDATE protocol SET protocol_status = N'InProgress' WHERE protocol_id = @input_paramiter`);
//              util_console_print({"99.상태 업데이트": result});

//         //res.send({resunt:{query_insert_distribution:query_insert_distribution}});
//         sql.close();    
//         }catch(err){
//             util_console_print({error: err});
//             //res.send({error:'2.result is 0'});

//             sql.close();
//         }
//         // 필요한 내부 함수 부분입니다//
//         function make_distribution_person(person, visit){
//             console.log({visit:visit.length});
//             var age ={
//                 range_9 :0,
//                 range_10 :0,
//                 range_20 :0,
//                 range_30 :0,
//                 range_40 :0,
//                 range_50 :0,
//                 range_60 :0,
//                 range_70 :0
//             };   // 9-, 10-19, 20-29, 30~39, 40~49, 50~59, 60~69, 70~
            
//             var result_visit={
//                 emergency :0,
//                 out: 0,
//                 in : 0
//             }  ;   // [9203: 응급, 9202 :외래, 9201:입원]

//             var gender ={
//                 male :0,
//                 female :0
//             } ;   // [male, female]
//             var now_year = new Date().format('yyyy');
//             //console.log({gender: person})
//             // 나이 및 성별 분포
//             for(i=0;i<person.length;i++){
//                 //gender
               
//                 if(person[i].gender==8507){
//                     gender.male +=1;
//                 }else{
//                     gender.female +=1;
//                 }

//                 //age 
//                 buf_age = now_year - person[i].year;
//                 if(buf_age <=9 ){
//                     age.range_9 +=1;
//                 }else if (buf_age <=19){
//                     age.range_10 +=1;
//                 }else if (buf_age <=29){
//                     age.range_20 +=1;
//                 }else if (buf_age <=39){
//                     age.range_30 +=1;
//                 }else if (buf_age <=49){
//                     age.range_40 +=1;
//                 }else if (buf_age <=59){
//                     age.range_50 +=1;
//                 }else if (buf_age <=69){
//                     age.range_60 +=1;
//                 }else{
//                     age.range_70 +=1;
//                 }
//             }// end for

//             // 방문
//             for(i=0;i<visit.length;i++){
//                 switch(Number(visit[i].visit_concept_id)){
//                     case(9203):
//                     result_visit.emergency +=1;
//                         break;
//                     case(9202):
//                     result_visit.out +=1;
//                         break;
//                     case(9201):
//                     result_visit.in +=1;
//                         break;
//                 }
//             }//end for
            
//             var result ={
//                 age : age,
//                 gender : gender,
//                 visit : result_visit
//             }

//             return result;
//         } //make_distribution_person
//         function make_distribution_condition(condition_info){
//             lp("make_distribution_condition","");
//             var rows = condition_info;

//             var result_condition_name =[];
//             var result_condition_value =[];
//             var result_condition_id = [];
//             var isNull;
            
//             var result =[];
//             console.log(condition_info.length);
//             for(i=0;i<condition_info.length;i++){        
//                 //i=0
//                 if(i==0){
//                     result_condition_id.push(rows[i].condition_concept_id);
//                     result_condition_name.push(rows[i].concept_name);
//                     result_condition_value.push(1);
//                 }else{
//                     isNull = result_condition_id.indexOf(rows[i].condition_concept_id) >-1;
//                     if(!isNull){
//                         result_condition_id.push(rows[i].condition_concept_id);
//                         result_condition_name.push(rows[i].concept_name);

//                         result_condition_value.push(1);
//                     }else{
//                         result_condition_value[result_condition_id.indexOf(rows[i].condition_concept_id)] +=1
//                     }
//                 }
//             }// end for

//             // 객체에 넣기
//             var chk =0;
//             for(i=0;i<result_condition_id.length;i++){
//                 result.push(class_distribution_result(result_condition_name[i], result_condition_value[i] ));
//                 chk += result_condition_value[i];
//             }
//             console.log(chk);
//             return result;
            
//         } //make_distribution_condition
//         function make_distribution_drug(drug_info){
//             lp("make_distribution_drug","");
//             var rows = drug_info;

//             var result_drug_name =[];
//             var result_drug_value =[];
//             var result_drug_id =[];

//             var isNull;
            
//             var result =[];
//             console.log(rows.length);
//             for(i=0;i<rows.length;i++){        
//                 //i=0
//                 if(i==0){
//                     result_drug_id.push(rows[i].drug_concept_id);
//                     result_drug_value.push(1);
//                     result_drug_name.push(rows[i].concept_name)
//                 }else{
//                     isNull = result_drug_id.indexOf(rows[i].drug_concept_id) >-1;
//                     if(!isNull){
//                         result_drug_id.push(rows[i].drug_concept_id);
//                         result_drug_value.push(1);
//                         result_drug_name.push(rows[i].concept_name)
//                     }else{
//                         result_drug_value[result_drug_id.indexOf(rows[i].drug_concept_id)] +=1
//                     }
//                 }
//             }// end for

//             // 객체에 넣기
//             var chk=0; // 잘 저장됐는지 검토하기.
//             for(i=0;i<result_drug_id.length;i++){
//                 result.push(class_distribution_result(result_drug_name[i], result_drug_value[i]));
//                 chk += result_drug_value[i];
//             }
//             console.log(chk)
//             return result;

//         } //make_distribution_drug     
//         function make_distribution_measurement(measurement_info){
//             lp("make_distribution_measurement","");
//             var rows = measurement_info;

//             var result_measurement_name =[];
//             var result_measurement_value =[];
//             var result_measurement_id =[];

//             var isNull;
            
//             var result =[];
//             console.log(rows.length);
//             for(i=0;i<rows.length;i++){        
//                 //i=0
//                 if(i==0){
//                     result_measurement_id.push(rows[i].measurement_concept_id);
//                     result_measurement_value.push(1);
//                     result_measurement_name.push(rows[i].concept_name);
//                 }else{
//                     isNull = result_measurement_id.indexOf(rows[i].measurement_concept_id) >-1;
//                     if(!isNull){
//                         result_measurement_id.push(rows[i].measurement_concept_id);
//                         result_measurement_value.push(1);
//                         result_measurement_name.push(rows[i].concept_name);

//                     }else{
//                         result_measurement_value[result_measurement_id.indexOf(rows[i].measurement_concept_id)] +=1
//                     }
//                 }
//             }// end for

//             // 객체에 넣기
//             var chk=0; // 잘 저장됐는지 검토하기.
//             for(i=0;i<result_measurement_id.length;i++){
//                 result.push(class_distribution_result( result_measurement_name[i], result_measurement_value[i],));
//                 chk += result_measurement_value[i];
//             }
//             console.log(chk)
//             return result;
//         } // make_distribution_measurement

//         function make_result_person_to_list(recordset){
//             let result = [];
//             for(i=0;i<recordset.length;i++){
//                 result.push(class_person(recordset[i].gender_concept_id, recordset[i].year_of_birth));
//             }
//             return result;
//         } 

//         function make_result_visit_to_list(recordset){
//             let result = [];
//             for (i = 0; i < recordset.length; i++) {
//                 result.push(class_visit(
//                     recordset[i].gender_concept_id,
//                     recordset[i].visit_concept_id,
//                     new Date(recordset[i].visit_start_date).format("yyyy-MM-dd"),
//                     new Date(recordset[i].visit_end_date).format("yyyy-MM-dd"),
//                     recordset[i].care_site_id
//                 ))
//             }
//             return result;
//         }

//         function make_result_condition_to_list(recordset) {
//             let result = [];
//             for (i = 0; i < recordset.length; i++) {
//                 result.push(class_condition(
//                     recordset[i].gender_concept_id,
//                     recordset[i].condition_concept_id,
//                     new Date(recordset[i].condition_start_date).format("yyyy-MM-dd"),
//                     recordset[i].concept_name
//                 ))
//             }

//             return result;
//         }

//         //durg 결과 내용을 리스트로 만들기. 
//         function make_result_drug_to_list(recordset){
//             let result = [];
//             for(i=0;i<recordset.length;i++){
//                 result.push(class_drug(
//                     recordset[i].gender_concept_id,
//                     recordset[i].drug_concept_id,
//                     new Date(recordset[i].drug_exposure_start_date).format("yyyy-MM-dd"),
//                     new Date(recordset[i].drug_exposure_end_date).format("yyyy-MM-dd"),
//                     recordset[i].quantity,
//                     recordset[i].days_supply,
//                     recordset[i].dose_unit_source_value,
//                     recordset[i].concept_name
//                     ))
//             }

//             return result;
//         }

//         //measurement_info 결과 내용을 리스트로 만들기.
//         function make_result_measurement_to_list(recordset){
//             let result = [];
//             for(i=0;i<recordset.length;i++){
//                 result.push(class_measurement(
//                     recordset[i].gender_concept_id,
//                     recordset[i].measurement_concept_id,
//                     new Date(recordset[i].measurement_date).format("yyyy-MM-dd"),
//                     recordset[i].value_as_number,
//                     recordset[i].range_low,
//                     recordset[i].range_high,
//                     recordset[i].unit_source_value,
//                     recordset[i].concept_name
//                     ))
//             }

//             return result;
//         }  
//         //환자번호 목록 리스트를 String 으로 다 붙여 넣는 함수 
//         //[1,2,3,4,5] => "1,2,3,4,5"
//         function make_list_to_string(obj){
//             let result='';

//             for(i=0;i<obj.recordset.length;i++){
//                 result += obj.recordset[i].person_id;
//                 if(i+1 != obj.recordset.length ){
//                     result += ", ";
//                 }else{
//                     result += " ";
//                 }
//             }
            
//             // result += `(${result})`
//             return result;
//         }
//         function asyncMain_2_0_assort_strength(result){
//             var out = {
//                 strength : [], 
//                 weak : [],
//                 visit:[], 
//                 user_define:[], 
//                 sql:[]
//             }
//             if(result.rowsAffected[0]==0){
//                 throw new Error(result);
//             }
//             rows = result.recordset;
//             for (i = 0; i < rows.length; i++) {
//                 if (rows[i].criteria_detail_table != 4 && rows[i].criteria_detail_table != 9 && rows[i].criteria_detail_table != 7 ) {
//                     if ((rows[i].strength == 1 || rows[i].strength == null)) {
//                         out.strength.push(rows[i]);
//                     } else {
//                         out.weak.push(rows[i]);
//                     }
//                 } else {
//                     if (rows[i].criteria_detail_table == 4) {
//                         out.visit.push(rows[i]);
//                     }
//                     if (rows[i].criteria_detail_table == 9) {
//                         out.user_define.push(rows[i]);
//                     }
//                     if( rows[i].criteria_detail_table == 7){
//                         out.sql.push(rows[i]);
//                     }
//                 }
//             }//for
//             return out;

//         }
//         async function asyncMain_2_0_1_list_of_user_define(rows_user_define){
//             console.log("2-1.asyncMain_2_0_1_list_of_user_define");
//             let out ={
//                 ud_inclusion_person_list :[],
//                 ud_exclusion_person_list : []
//             }
//             let rows_us;
//             if(rows_user_define.length==0){
//                 return out;
//             }else{
//                 rows_us = JSON.parse(JSON.stringify(rows_user_define));
//             }
//             //console.log({rows_us:rows_us});

//             //console.log({rows_user_define:rows_user_define});
//             // rows[] userdefide 관련 내용 ->  값 가지고와서 쿼리문 만들어야함
//             // 1. Inclusion, Exclusion 분류
//             // 2. 분류된 내용으로 다시 실제 조건 쿼리 만들고 리스트 함수에 각각 넣기. 
//             // 3. 리턴

//             let in_ex ={
//                 in : [],
//                 ex : []
//             }
//             for(i=0;i<rows_us.length;i++){
//                 //console.log({rows_i : rows_us[i]});
//                 if(rows_us[i].criteria_state==1){
//                     in_ex.in.push(rows_us[i]);
//                 }else{
//                     in_ex.ex.push(rows_us[i]);
//                 }
//             }//for
//             //console.log({in_ex:in_ex})
//             // inclusion
//             console.log('2-2.asyncMain_2_0_1_list_of_user_define');

//             try{
                
//                 let ud_pool = await sql.connect(dbConfig);
//                 let ud_id =0;
//                 let ud_result;
//                 var str_or_weak;
//                 let query_list_ud
//                 //Inclusion
//                 for(i=0;i<in_ex.in.length;i++){
//                     ud_id = in_ex.in[i].criteria_detail_value
//                     ud_result = await ud_pool.request()
//                     .input('input_paramiter1', sql.Int, ud_id)
//                     .query(`SELECT * FROM user_criteria_detail WHERE user_define_id = @input_paramiter1`)
                    
//                     ud_str_or_weak  = asyncMain_2_0_assort_strength(ud_result); // console.log({result_str_or_weak: str_or_weak_or_ud}); // 강한조건, 유연조건 나누기, User_define
//                     ud_query_list = asyncMain_2_1_make_query(ud_str_or_weak,1);
//                     console.log({ud_query_list:ud_query_list})
//                     ud_query = `SELECT distinct ud.person_id FROM (${ud_query_list[0]}) ud`

//                     ud_pool.close()
//                     sql.close();

//                     ud_pool = await sql.connect(CdmDbConfig);
//                     ud_result = await ud_pool.request()
//                     .query(ud_query)
//                     ud_pool.close()
//                     //console.log({ud_result:ud_result.recordset})
//                     ud_rows = ud_result.recordset

//                     for(j=0;j<ud_rows.length;j++){
//                         out.ud_inclusion_person_list= chk_person_id(out.ud_inclusion_person_list, ud_rows[j].person_id);
//                     }     
//                 }
//                 ud_pool.close()
//                 sql.close();

//                 ud_pool = await sql.connect(dbConfig);
//                 //Exclusion
//                 for(i=0;i<in_ex.ex.length;i++){
//                     ud_id = in_ex.ex[i].criteria_detail_value
//                     ud_result = await ud_pool.request()
//                     .input('input_paramiter1', sql.Int, ud_id)
//                     .query(`SELECT * FROM user_criteria_detail WHERE user_define_id = @input_paramiter1`)
                    
//                     ud_str_or_weak  = asyncMain_2_0_assort_strength(ud_result); // console.log({result_str_or_weak: str_or_weak_or_ud}); // 강한조건, 유연조건 나누기, User_define
//                     ud_query_list = asyncMain_2_1_make_query(ud_str_or_weak,1);
//                     //console.log({ud_query_list:ud_query_list})
//                     ud_query = `SELECT distinct ud.person_id FROM (${ud_query_list[0]}) ud`

//                     ud_pool.close()
//                     sql.close();

//                     ud_pool = await sql.connect(CdmDbConfig);
//                     ud_result = await ud_pool.request()
//                     .query(ud_query)
//                     ud_pool.close()
//                     //console.log({ud_result:ud_result.recordset})
//                     ud_rows = ud_result.recordset

//                     for(j=0;j<ud_rows.length;j++){
//                         out.ud_exclusion_person_list= chk_person_id(out.ud_exclusion_person_list, ud_rows[j].person_id);
//                     }     
//                 }

//                // console.log({out:out.ud_exclusion_person_list.length});
//                 ud_pool.close()
//                 sql.close()

//                 //선정 - 비선정 구하기 
//                 let idx =-1;
//                 for(i=0;i<out.ud_exclusion_person_list.length;i++){
//                     idx = out.ud_inclusion_person_list.indexOf(out.ud_exclusion_person_list[i])
//                     if(idx!=-1){
//                         out.ud_inclusion_person_list = out.ud_inclusion_person_list.splice(idx,1);
//                     }
//                     idx = -1;
//                 }
//                 //console.log({out:out});
//                 return out;
//             }catch(err){
//                 console.log({'err_2-2.asyncMain_2_0_1_list_of_user_define':err})
//                 //ud_pool.close()
//                 sql.close()
//             }
//            function chk_person_id (arr, person_id){
//                 if(arr.indexOf(person_id)==-1){
//                     arr.push(person_id);
//                 };
//                 return arr;
//            }
//         } // asyncMain_2_0_1_list_of_user_define

//         function asyncMain_2_1_make_query(rows_str_weak, condition){
//             util_console_print('------asyncMain_2_1_make_query------'); // console.log(rows_str_weak);
//             // 강한 조건에서 다시 조건 하나를 넣어준다. 
//             // 조건이 넣어지면 다시 선정 제외조건을 나눠서 쿼리문을 만들어준다. 
//             // 강한 조건이 끝나면 다시 약한 조건으로 하나씩 쿼리를 만든다. 
//             let in_rows=[], ex_rows=[];
//             let query_list =[]; 
//             let inclusion='' , exclusion='';
//             let visit ='';
//             let buf_row;

//             // visit 쿼리 만들기
//                                 // //visit
//                                 // case 4:
//                                 // result.visit.push (`( SELECT person_id FROM VISIT_OCCURRENCE WHERE person_id >0 ${val} )`);
//                                 // break;
//             if(rows_str_weak.visit!=null && rows_str_weak.visit.length!=0){
//                 buf_row = rows_str_weak.visit;
//                 table = buf_row[0].criteria_detail_table; //0
//                 field = buf_row[0].criteria_detail_attribute; //0
//                 con = buf_row[0].criteria_detail_condition;
//                 val = buf_row[0].criteria_addition;
//                 title = buf_row[0].criteria_title;
              
//                 visit = `SELECT distinct person_id FROM VISIT_OCCURRENCE WHERE person_id >0 and ${val} `
//             }
//            // console.log({rows_str_weak:rows_str_weak})
//             rows_str_weak = {
//                 strength : rows_str_weak.strength,
//                 weak : rows_str_weak.weak
//             }
           
//             //강한조건부터
//             let rows = rows_str_weak.strength;          //console.log({rows_count:rows.length});
//             for(k=0;k<rows.length;k++){
//                 //console.log({k:k});
//                 if(rows[k].criteria_state == 1 && rows[k].criteria_detail_table !=4){
//                     in_rows.push(rows[k]);
//                 }else if (rows[k].criteria_state != 1 && rows[k].criteria_detail_table !=4){
//                     ex_rows.push(rows[k]);
//                 }
//                 //console.log({in_rows:in_rows.length, ex_rows:ex_rows.length});
//                 inclusion = make_query_0(in_rows,1);              // 선정 1, 제외 0 
//                 exclusion = make_query_0(ex_rows,0);              //console.log({for: {in:in_rows.length, ex:ex_rows.length}});

//                 //condition =1은 user_define 
//                 if(inclusion.length != 0 && condition !=1){
//                     query_list.push(make_query_1(inclusion, exclusion, visit));
//                 }else if(k+1 == rows.length){
//                     query_list.push(make_query_1(inclusion, exclusion, visit));
//                 }
                
//             }
            
//             //약한조건을 한 개씩 추가해보기
//             rows = rows_str_weak.weak;
//             //console.log(rows);
//             let buf_in_rows =[], buf_ex_rows = [];
            
//             for(k=0;k<rows.length;k++){
//                 //console.log(rows[k]);
//                 buf_in_rows = JSON.parse(JSON.stringify(in_rows));
//                 buf_ex_rows = JSON.parse(JSON.stringify(ex_rows));
//                 if(rows[k].criteria_state == 1 && rows[k].criteria_detail_table !=4){
  
//                     buf_in_rows.push(rows[k]);
//                 }else if (rows[k].criteria_state != 1 && rows[k].criteria_detail_table !=4){
//                     buf_ex_rows.push(rows[k]);
//                 }
//                 inclusion = make_query_0(buf_in_rows,1); // 선정 1, 제외 0 
//                 exclusion = make_query_0(buf_ex_rows,0);              //console.log({for: {in:in_rows.length, ex:ex_rows.length}});
                

//                 if(inclusion.length != 0 ){
//                     query_list.push(make_query_1(inclusion, exclusion, visit));
//                 }
//             }

//             return query_list;
            
//         }
//         function make_query_0(rows, in_or_ex) {
//             //util_console_print('----make_query_0--in_or_ex: '+in_or_ex+'--')
//             var result = {
//                 person: [],
//                 drug: [],
//                 condition: [],
//                 measurement: [],
//                 user_define : []
//             }
//             var table = 0, field = 0;
//             var c_id = 0;
            
//             // 카테고리화 문제 때문에 
//             // table 값이 6인경우 mtdb에서 해당 id값을 가져와야함 
//             // criteria_table 에 텍스트 값을 비교하여 6: 진단, 5: 약을 구분 
//             // criteria_detail_attribute , {1,2,3}, {대, 중, 소}
//             // SELECT concept_id FROM mtdb.dbo.icd10_snomed WHERE SMALL = "%Gout%" 
            
//             var select_table = ['atc_rxnorm_all', 'icd10_snomed']; // -5
//             var select_lms = ['LARGE','MID','SMALL']; // -1 
//             var select_lms_atc = ['FIRST','SECOND','THIRD','FORTH','FIFTH'];
//             // var cdm={"table":
//             //     [
//             //         {"tableName":"person","fieldSet":["gender_concept_id","year_of_birth"]},
//             //         {"tableName":"drug_exposure","fieldSet":["drug_concept_id","drug_exposure_start_date","drug_exposure_end_date","drug_source_value"]},
//             //         {"tableName":"condition_occurrence","fieldSet":["condition_concept_id","condition_start_date","condition_end_date"]},
//             //         {"tableName":"measurement","fieldSet":["measurement_concept_id","measurement_date","value_as_number"]},
//             //         {"tableName":"visit_occurence","fieldSet":["visit_start_date","visit_type_concept_id"]}
//             //     ]}

//             for (i = 0; i < rows.length; i++) {
//                 table = rows[i].criteria_detail_table; //0
//                 field = rows[i].criteria_detail_attribute; //0
//                 con = rows[i].criteria_detail_condition;
//                 val = rows[i].criteria_detail_value;
//                 title = rows[i].criteria_title;
//                 if(rows[i].criteria_addition.length >0 && rows[i].criteria_addition!='null'){
//                     val += " " +rows[i].criteria_addition;
//                 }

//                 c_id = rows[i].criteria_id;
//                 //console.log("SELECT person_id FROM "+ cdm[table].tableName + " WHERE "+cdm[table].fieldSet[field]+" "+condition[con]+ " " + val );
//                 //condition확인
//                 //console.log(table);
//                 switch (Number(table)) {
//                     //person
//                     case 0:
//                         if(field==0){ //0 젠더
//                             result.person.push("( SELECT person_id, provider_id FROM person WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
//                         }else{
//                             result.person.push("( SELECT person_id, provider_id FROM person WHERE person_id  >0 " +rows[i].criteria_addition+ " )");
//                         }
                        
//                         break;
//                     //drug
//                     case 1:
//                         result.drug.push("( SELECT person_id, provider_id FROM drug_exposure WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
//                         break;
//                     //condition
//                     case 2:
//                         result.condition.push("( SELECT person_id, provider_id FROM condition_occurrence WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
//                         break;
//                     //measurement
//                     case 3:
//                     if(rows[i].measurement_count!=''||rows[i].measurement_method!=''){
//                         result.measurement.push("( SELECT person_id, provider_id FROM measurement WHERE person_id IN (  ( SELECT person_id FROM ( SELECT A.person_id  "+ measurement_method2( rows[i].measurement_method) +" FROM ( SELECT ROW_NUMBER() OVER(partition by person_id ORDER BY measurement_date DESC ) AS num, person_id, measurement_date, value_as_number FROM dbo.MEASUREMENT WHERE measurement_concept_id = "+ rows[i].criteria_detail_value + " "+ measurement_date(rows[i].date_addition_value)+ " ) A " +measurement_num(rows[i].measurement_count )+ " GROUP BY A.person_id ) B "+measurement_min_max(rows[i].value_addition_min, rows[i].value_addition_max,rows[i].min_condition, rows[i].max_condition) +")))" );
//                     }else{
//                         result.measurement.push("( SELECT person_id, provider_id FROM measurement WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val +")");
//                     }
                        
//                         break;
//                     //mtdb 약2
//                     case 5:
//                         // SELECT concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE SMALL = "%Gout%" 
//                         result.drug.push(`( SELECT person_id, provider_id FROM drug_exposure WHERE drug_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE ${select_lms_atc[field-1]} LIKE '%${title}%'))`);
//                         break;

//                     //mtdb 병력
//                     case 6:
//                         // SELECT concept_id FROM mtdb.dbo.icd10_snomed WHERE SMALL = "%Gout%" 
//                         result.condition.push(`( SELECT person_id, provider_id FROM  CONDITION_OCCURRENCE WHERE condition_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.icd10_snomed WHERE ${select_lms[field-1]} LIKE '%${title}%'))`);
//                         break;
//                 }// switch
                
//                 function measurement_method2(method){
//                     if(method.length!=0){
//                         return `, ${method}(A.value_as_number) as value_as_number `;
//                     }
//                     return '';
//                 }

//                 function measurement_num(num){
//                     if(num.length==0||num==null){
//                         return '';
//                     }else{
//                         return "WHERE num < " + num;
//                     }
//                 }

//                 function measurement_date(value_date){
//                    // console.log("hihi"+value_date.length);
//                     if(value_date.length==0){
//                         return '';
//                     }
//                     var split_date = value_date.split(" - ");
//                     var start_date = split_date[0];
//                     var end_date = split_date[1];
//                     var start_split = start_date.split("/");
//                     var end_split = end_date.split("/");
//                     var start_data = start_split[2]+"-"+start_split[1]+"-"+start_split[0];
//                     var end_data = end_split[2]+"-"+end_split[1]+"-"+end_split[0];
                    
//                     date_start = "and measurement_date >= '"+start_data+"' ";
//                     date_end = "and measurement_date <= '"+end_data+"' ";

//                     return date_start + date_end
//                 }// measurement_date 

//                 function measurement_min_max(min,max,min_con, max_con){
//                    var out ='';
//                    //console.log({in:min,max,min_con, max_con});
//                     if(min!=''){
//                         out += " WHERE B.value_as_number "+ measurement_condition(min_con) + " " +min ;
//                     }
//                     if(max!=''){
//                         if(out.length!=0){
//                             out += " and  B.value_as_number " + measurement_condition(max_con) + " "+ max;
//                         }else{
//                             out += " B.value_as_number " + measurement_condition(max_con) + " "+ max;
//                         }
                        
//                     }
//                     return out;
//                 }// measurement_min_max

//                 function measurement_condition(con){
//                     switch(con){
//                         case 'more than or equal':
//                         return '>=';
                        
//                         case 'more than':
//                         return '>';
                        
//                         case 'equal':
//                         return '=';

//                         case 'less than or equal':
//                         return '<=';
                        
//                         case 'less than':
//                         return '<';
//                     }//switch
//                 }// measurement_condition
//             }
//             //최종 쿼리 f_q
//             //person
//             var tables = ["person", "drug", "condition", "measurement"];
//             var f_q = {
//                 person: "",
//                 drug: "",
//                 condition: "",
//                 measurement: ""
//             }
//             var where = "";
//             var final = "";
//             if (in_or_ex == 1) {
//                 for (i = 0; i < tables.length; i++) {
//                     if (result[tables[i]].length != 0) {
//                         where = ""
//                         for (j = 0; j < result[tables[i]].length; j++) {
//                             f_q[tables[i]] += result[tables[i]][j] + " a" + j;
//                             if (j + 1 != result[tables[i]].length) {
//                                 f_q[tables[i]] += ", ";
//                             } else {
//                                 f_q[tables[i]] += " ";
//                             }
//                         }
//                         //where문장
//                         for (j = 0; j < result[tables[i]].length - 1; j++) {
//                             if (j + 1 != result[tables[i]].length - 1) {
//                                 where += "a" + j + ".person_id = a" + (j + 1) + ".person_id and ";
//                             } else {
//                                 where += "a" + j + ".person_id = a" + (j + 1) + ".person_id ";
//                             }
//                         }
    
//                         //쿼리문 작성
//                         if (result[tables[i]].length == 1) {
//                             f_q[tables[i]] = "SELECT DISTINCT a0.person_id, a0.provider_id FROM " + f_q[tables[i]];
//                         } else {
//                             f_q[tables[i]] = "SELECT DISTINCT a0.person_id, a0.provider_id FROM " + f_q[tables[i]] + " WHERE " + where;
//                         }
//                     }
    
//                 }
    
//                 //최종 추출 idx값 구하기.
//                 // var tables = ["person","drug","condition","measurement"];
//                 var idx = 0;
//                 if (result[tables[2]].length != 0) {
//                     idx = 2;
//                 } else if (result[tables[3]].length != 0) {
//                     idx = 3
//                 } else if (result[tables[1]].length != 0) {
//                     idx = 1;
//                 }
//                 // condition 내용 유무                console.log("condition유무: " + idx);
    
//                 //1 = inclusion, 0=exclusion
    
//                 var buf_tables = [];
//                 for (i = 0; i < tables.length; i++) {
//                     if (f_q[tables[i]].length != 0) {
//                         buf_tables.push(tables[i]);
//                     }
//                 }
//                 // console.log("Inserted tables:");                console.log(buf_tables);                console.log();
    
//                 idx = buf_tables.indexOf(tables[idx]);
                
//                 final += "SELECT b" + idx + ".provider_id, b" + idx + ".person_id, p1.year_of_birth, p1.gender_concept_id FROM "
    
//                 for (i = 0; i < buf_tables.length; i++) {
//                     if (buf_tables.length == 1) {
//                         final += "( " + f_q[buf_tables[i]] + " ) b" + i;
//                     } else {
//                         if (f_q[buf_tables[i]].length != 0) {
    
//                             final += "( " + f_q[buf_tables[i]] + " ) b" + i;
//                             if (i + 1 != buf_tables.length) {
//                                 final += ", ";
//                             } else {
//                                 final += " ";
//                             }
//                            // console.log("--final query--" + buf_tables[i]);                             console.log(final);                            console.log();
//                         }
//                     }
    
//                 }
//                 //final += " WHERE b0.person_id = b1.person_id and b1.person_id = b2.person_id and b2.person_id = b3.person_id and b3.person_id = b1.person_id";
    
//                 //각 항목에 내용 여부 체크
//                 var f_where = [];
//                 for (i = 0; i < 4; i++) {
//                     if (f_q[tables[i]].length != 0) {
//                         f_where.push(buf_tables.indexOf(tables[i]));
//                     }
//                 }
    
//                 var where_f = ", person p1 WHERE "
    
//                 for (i = 0; i < f_where.length - 1; i++) {
//                     if (i + 1 != f_where.length - 1) {
//                         where_f += "b" + f_where[i] + ".person_id = b" + f_where[i + 1] + ".person_id and ";
//                     } else {
//                         where_f += "b" + f_where[i] + ".person_id = b" + f_where[i + 1] + ".person_id ";
//                     }
//                 }
//                // console.log("f_where.length: " + f_where.length);                 console.log();    
//                 if(f_where.length !=1){
//                     final += where_f + "and b" + f_where[0] + ".person_id = b" + f_where[f_where.length - 1] + ".person_id and b" + idx + ".person_id = p1.person_id";
//                 }else{
//                     final += where_f + " b" + f_where[0] + ".person_id = b" + f_where[f_where.length - 1] + ".person_id and b" + idx + ".person_id = p1.person_id";
//                 }
                
//                // console.log("last final");                 console.log(final);                console.log();
    
//             } else {
//                 // exclusion
    
//                 for (i = 0; i < tables.length; i++) {
//                     //없는 각 테이블 버리기
//                     if (result[tables[i]].length != 0) {
//                         for (j = 0; j < result[tables[i]].length; j++) {
//                             // console.log(i+", "+j)
//                             final += result[tables[i]][j] + " UNION "
//                         }
//                     }
//                 }
//                 if (final.length != 0) {
//                     final = "SELECT c0.person_id FROM ( " + final.substring(0, final.length - 6) + " ) c0";
//                 } else {
//                     final = "";
//                 }
    
//             }
//             //result, f_q, final
//            // util_console_print('----make_query_0--end');
//             return final;
//         }
//         function make_query_1(inclusion, exclusion,visit){
//             util_console_print('-----make_query_1-----');
//             var query;
//             if (inclusion.length == 0 || exclusion == 0) {
//                 query = "SELECT DISTINCT A.person_id FROM ( " + inclusion + " ) A  "
//             } else {
//                 query = "SELECT DISTINCT A.person_id FROM FROM ( " + inclusion + " ) A LEFT JOIN ( " + exclusion + " ) B ON A.person_id = B.person_id WHERE B.person_id IS NULL"
//             }

//              // visit 적용
//             if(visit.length!=0){
//                 query = `SELECT F.person_id FROM ( ${query} ) F WHERE F.person_id in ( ${visit} )`;
//             }
//             return query;
//         }
//         function make_query_insert_1(result, protocol_id){
//             console.log({result:result})
//             var q_execute = []; var q_cnt = 0; // 1000개 이상일 경우 
//             if (result.recordset != null) {

//                 let rows = result.recordset
//                 //als;jdfl;kasjflkas;lfajskfjasd;lfsa;lflds;af;alksdjflksd;dlskflksd;fjd
//                 //rows = result_m;
//                 console.log("query result: " + rows.length);
//                 //결과값 0 체크
//                 if (rows.length != 0) {
//                     var q_list = [];
//                     var chk_num = -1;
                    
//                     //make_query_execute2(rows);
//                     // --원본--
//                     console.log("q_execute start")

//                     for (i = 0; i < rows.length; i++) {
//                             q_execute.push("( "+rows[i].person_id+", "+protocol_id+" )");
//                     }
//                     //console.log("q_execute");
//                     //console.log(q_execute)
//                     //res.send(q_execute);
//                     return q_execute;
//                 }else {
//                     console.log({ result: 0 })
//                 }
//                 //console.log(err);
//             } else {
//                 console.log(err);
//                 //res.send(err);
//             }
//         }//make_query_insert_1
//         function make_query_insert_5(q_execute){
//             var cnt_1000 = 0;
//             var query = "";
//             var q_final = [];
//             //console.log(q_execute);
//             for (i = 0; i < q_execute.length; i++) {
//                 query += q_execute[i];
//                 //console.log(q_execute[i]);
//                 cnt_1000 += 1;
//                 if (cnt_1000 + 1 == 1000) {
//                     q_final.push("INSERT INTO protocol_result VALUES " + query + " ;");
//                     cnt_1000 = 0;
//                     query = "";
//                 } else {
//                     if (i + 1 != q_execute.length) {
//                         query += ", ";
//                     }
//                 }
//             }
    
//             q_final.push("INSERT INTO protocol_result VALUES " + query + " ;");
//             //res.send(q_final);
//             console.log("q_final.length: " + q_final.length);
//             q_list1 = "";
//             for (i = 0; i < q_final.length; i++) {
//                 q_list1 += q_final[i];
//             }
//             return q_list1;
//         }//make_query_insert_5

//     }//async
// });//test-site-execute/:study_id/:query_id

// //////////Patiant
// app.get('/test-patient-execute/:study_id/:query_id',(req,res)=>{
//     util_console_print("----/test-site-execute/:study_id/:query_id----");
//     var study_id = Number(req.param('study_id'));
//     var query_id = Number(req.param('query_id'));
    
//     var query ='';
//     var provider_id ;
//     //let result ;
//     //study_id 가 provider_id 임
//     asyncMain(study_id, query_id);
    
//     ////Async 함수
//     async function asyncMain(study_id, query_id) {
//         try{
            
//             let pool =  await sql.connect(dbConfig); //scrn_cloud
//            //console.log(study_id, query_id);
//             //0. provider_id 가져오기
//             console.log('0.get provider_id  ');
//             let result = await pool.request() 
//             .input('input_paramiter1', sql.Int, study_id)
//             .query(`SELECT invest_num FROM study  WHERE study_id = @input_paramiter1`)
//             //            .query(`SELECT invest_num FROM patient_study  WHERE study_id = @input_paramiter1`)
//             //console.log(`UPDATE protocol SET protocol_status = N'InProgress' WHERE protocol_id = @input_paramiter`);
//             provider_id =result.recordset[0].invest_num;
//             util_console_print({"1.상태 업데이트": result});            // res.send({resunt:{provider_id:provider_id}}); sql.close();    
   
//             // 1.실행 버튼 클릭 시 '진행' 상태 변경하기.
//             console.log('1.상태 업데이트')
//             result = await pool.request() 
//             .input('input_paramiter1', sql.Int, study_id)
//             .input('input_paramiter2', sql.Int, query_id)
//             .query(`UPDATE patient_query SET query_status = N'In Progress' WHERE study_id = @input_paramiter1 and query_id = @input_paramiter2`)
//             //console.log(`UPDATE protocol SET protocol_status = N'InProgress' WHERE protocol_id = @input_paramiter`);
//             util_console_print({"1.상태 업데이트": result});  // res.send({resunt:{provider_id:provider_id}}); sql.close();  
           
//             // 2. 조건 쿼리 받아와서 쿼리문 만들기.
//             console.log('2. 조건쿼리 받아서 쿼리문 만들기');
//             result = await pool.request()
//             .input('input_paramiter1', sql.Int, study_id)
//             .input('input_paramiter2', sql.Int, query_id)
//             .query(`SELECT * FROM patient_criteria_detail WHERE study_id = @input_paramiter1 and query_id = @input_paramiter2`)
//             util_console_print({"2. 조건쿼리 받아서 쿼리문 만들기":result.recordset.length});



//             var str_or_weak_or_ud  = asyncMain_2_0_assort_strength(result);  //console.log({result_str_or_weak: str_or_weak_or_ud}); // 강한조건, 유연조건 나누기, User_define
//             sql.close();
//             //console.log({str_or_weak_or_ud:str_or_weak_or_ud})

//             // res.send(str_or_weak_or_ud);
//             // return 0
//             // out ={inclustion_list [], exclusion_list[]}
//             var user_define_list = await asyncMain_2_0_1_list_of_user_define(str_or_weak_or_ud.user_define);
//             //console.log({user_define_list:user_define_list});
//             var query_list = asyncMain_2_1_make_query(str_or_weak_or_ud,0, provider_id);

//             // 3. 마지막쿼리 돌려서 결과값 넣기.
//             console.log("----3번시작")
//             sql.close();
//             pool =  await sql.connect(CdmDbConfig); //scrn_cloud
//             var query_result =[];
//             console.log({query_list:query_list})
            
//             ////////////////////////////////////////
//             //User_Define 적용하기
//             let buf =''
//             let query_3 = `SELECT * FROM (${query_list[query_list.length-1]}) q3 `;
//             let out = user_define_list
//             if(out.ud_inclusion_person_list.length!=0){
//                 for(i=0;i<out.ud_inclusion_person_list.length;i++){
//                     buf += out.ud_inclusion_person_list[i]
//                     if(i+1 != out.ud_inclusion_person_list.length){
//                         buf += ", ";
//                     }
//                 }
//                 query_3 += `WHERE person_id in (${buf}) `;
                
//                 buf ='';

//                 if(out.ud_exclusion_person_list.length!=0){
//                     for(i=0;i<out.ud_exclusion_person_list.length;i++){
//                         buf += out.ud_exclusion_person_list[i]
//                         if(i+1 != out.ud_exclusion_person_list.length){
//                             buf += ", ";
//                         }
//                     }
//                     query_3 += `and person_id not in (${buf}) `;
//                 }
//             }else{
//                 if(out.ud_exclusion_person_list.length!=0){
//                     for(i=0;i<out.ud_exclusion_person_list.length;i++){
//                         buf += out.ud_exclusion_person_list[i]
//                         if(i+1 != out.ud_exclusion_person_list.length){
//                             buf += ", ";
//                         }
//                     }
//                     query_3 += `WHERE person_id not in (${buf}) `;
//                 }
//             }

//             // SQL문 적용하기 
//             // 1. 포함 비포함 분류하기. 
//             // 2. 포함 비포함에 맞는 sql문 적용하기. 
//             let sql_in_ex = {
//                 in :[],
//                 ex : []
//             }
//             if(str_or_weak_or_ud.sql.length!=0){
//                 //1. 분류하기.
//                 for(i=0;i<str_or_weak_or_ud.sql.length;i++){
//                     if(str_or_weak_or_ud.sql[i].criteria_state=1){
//                         sql_in_ex.in.push(str_or_weak_or_ud.sql[i]);
//                     }else{
//                         sql_in_ex.ex.push(str_or_weak_or_ud.sql[i]);
//                     }
//                 }

//                 //2. sql문 적용하기
//                 //inclusion.
//                 if(sql_in_ex.in.length!=0){
//                     if(sql_in_ex.ex.length!=0){
//                         query_3 = ` SELECT * FROM (${query_3}) sql WHERE person_id in (${sql_in_ex.in[0].criteria_addition}) and person_id not in (${sql_in_ex.ex[0].criteria_addition}) `;
//                     }else{
//                         query_3 = ` SELECT * FROM (${query_3}) sql WHERE person_id in (${sql_in_ex.in[0].criteria_addition})`;
//                     }
//                 }else{
//                     if(sql_in_ex.ex.length!=0){
//                         query_3 = ` SELECT * FROM (${query_3}) sql WHERE person_id not in (${sql_in_ex.ex[0].criteria_addition}) `;
//                     }
//                 }             
//             }//if(str_or_weak_or_ud.sql.length!=0)
//             ///////////////////////////////////

//             //최종 쿼리 실행
//             console.log({query_3 :query_3});
//             result = await pool.request()
//             .query(query_3);

//             //4. 기존 쿼리 결과 지우기
//             //change dbconfig
//             sql.close();
//             pool =  await sql.connect(dbConfig); //scrn_cloud
//             console.log("----4번시작");
//             var result_3 = make_query_insert_1(result,study_id, query_id, provider_id);     //     res.send({resunt:{result_3:result_3}}); // sql.close();  
            
//             q = "DELETE FROM patient_result WHERE study_id = " + study_id + " and query_id = " + query_id; //console.log(q)
//             result = await pool.request()
//             .query(q);
//             util_console_print({"4. 기존쿼리결과지우기":result});
            
//             //5. 쿼리 삽입하기.
//             console.log("----5번시작");
//             var query_5 = make_query_insert_5(result_3);
//             cons
//             result = await pool.request()
//             .query(query_5);
            

//             console.log('6.상태 업데이트')
//             result = await pool.request() 
//             .input('input_paramiter1', sql.Int, study_id)
//             .input('input_paramiter2', sql.Int, query_id)
//             .query(`UPDATE patient_query SET query_status = N'InProgress' WHERE study_id = @input_paramiter1 and query_id = @input_paramiter2`)
//             //console.log(`UPDATE protocol SET protocol_status = N'InProgress' WHERE protocol_id = @input_paramiter`);
//             util_console_print({"6.상태 업데이트": result});  // res.send({resunt:{provider_id:provider_id}}); sql.close();  
//         //res.send({resunt:{result:result}});
//         sql.close();    
//         }catch(err){
//             util_console_print({error: err});
//            // res.send({error:'2.result is 0'});

//             sql.close();
//         }
       

//         function asyncMain_2_0_assort_strength(result){
//             var out = {
//                 strength : [], 
//                 weak : [],
//                 visit:[], 
//                 user_define:[], 
//                 sql:[], 
//             }
//             if(result.rowsAffected[0]==0){
//                 throw new Error(result);
//             }
//             rows = result.recordset;
//             for (i = 0; i < rows.length; i++) {
//                 if (rows[i].criteria_detail_table != 4 && rows[i].criteria_detail_table != 9 && rows[i].criteria_detail_table != 7 ) {
//                     if ((rows[i].strength == 1 || rows[i].strength == null)) {
//                         out.strength.push(rows[i]);
//                     } else {
//                         out.weak.push(rows[i]);
//                     }
//                 } else {
//                     if (rows[i].criteria_detail_table == 4) {
//                         out.visit.push(rows[i]);
//                     }
//                     if (rows[i].criteria_detail_table == 9) {
//                         out.user_define.push(rows[i]);
//                     }
//                     if( rows[i].criteria_detail_table == 7){
//                         out.sql.push(rows[i]);
//                     }
//                 }
//             }//for
//             return out;;

//         }
//         //User_Define 내용 만들기.
//         async function asyncMain_2_0_1_list_of_user_define(rows_user_define){
//             console.log("2-1.asyncMain_2_0_1_list_of_user_define");
//             let out ={
//                 ud_inclusion_person_list :[],
//                 ud_exclusion_person_list : []
//             }
//             let rows_us;
//             if(rows_user_define.length==0){
//                 return out;
//             }else{
//                 rows_us = JSON.parse(JSON.stringify(rows_user_define));
//             }
//            // console.log({rows_us:rows_us});

//             //console.log({rows_user_define:rows_user_define});
//             // rows[] userdefide 관련 내용 ->  값 가지고와서 쿼리문 만들어야함
//             // 1. Inclusion, Exclusion 분류
//             // 2. 분류된 내용으로 다시 실제 조건 쿼리 만들고 리스트 함수에 각각 넣기. 
//             // 3. 리턴

            

//             let in_ex ={
//                 in : [],
//                 ex : []
//             }
//             for(i=0;i<rows_us.length;i++){
//                 //console.log({rows_i : rows_us[i]});
//                 if(rows_us[i].criteria_state==1){
//                     in_ex.in.push(rows_us[i]);
//                 }else{
//                     in_ex.ex.push(rows_us[i]);
//                 }
//             }//for
//             //console.log({in_ex:in_ex})
//             // inclusion
//             console.log('2-2.asyncMain_2_0_1_list_of_user_define');

//             try{
                
//                 let ud_pool = await sql.connect(dbConfig);
//                 let ud_id =0;
//                 let ud_result;
//                 var str_or_weak;
//                 let query_list_ud
//                 //Inclusion
//                 for(i=0;i<in_ex.in.length;i++){
//                     ud_id = in_ex.in[i].criteria_detail_value
//                     ud_result = await ud_pool.request()
//                     .input('input_paramiter1', sql.Int, ud_id)
//                     .query(`SELECT * FROM user_criteria_detail WHERE user_define_id = @input_paramiter1`)
                    
//                     ud_str_or_weak  = asyncMain_2_0_assort_strength(ud_result); // console.log({result_str_or_weak: str_or_weak_or_ud}); // 강한조건, 유연조건 나누기, User_define
//                     ud_query_list = asyncMain_2_1_make_query(ud_str_or_weak,1);
//                     //console.log({ud_query_list:ud_query_list})
//                     ud_query = `SELECT distinct ud.person_id FROM (${ud_query_list[0]}) ud`

//                     ud_pool.close()
//                     sql.close();

//                     ud_pool = await sql.connect(CdmDbConfig);
//                     ud_result = await ud_pool.request()
//                     .query(ud_query)
//                     ud_pool.close()
//                     //console.log({ud_result:ud_result.recordset})
//                     ud_rows = ud_result.recordset

//                     for(j=0;j<ud_rows.length;j++){
//                         out.ud_inclusion_person_list= chk_person_id(out.ud_inclusion_person_list, ud_rows[j].person_id);
//                     }     
//                 }
//                 ud_pool.close()
//                 sql.close();

//                 ud_pool = await sql.connect(dbConfig);
//                 //Exclusion
//                 for(i=0;i<in_ex.ex.length;i++){
//                     ud_id = in_ex.ex[i].criteria_detail_value
//                     ud_result = await ud_pool.request()
//                     .input('input_paramiter1', sql.Int, ud_id)
//                     .query(`SELECT * FROM user_criteria_detail WHERE user_define_id = @input_paramiter1`)
                    
//                     ud_str_or_weak  = asyncMain_2_0_assort_strength(ud_result); // console.log({result_str_or_weak: str_or_weak_or_ud}); // 강한조건, 유연조건 나누기, User_define
//                     ud_query_list = asyncMain_2_1_make_query(ud_str_or_weak,1);
//                     //console.log({ud_query_list:ud_query_list})
//                     ud_query = `SELECT distinct ud.person_id FROM (${ud_query_list[0]}) ud`

//                     ud_pool.close()
//                     sql.close();

//                     ud_pool = await sql.connect(CdmDbConfig);
//                     ud_result = await ud_pool.request()
//                     .query(ud_query)
//                     ud_pool.close()
//                     //console.log({ud_result:ud_result.recordset})
//                     ud_rows = ud_result.recordset

//                     for(j=0;j<ud_rows.length;j++){
//                         out.ud_exclusion_person_list= chk_person_id(out.ud_exclusion_person_list, ud_rows[j].person_id);
//                     }     
//                 }

//                // console.log({out:out.ud_exclusion_person_list.length});
//                 ud_pool.close()
//                 sql.close()

//                 //선정 - 비선정 구하기 
//                 let idx =-1;
//                 for(i=0;i<out.ud_exclusion_person_list.length;i++){
//                     idx = out.ud_inclusion_person_list.indexOf(out.ud_exclusion_person_list[i])
//                     if(idx!=-1){
//                         out.ud_inclusion_person_list = out.ud_inclusion_person_list.splice(idx,1);
//                     }
//                     idx = -1;
//                 }
//                 //console.log({out:out});
//                 return out;
//             }catch(err){
//                 console.log({'err_2-2.asyncMain_2_0_1_list_of_user_define':err})
//                 ud_pool.close()
//                 sql.close()
//             }
//            function chk_person_id (arr, person_id){
//                 if(arr.indexOf(person_id)==-1){
//                     arr.push(person_id);
//                 };
//                 return arr;
//            }
//         } // asyncMain_2_0_1_list_of_user_define


//         function asyncMain_2_1_make_query(rows_str_weak, condition, provider_id){
//              //util_console_print('------asyncMain_2_1_make_query------'); // console.log(rows_str_weak);
//             // 강한 조건에서 다시 조건 하나를 넣어준다. 
//             // 조건이 넣어지면 다시 선정 제외조건을 나눠서 쿼리문을 만들어준다. 
//             // 강한 조건이 끝나면 다시 약한 조건으로 하나씩 쿼리를 만든다. 
//             let in_rows=[], ex_rows=[];
//             let query_list =[]; 
//             let inclusion='' , exclusion='';
//             let visit ='';
//             let buf_row;
           
//             // break;
//             if(rows_str_weak.visit!=null && rows_str_weak.visit.length!=0){
//                 buf_row = rows_str_weak.visit;
//                 table = buf_row[0].criteria_detail_table; //0
//                 field = buf_row[0].criteria_detail_attribute; //0
//                 con = buf_row[0].criteria_detail_condition;
//                 val = buf_row[0].criteria_addition;
//                 title = buf_row[0].criteria_title;
                
//                 visit = `SELECT distinct person_id FROM VISIT_OCCURRENCE WHERE person_id >0 ${val} `
//             }
//             //console.log({rows_str_weak:rows_str_weak})

//             rows_str_weak = {
//                 strength : rows_str_weak.strength,
//                 weak : rows_str_weak.weak
//             }

//             //강한조건부터
//             let rows = rows_str_weak.strength;          //console.log({rows_count:rows.length});
//             for(k=0;k<rows.length;k++){
//                 //console.log({k:k});
//                 if(rows[k].criteria_state == 1 && rows[k].criteria_detail_table !=4){
//                     in_rows.push(rows[k]);
//                 }else if (rows[k].criteria_state != 1 && rows[k].criteria_detail_table !=4){
//                     ex_rows.push(rows[k]);
//                 }
//                 //console.log({in_rows:in_rows.length, ex_rows:ex_rows.length});
//                 inclusion = make_query_0(in_rows,1, provider_id);              // 선정 1, 제외 0 
//                 exclusion = make_query_0(ex_rows,0, provider_id);              //console.log({for: {in:in_rows.length, ex:ex_rows.length}});

//                 //condition =1은 user_define 
//                 if(inclusion.length != 0 && condition !=1){
//                     query_list.push(make_query_1(inclusion, exclusion, visit));
//                 }else if(k+1 == rows.length){
//                     query_list.push(make_query_1(inclusion, exclusion, visit));
//                 }
                
//             }
            
//             //약한조건을 한 개씩 추가해보기
//             rows = rows_str_weak.weak;
//             //console.log(rows);
//             let buf_in_rows =[], buf_ex_rows = [];
            
//             for(k=0;k<rows.length;k++){
//                 //console.log(rows[k]);
//                 buf_in_rows = JSON.parse(JSON.stringify(in_rows));
//                 buf_ex_rows = JSON.parse(JSON.stringify(ex_rows));
//                 if(rows[k].criteria_state == 1 && rows[k].criteria_detail_table !=4){
  
//                     buf_in_rows.push(rows[k]);
//                 }else if (rows[k].criteria_state != 1 && rows[k].criteria_detail_table !=4){
//                     buf_ex_rows.push(rows[k]);
//                 }
//                 inclusion = make_query_0(buf_in_rows,1, provider_id); // 선정 1, 제외 0 
//                 exclusion = make_query_0(buf_ex_rows,0, provider_id);              //console.log({for: {in:in_rows.length, ex:ex_rows.length}});
                

//                 if(inclusion.length != 0 ){
//                     query_list.push(make_query_1(inclusion, exclusion, visit));
//                 }
//             }

//             return query_list;
            
//         }
//         function make_query_0(rows, in_or_ex, provider_id) {
//             //util_console_print('----make_query_0--in_or_ex: '+in_or_ex+'--')
//             var result = {
//                 person: [],
//                 drug: [],
//                 condition: [],
//                 measurement: [],
//                 user_define : []
//             }
//             var table = 0, field = 0;
//             var c_id = 0;
            
//             // 카테고리화 문제 때문에 
//             // table 값이 6인경우 mtdb에서 해당 id값을 가져와야함 
//             // criteria_table 에 텍스트 값을 비교하여 6: 진단, 5: 약을 구분 
//             // criteria_detail_attribute , {1,2,3}, {대, 중, 소}
//             // SELECT concept_id FROM mtdb.dbo.icd10_snomed WHERE SMALL = "%Gout%" 
            
//             var select_table = ['atc_rxnorm_all', 'icd10_snomed']; // -5
//             var select_lms = ['LARGE','MID','SMALL']; // -1 
//             var select_lms_atc = ['FIRST','SECOND','THIRD','FORTH','FIFTH'];
//             // var cdm={"table":
//             //     [
//             //         {"tableName":"person","fieldSet":["gender_concept_id","year_of_birth"]},
//             //         {"tableName":"drug_exposure","fieldSet":["drug_concept_id","drug_exposure_start_date","drug_exposure_end_date","drug_source_value"]},
//             //         {"tableName":"condition_occurrence","fieldSet":["condition_concept_id","condition_start_date","condition_end_date"]},
//             //         {"tableName":"measurement","fieldSet":["measurement_concept_id","measurement_date","value_as_number"]},
//             //         {"tableName":"visit_occurence","fieldSet":["visit_start_date","visit_type_concept_id"]}
//             //     ]}

//             for (i = 0; i < rows.length; i++) {
//                 table = rows[i].criteria_detail_table; //0
//                 field = rows[i].criteria_detail_attribute; //0
//                 con = rows[i].criteria_detail_condition;
//                 val = rows[i].criteria_detail_value;
//                 title = rows[i].criteria_title;
//                 if(rows[i].criteria_addition.length >0 && rows[i].criteria_addition!='null' ){
//                     val += " " +rows[i].criteria_addition;
//                 }


//                 c_id = rows[i].criteria_id;
//                 //console.log("SELECT person_id FROM "+ cdm[table].tableName + " WHERE "+cdm[table].fieldSet[field]+" "+condition[con]+ " " + val );
//                 //condition확인
//                 //console.log(table);
//                 switch (Number(table)) {
//                     //person
//                     case 0:
//                         if(field==0){ //0 젠더
//                             result.person.push("( SELECT person_id, provider_id FROM person WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
//                         }else{
//                             result.person.push("( SELECT person_id, provider_id FROM person WHERE person_id  >0 " +rows[i].criteria_addition+ " )");
//                         }
                        
//                         break;
//                     //drug
//                     case 1:
//                         result.drug.push("( SELECT person_id, provider_id FROM drug_exposure WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
//                         break;
//                     //condition
//                     case 2:
//                         result.condition.push("( SELECT person_id, provider_id FROM condition_occurrence WHERE provider_id = "+provider_id+" and " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
//                         break;
//                     //measurement
//                     case 3:
//                     if(rows[i].measurement_count!=''||rows[i].measurement_method!=''){
//                         result.measurement.push("( SELECT person_id, provider_id FROM measurement WHERE person_id IN (  ( SELECT person_id FROM ( SELECT A.person_id  "+ measurement_method2( rows[i].measurement_method) +" FROM ( SELECT ROW_NUMBER() OVER(partition by person_id ORDER BY measurement_date DESC ) AS num, person_id, measurement_date, value_as_number FROM dbo.MEASUREMENT WHERE measurement_concept_id = "+ rows[i].criteria_detail_value + " "+ measurement_date(rows[i].date_addition_value)+ " ) A " +measurement_num(rows[i].measurement_count )+ " GROUP BY A.person_id ) B "+measurement_min_max(rows[i].value_addition_min, rows[i].value_addition_max,rows[i].min_condition, rows[i].max_condition) +")))" );
//                     }else{
//                         result.measurement.push("( SELECT person_id, provider_id FROM measurement WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val +")");
//                     }
                        
//                         break;
//                     //mtdb 약2
//                     case 5:
//                         // SELECT concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE SMALL = "%Gout%" 
//                         result.drug.push(`( SELECT person_id, provider_id FROM drug_exposure WHERE drug_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE ${select_lms_atc[field-1]} LIKE '%${title}%'))`);
//                         break;

//                     //mtdb 병력
//                     case 6:
//                         // SELECT concept_id FROM mtdb.dbo.icd10_snomed WHERE SMALL = "%Gout%" 
//                         result.condition.push(`( SELECT person_id, provider_id FROM  CONDITION_OCCURRENCE WHERE condition_concept_id IN ( SELECT distinct concept_id FROM mtdb.dbo.icd10_snomed WHERE ${select_lms[field-1]} LIKE '%${title}%') and provider_id = ${provider_id})`);
//                         break;
//                 }// switch
                
//                 function measurement_method2(method){
//                     if(method.length!=0){
//                         return `, ${method}(A.value_as_number) as value_as_number `;
//                     }
//                     return '';
//                 }

//                 function measurement_num(num){
//                     if(num.length==0||num==null){
//                         return '';
//                     }else{
//                         return "WHERE num < " + num;
//                     }
//                 }

//                 function measurement_date(value_date){
//                    // console.log("hihi"+value_date.length);
//                     if(value_date.length==0){
//                         return '';
//                     }
//                     var split_date = value_date.split(" - ");
//                     var start_date = split_date[0];
//                     var end_date = split_date[1];
//                     var start_split = start_date.split("/");
//                     var end_split = end_date.split("/");
//                     var start_data = start_split[2]+"-"+start_split[1]+"-"+start_split[0];
//                     var end_data = end_split[2]+"-"+end_split[1]+"-"+end_split[0];
                    
//                     date_start = "and measurement_date >= '"+start_data+"' ";
//                     date_end = "and measurement_date <= '"+end_data+"' ";

//                     return date_start + date_end
//                 }// measurement_date 

//                 function measurement_min_max(min,max,min_con, max_con){
//                    var out ='';
//                    //console.log({in:min,max,min_con, max_con});
//                     if(min!=''){
//                         out += " WHERE B.value_as_number "+ measurement_condition(min_con) + " " +min ;
//                     }
//                     if(max!=''){
//                         if(out.length!=0){
//                             out += " and  B.value_as_number " + measurement_condition(max_con) + " "+ max;
//                         }else{
//                             out += " B.value_as_number " + measurement_condition(max_con) + " "+ max;
//                         }
                        
//                     }
//                     return out;
//                 }// measurement_min_max

//                 function measurement_condition(con){
//                     switch(con){
//                         case 'more than or equal':
//                         return '>=';
                        
//                         case 'more than':
//                         return '>';
                        
//                         case 'equal':
//                         return '=';

//                         case 'less than or equal':
//                         return '<=';
                        
//                         case 'less than':
//                         return '<';
//                     }//switch
//                 }// measurement_condition
//             }
//             //최종 쿼리 f_q
//             //person
//             var tables = ["person", "drug", "condition", "measurement"];
//             var f_q = {
//                 person: "",
//                 drug: "",
//                 condition: "",
//                 measurement: ""
//             }
//             var where = "";
//             var final = "";
//             if (in_or_ex == 1) {
//                 for (i = 0; i < tables.length; i++) {
//                     if (result[tables[i]].length != 0) {
//                         where = ""
//                         for (j = 0; j < result[tables[i]].length; j++) {
//                             f_q[tables[i]] += result[tables[i]][j] + " a" + j;
//                             if (j + 1 != result[tables[i]].length) {
//                                 f_q[tables[i]] += ", ";
//                             } else {
//                                 f_q[tables[i]] += " ";
//                             }
//                         }
//                         //where문장
//                         for (j = 0; j < result[tables[i]].length - 1; j++) {
//                             if (j + 1 != result[tables[i]].length - 1) {
//                                 where += "a" + j + ".person_id = a" + (j + 1) + ".person_id and ";
//                             } else {
//                                 where += "a" + j + ".person_id = a" + (j + 1) + ".person_id ";
//                             }
//                         }
    
//                         //쿼리문 작성
//                         if (result[tables[i]].length == 1) {
//                             f_q[tables[i]] = "SELECT DISTINCT a0.person_id, a0.provider_id FROM " + f_q[tables[i]];
//                         } else {
//                             f_q[tables[i]] = "SELECT DISTINCT a0.person_id, a0.provider_id FROM " + f_q[tables[i]] + " WHERE " + where;
//                         }
//                     }
    
//                 }
    
//                 //최종 추출 idx값 구하기.
//                 // var tables = ["person","drug","condition","measurement"];
//                 var idx = 0;
//                 if (result[tables[2]].length != 0) {
//                     idx = 2;
//                 } else if (result[tables[3]].length != 0) {
//                     idx = 3
//                 } else if (result[tables[1]].length != 0) {
//                     idx = 1;
//                 }
//                 // condition 내용 유무                console.log("condition유무: " + idx);
    
//                 //1 = inclusion, 0=exclusion
    
//                 var buf_tables = [];
//                 for (i = 0; i < tables.length; i++) {
//                     if (f_q[tables[i]].length != 0) {
//                         buf_tables.push(tables[i]);
//                     }
//                 }
//                 // console.log("Inserted tables:");                console.log(buf_tables);                console.log();
    
//                 idx = buf_tables.indexOf(tables[idx]);
//                 final += "SELECT b" + idx + ".provider_id, b" + idx + ".person_id, p1.year_of_birth, p1.gender_concept_id FROM "
    
//                 for (i = 0; i < buf_tables.length; i++) {
//                     if (buf_tables.length == 1) {
//                         final += "( " + f_q[buf_tables[i]] + " ) b" + i;
//                     } else {
//                         if (f_q[buf_tables[i]].length != 0) {
    
//                             final += "( " + f_q[buf_tables[i]] + " ) b" + i;
//                             if (i + 1 != buf_tables.length) {
//                                 final += ", ";
//                             } else {
//                                 final += " ";
//                             }
//                            // console.log("--final query--" + buf_tables[i]);                             console.log(final);                            console.log();
//                         }
//                     }
    
//                 }
//                 //final += " WHERE b0.person_id = b1.person_id and b1.person_id = b2.person_id and b2.person_id = b3.person_id and b3.person_id = b1.person_id";
    
//                 //각 항목에 내용 여부 체크
//                 var f_where = [];
//                 for (i = 0; i < 4; i++) {
//                     if (f_q[tables[i]].length != 0) {
//                         f_where.push(buf_tables.indexOf(tables[i]));
//                     }
//                 }
    
//                 var where_f = ", person p1 WHERE "
    
//                 for (i = 0; i < f_where.length - 1; i++) {
//                     if (i + 1 != f_where.length - 1) {
//                         where_f += "b" + f_where[i] + ".person_id = b" + f_where[i + 1] + ".person_id and ";
//                     } else {
//                         where_f += "b" + f_where[i] + ".person_id = b" + f_where[i + 1] + ".person_id ";
//                     }
//                 }
//                // console.log("f_where.length: " + f_where.length);                 console.log();    
//                 if(f_where.length !=1){
//                     final += where_f + "and b" + f_where[0] + ".person_id = b" + f_where[f_where.length - 1] + ".person_id and b" + idx + ".person_id = p1.person_id";
//                 }else{
//                     final += where_f + " b" + f_where[0] + ".person_id = b" + f_where[f_where.length - 1] + ".person_id and b" + idx + ".person_id = p1.person_id";
//                 }
                
//                // console.log("last final");                 console.log(final);                console.log();
    
//             } else {
//                 // exclusion
    
//                 for (i = 0; i < tables.length; i++) {
//                     //없는 각 테이블 버리기
//                     if (result[tables[i]].length != 0) {
//                         for (j = 0; j < result[tables[i]].length; j++) {
//                             // console.log(i+", "+j)
//                             final += result[tables[i]][j] + " UNION "
//                         }
//                     }
//                 }
//                 if (final.length != 0) {
//                     final = "SELECT c0.person_id FROM ( " + final.substring(0, final.length - 6) + " ) c0";
//                 } else {
//                     final = "";
//                 }
    
//             }
//             //result, f_q, final
//            // util_console_print('----make_query_0--end');
//             return final;
//         }
//         function make_query_1(inclusion, exclusion, visit){
//             util_console_print('-----make_query_1-----');
//             var query;
//             if (inclusion.length == 0 || exclusion == 0) {
//                 query = "SELECT provider_id, A.person_id, A.year_of_birth, A.gender_concept_id FROM ( " + inclusion + " ) A  "
//             } else {
//                 query = "SELECT provider_id, A.person_id, A.year_of_birth, A.gender_concept_id FROM ( " + inclusion + " ) A LEFT JOIN ( " + exclusion + " ) B ON A.person_id = B.person_id WHERE B.person_id IS NULL"
//             }
//             // visit 적용
//             if(visit.length!=0){
//                 query = `SELECT DISTINCT F.provider_id, F.person_id, F.year_of_birth, F.gender_concept_id FROM ( ${query} ) F WHERE F.person_id in ( ${visit} )`;
//             }

//             return query;
//         }
//         function make_query_insert_1(result, study_id, query_id, provider_id){
//             var q_execute = []; var q_cnt = 0; // 1000개 이상일 경우 
//             if (result != null) {
//                 let rows = result.recordset
//                 //als;jdfl;kasjflkas;lfajskfjasd;lfsa;lflds;af;alksdjflksd;dlskflksd;fjd
//                 //rows = result_m;
//                 console.log("query result: " + rows.length);
//                 //결과값 0 체크
//                 if (rows.length != 0) {
//                     var q_list = [];
//                     var chk_num = -1;
                    
//                     //console.log(q_list);
//                     //res.send(q_list);


//                     //make_query_execute2(rows);
//                     // --원본--
//                     console.log("q_execute start")

//                     for (i = 0; i < rows.length; i++) {
//                             q_execute.push("( " + study_id + ", " + query_id + ", " +rows[i].person_id+ ", " + rows[i].year_of_birth + ", " +rows[i].gender_concept_id + "," + provider_id+ ")");
//                     }
//                     //console.log("q_execute");
//                     //console.log(q_execute)
//                     //res.send(q_execute);
//                     return q_execute;
//                 } else {
//                     console.log({ result: 0 })
//                     return q_execute;
//                 }
//                 //console.log(err);
//             } else {
//                 console.log(err);
//                 //res.send(err);
//             }

//         }//make_query_insert_1
//         function make_query_insert_5(q_execute) {
//             var cnt_1000 = 0;
//             var query = "";
//             var q_final = [];
//             q_list1 = "";
//             //console.log(q_execute);
//             if (q_execute.length != 0) {
//                 for (i = 0; i < q_execute.length; i++) {
//                     query += q_execute[i];
//                     //console.log(q_execute[i]);
//                     cnt_1000 += 1;
//                     if (cnt_1000 + 1 == 1000) {
//                         q_final.push("INSERT INTO patient_result(study_id, query_id, patient_id, patient_age, patient_gender, provider_id) VALUES " + query + " ;");
//                         cnt_1000 = 0;
//                         query = "";
//                     } else {
//                         if (i + 1 != q_execute.length) {
//                             query += ", ";
//                         }

//                     }
//                 }

//                 q_final.push("INSERT INTO patient_result(study_id, query_id, patient_id, patient_age, patient_gender, provider_id) VALUES " + query + " ;");

//                 //res.send(q_final);
//                 console.log("q_final.length: " + q_final.length);
//                for (i = 0; i < q_final.length; i++) {
//                     q_list1 += q_final[i];
//                 }
//             }

//             return q_list1;
//         }//make_query_insert_5

//     }//async
    
    
// });///test-site-execute/:study_id/:query_id


// app.get('/test-protocol-advanced/:protocol_id', (req,res)=>{
//     console.log('/test-protocol-advanced/:protocol_id');
//     var protocol_id = Number(req.param('protocol_id'));
   
//     start(protocol_id,res);

//     async function start(protocol_id){
//         try{
//             let out='';


//             let pool =  await sql.connect(dbConfig); //scrn_cloud
//             let result = await pool.request() 
//              .input('input_paramiter1', sql.Int, protocol_id)
//              .query(`SELECT DISTINCT person_id FROM protocol_result  WHERE protocol_id = @input_paramiter1`)
//             pool.close();
//             sql.close();


//             if(result !=null){
//                 let rows = result.recordset;
//                 var personList = person_set_by_rows_string(rows);
//                 console.log({personList:personList});
                
//                 pool =  await sql.connect(CdmDbConfig); //scrn_cloud
                
//                 console.log("0")
//                 result = await pool.request() 
//                 .query(`SELECT person_id, gender_concept_id, year_of_birth FROM person  WHERE person_id in ( ${personList} )`)
//                 console.log("1");
//                 if(result != null){
//                     rows = result.recordset
//                     console.log("pool2_query_result: " + rows.length);
//                     var set = person_set_by_rows(rows);
//                     //res.send(set);
//                     pool.close();
//             sql.close();
//                 }
//             }

//         }catch(err){
//             console.log({err:err});
//             //res.send(0);
//             pool.close();
//             sql.close();
//         }finally{
//             //res.send(0);
//             pool.close();
//             sql.close();
//         }
        
//     }// async function start()

//     function person_set_by_rows_string(rows){
//         var obj ='';
    
//         for(i=0;i<rows.length;i++){
//             obj += rows[i].person_id ;
//             if(i+1 != rows.length){
//                 obj += ", ";
//             }
//         }
//         return obj;
//     }
//     function person_set_by_rows(rows){
      
//         var list =[];
//         var now_year = new Date().format('yyyy');
//         for(i=0;i<rows.length;i++){
//             list.push(make_person_set(i, rows[i].gender_concept_id, (now_year-rows[i].year_of_birth)));
//         }
//         console.log({person_set_by_rows:list});
//         return list ;
//     }
//     function make_person_set(person_id, gender, year){
//         var gen=''; 
//         if(gender==8507){
//             gen ="Male";
//         }else{
//             gen ="Female";
//         }
    
//         var obj ={
//             id : person_id,
//             gender : gen,
//             age : year
//         }
//         return obj;
//     }
    

// });///test-protocol-advanced/:protocol_id



/////////////////////////////////////////////////////////////////////////////////////////// 서버 실행 부분
app.use((error,req,res,next)=>{
    res.json({message: error.message});
})


// 1092374098123498378241234124124324123412341242142134
// 웹 서버를 실행
http.createServer(app).listen(52274, function(req, res){
    console.log('Test Server Running at http://127.0.0.1:52274');
    console.log();
});
// 1092374098123498378241234124124324123412341242142134
/////////////////////////////////////////////////////////////////////////////////////////// 서버 실행 부분 끝


function obj_provider(id){
    var obj={
        id : id,
        total : 0,
        male: {d9:0,d10:0,d20:0,d30:0, d40:0, d50:0, d60:0,d70:0},
        female: {d9:0,d10:0,d20:0,d30:0, d40:0, d50:0, d60:0,d70:0}
    }
    return obj;
}

function obj_provider_update(obj, year, gen){
    if(year<9){
        year = 9;
    }
    if(year>70){
        year = 70;
    }
    obj.total +=1;
    if(gen==8507){
        obj.male["d"+year] += 1;
    }else{
        obj.female["d"+year] += 1;
    }
    return obj;
}

Date.prototype.format = function(f) {
    if (!this.valueOf()) return " ";

    var weekName = ["일요일", "월요일", "화요일", "수요일", "목요일", "금요일", "토요일"];
    var d = this;
        
    return f.replace(/(yyyy|yy|MM|dd|E|hh|mm|ss|a\/p)/gi, function($1) {
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

    String.prototype.string = function(len){var s = '', i = 0; while (i++ < len) { s += this; } return s;};
    String.prototype.zf = function(len){return "0".string(len - this.length) + this;};
    Number.prototype.zf = function(len){return this.toString().zf(len);};

// 로그 프린터 함수
function lp(title,mes){
    console.log();
    console.log("---"+title+"---");
    console.log(mes);
    console.log();
}



/// 유용한 함수
function util_console_print(mes){
    console.log(mes);
    console.log();
}
