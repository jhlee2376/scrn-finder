const { class_drug, class_measurement, class_condition, class_visit,class_person,class_distribution_result} = require('./class.js');
const { Pool, Client } = require('pg')
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
var CdmDbConfig = 
  {
    user: 'postgres',
    host: '192.1.173.230',
    database: 'postgres',
    password: 'scrn123!!',
    port: 5432
  }


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
  var study_id = Number(req.param('study_id'));
  var query_id = Number(req.param('query_id'));

  var query ='';

  //let result ;
  asyncMain(study_id, query_id);
  
  ////Async 함수
  async function asyncMain(study_id, query_id) {
      try{
          let pool =  await sql.connect(dbConfig); //scrn_cloud
         //console.log(study_id, query_id);
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
          util_console_print({"2. 조건쿼리 받아서 쿼리문 만들기":result.recordset.length});

          var str_or_weak  = asyncMain_2_0_assort_strength(result);  //console.log({result_str_or_weak: str_or_weak}); // 강한조건, 유연조건 나누기. 
          var query_list = asyncMain_2_1_make_query(str_or_weak);

          // 3. 마지막쿼리 돌려서 결과값 넣기.
          console.log("----3번시작")
          sql.close();
          // pool =  await sql.connect(CdmDbConfig); //scrn_cloud
          // var query_result =[];
          // console.log({query_list:query_list})
          // result = query_list;
          // result = await pool.request()
          // .query(query_list[query_list.length-1]);



          ///////////////////////////postgre
          
          //var q = `SELECT provider_id, A.person_id, A.year_of_birth, A.gender_concept_id FROM ( SELECT b2.provider_id, b2.person_id, p1.year_of_birth, p1.gender_concept_id FROM ( SELECT DISTINCT a0.person_id, a0.provider_id FROM ( SELECT person_id, provider_id FROM person WHERE person_id  >0 and year_of_birth <= 1998 and year_of_birth >= 1948  ) a0  ) b0, ( SELECT DISTINCT a0.person_id, a0.provider_id FROM ( SELECT person_id, provider_id FROM drug_exposure WHERE drug_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE FIFTH LIKE \'%sodium chloride%\')) a0  ) b1, ( SELECT DISTINCT a0.person_id, a0.provider_id FROM ( SELECT person_id, provider_id FROM  CONDITION_OCCURRENCE WHERE condition_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.icd10_snomed WHERE SMALL LIKE \'%I10 Essential (primary) hypertension%\')) a0  ) b2 , person p1 WHERE b0.person_id = b1.person_id and b1.person_id = b2.person_id and b0.person_id = b2.person_id and b2.person_id = p1.person_id ) A LEFT JOIN ( SELECT c0.person_id FROM ( ( SELECT person_id, provider_id FROM drug_exposure WHERE drug_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE THIRD LIKE \'%M10 Gout%\'))  ) c0 ) B ON A.person_id = B.person_id WHERE B.person_id IS NULL`
          //console.log(q.substring(132));

     

         
            const client = new Client(CdmDbConfig)
            await client.connect()
            console.log('query start!!')
            result = await client.query(query_list[query_list.length-1]);
            //console.log(result);
            await client.end();
                
      


        //   ///////////////////////////postgre end
          
          //4. 기존 쿼리 결과 지우기
          //change dbconfig
          //sql.close();
          pool =  await sql.connect(dbConfig); //scrn_cloud
          console.log("----4번시작");
          var result_3 = make_query_insert_1(result);
          q = "DELETE FROM doctor_result WHERE study_id = " + study_id + " and qeury_id = " + query_id;
          console.log(q)
          result = await pool.request()
          .query(q);
          util_console_print({"4. 기존쿼리결과지우기":result});
          
          //5. 쿼리 삽입하기.
          console.log("----5번시작");
          var query_5 = make_query_insert_5(result_3);
          result = await pool.request()
          .query(query_5);
          
          // 6.실행 버튼 클릭 시 '진행' 상태 변경하기.
          console.log('6 .상태 업데이트')
         result = await pool.request() 
          .input('input_paramiter1', sql.Int, study_id)
          .input('input_paramiter2', sql.Int, query_id)
          .query(`UPDATE query SET query_status = N'Done' WHERE study_id = @input_paramiter1 and query_id = @input_paramiter2`)
          //console.log(`UPDATE protocol SET protocol_status = N'InProgress' WHERE protocol_id = @input_paramiter`);
          util_console_print({"6.상태 업데이트": result});

      res.send({resunt:{result:result}});
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
              if(rows[i].strength==1 || rows[i].strength==null ){
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
              inclusion = make_query_0(in_rows,1);              // 선정 1, 제외 0 
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

              c_id = rows[i].criteria_id;
              //console.log("SELECT person_id FROM "+ cdm[table].tableName + " WHERE "+cdm[table].fieldSet[field]+" "+condition[con]+ " " + val );
              //condition확인
              //console.log(table);
              switch (Number(table)) {
                  //person
                  case 0:
                      if(field==0){ //0 젠더
                          result.person.push("( SELECT person_id, provider_id FROM bigdatadb.person WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                      }else{
                          result.person.push("( SELECT person_id, provider_id FROM bigdatadb.person WHERE person_id  >0 " +rows[i].criteria_addition+ " )");
                      }
                      
                      break;
                  //drug
                  case 1:
                      result.drug.push("( SELECT person_id, provider_id FROM bigdatadb.drug_exposure WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                      break;
                  //condition
                  case 2:
                      result.condition.push("( SELECT person_id, provider_id FROM bigdatadb.condition_occurrence WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                      break;
                  //measurement
                  case 3:
                  if(rows[i].measurement_count!=''||rows[i].measurement_method!=''){
                      result.measurement.push("( SELECT person_id, provider_id FROM bigdatadb.measurement WHERE person_id IN (  ( SELECT person_id FROM ( SELECT A.person_id  "+ measurement_method2( rows[i].measurement_method) +" FROM ( SELECT ROW_NUMBER() OVER(partition by person_id ORDER BY measurement_date DESC ) AS num, person_id, measurement_date, value_as_number FROM bigdatadb.MEASUREMENT WHERE measurement_concept_id = "+ rows[i].criteria_detail_value + " "+ measurement_date(rows[i].date_addition_value)+ " ) A " +measurement_num(rows[i].measurement_count )+ " GROUP BY A.person_id ) B "+measurement_min_max(rows[i].value_addition_min, rows[i].value_addition_max,rows[i].min_condition, rows[i].max_condition) +")))" );
                  }else{
                      result.measurement.push("( SELECT person_id, provider_id FROM bigdatadb.measurement WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val +")");
                  }
                      
                      break;
                  //mtdb 약2
                  case 5:
                      //console.log(`'%${title}%'`);
                      // SELECT concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE SMALL = "%Gout%" 
                      result.drug.push(`( SELECT person_id, provider_id FROM bigdatadb.drug_exposure WHERE drug_concept_id IN (SELECT distinct concept_id FROM bigdatadb.atc_rxnorm_all WHERE ${select_lms_atc[field-1]} LIKE \'${title}\'))`);
                      break;

                  //mtdb 병력
                  case 6:
                      // SELECT concept_id FROM mtdb.dbo.icd10_snomed WHERE SMALL = "%Gout%" 
                      //console.log(`'${title}'`)
                      result.condition.push(`( SELECT person_id, provider_id FROM  bigdatadb.CONDITION_OCCURRENCE WHERE condition_concept_id IN (SELECT distinct concept_id FROM bigdatadb.icd10_snomed WHERE ${select_lms[field-1]} LIKE \'${title}\'))`);
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
  
              var where_f = ", bigdatadb.person p1 WHERE "
  
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
      function make_query_insert_1(result){
          var q_execute = []; var q_cnt = 0; // 1000개 이상일 경우 
          if (result != null) {

              let rows = result.rows
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
                  }
                  //console.log(q_list);
                  //res.send(q_list);


                  //make_query_execute2(rows);
                  // --원본--
                  console.log("q_execute start")

                  for (i = 0; i < q_list.length; i++) {
                      if (q_list[i].total > 0) {
                          q_execute.push("(6, " + q_list[i].id + ", " + q_list[i].total + ", " + query_id + ", " + study_id + ", N'" + output_string(q_list[i].male) + "', N'" + output_string(q_list[i].female) + "')");

                      }
                  }
                  //console.log("q_execute");
                  //console.log(q_execute)
                  //res.send(q_execute);
                  return q_execute;
              } else {
                  console.log({ result: 0 })
              }
              console.log(err);
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
          return q_list1;
      }//make_query_insert_5

  }//async
});///test-site-execute/:study_id/:query_id

//Protocol 
app.get('/test-protocol-execute/:protocol_id/', (req,res)=>{
  util_console_print("----test-protocol-execute/:protocol_id/:user_id----");
  var protocol_id = Number(req.param('protocol_id'));
  var query ='';
  
  //let result ;
  asyncMain(protocol_id);
  
  ////Async 함수
  async function asyncMain(protocol_id) {
      try{
          let pool =  await sql.connect(dbConfig); //scrn_cloud
         //console.log(study_id, query_id);
          // 1.실행 버튼 클릭 시 '진행' 상태 변경하기.
          console.log('1.상태 업데이트')
         let result = await pool.request() 
          .input('input_paramiter1', sql.Int, protocol_id)
          .query(`UPDATE protocol SET protocol_status = N'In Progress' WHERE protocol_id = @input_paramiter1`)
          //console.log(`UPDATE protocol SET protocol_status = N'InProgress' WHERE protocol_id = @input_paramiter`);
          util_console_print({"1.상태 업데이트": result});
         
          // 2. 조건 쿼리 받아와서 쿼리문 만들기.
          console.log('2. 조건쿼리 받아서 쿼리문 만들기');
          result = await pool.request()
          .input('input_paramiter1', sql.Int, protocol_id)
          .query(`SELECT * FROM protocol_criteria_detail WHERE protocol_id = @input_paramiter1 `)
          util_console_print({"2. 조건쿼리 받아서 쿼리문 만들기":result.recordset.length});

          var str_or_weak  = asyncMain_2_0_assort_strength(result);  //console.log({result_str_or_weak: str_or_weak}); // 강한조건, 유연조건 나누기. 
          var query_list = asyncMain_2_1_make_query(str_or_weak);
          console.log({query_list:query_list});


          
          //////////////////////////////////////////Postgresql
          // 3. 마지막쿼리 돌려서 결과값 넣기.
          // console.log("----3번시작")
          // sql.close();
          // pool =  await sql.connect(CdmDbConfig); //scrn_cloud
          // var query_result =[];
          
          // result = await pool.request()
          // .query(query_list[query_list.length-1]);
          //////////////////////////////////////////Postgresql
          console.log("----3번시작")
          const client = new Client(CdmDbConfig)
          await client.connect()
          console.log('query start!!')
          result = await client.query(query_list[query_list.length-1]);
          //console.log(result);
          await client.end();



          //4. 기존 쿼리 결과 지우기
          //change dbconfig
         
          //pool =  await sql.connect(dbConfig); //scrn_cloud
          console.log("----4번시작");
          var result_3 = make_query_insert_1(result,protocol_id);
          q = "DELETE FROM protocol_result WHERE protocol_id = " + protocol_id ;
          //console.log(q)
          result = await pool.request()
          .query(q);
          util_console_print({"4. 기존쿼리결과지우기":result});
          
          //5. 쿼리 삽입하기.
          // protocol_result 환자리스트 넣기
          console.log("----5번시작");
          var query_5 = make_query_insert_5(result_3);
          result = await pool.request()
          .query(query_5);

          //6. protocol_detail_result
          // protocol_result 환자리스트 넣기
          let person_list = '';

          let drug_info = [];
          let measurement_info = [];
          let condition_info = [];
          let visit_info =[];
          let person_info =[];

          let result_person = await pool.request()
              .input('input_paramiter', sql.Int, protocol_id)
              .query('SELECT person_id FROM dbo.protocol_result WHERE protocol_id = @input_paramiter')
          person_list = make_list_to_string(result_person);  //console_println(person_list); //log찍기

          //// 환자
          let result_person1 = await pool.request()
              .query('SELECT gender_concept_id, year_of_birth FROM BIGDATADB.dbo.PERSON WHERE person_id in ( '+person_list+' );')
          person_info = make_result_person_to_list(result_person1.recordset);
          
          var newdate = new Date();
          var date_start = `'${newdate.getFullYear()-5}-${newdate.getMonth()+1}-${newdate.getDate()}'`;
         
          ////약.
          let result_drug = await pool.request()
              .query('SELECT p1.gender_concept_id, d1.drug_concept_id, d1.drug_exposure_start_date, d1.drug_exposure_end_date, d1.quantity, d1.days_supply, d1.dose_unit_source_value FROM (SELECT person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, quantity, days_supply, dose_unit_source_value FROM BIGDATADB.dbo.DRUG_EXPOSURE WHERE BIGDATADB.dbo.DRUG_EXPOSURE.person_id in('+person_list+')) d1, BIGDATADB.dbo.PERSON p1 WHERE d1.person_id = p1.person_id and drug_exposure_start_date >= '+date_start);
          // durg 결과 내용을 리스트로 만들기.
          drug_info = make_result_drug_to_list(result_drug.recordset);    //console_println(drug_info.length);

          ////Lab
          let result_measurement = await pool.request()
              .query('SELECT p1.gender_concept_id, d1.measurement_concept_id, d1.measurement_date, d1.value_as_number, d1.range_low, d1.range_high, d1.unit_source_value FROM (SELECT person_id, measurement_concept_id, measurement_date, value_as_number, range_low, range_high, unit_source_value FROM BIGDATADB.dbo.MEASUREMENT WHERE BIGDATADB.dbo.MEASUREMENT.person_id in('+person_list+')) d1, BIGDATADB.dbo.PERSON p1 WHERE d1.person_id = p1.person_id and d1.measurement_date >='+ date_start);
          measurement_info= make_result_measurement_to_list(result_measurement.recordset);    //console_println(measurement_info.length);

          ////Condition
          let result_condition = await pool.request()
              .query('SELECT p1.gender_concept_id, d1.condition_concept_id, d1.condition_start_date FROM (SELECT person_id, condition_concept_id, condition_start_date FROM BIGDATADB.dbo.CONDITION_OCCURRENCE WHERE BIGDATADB.dbo.CONDITION_OCCURRENCE.person_id in('+person_list+')) d1, BIGDATADB.dbo.PERSON p1 WHERE d1.person_id = p1.person_id and d1.condition_start_date >= '+ date_start);
          condition_info= make_result_condition_to_list(result_condition.recordset);  //console_println(condition_info.length);

          ////Visit
          let result_visit = await pool.request()
          .query('SELECT p1.gender_concept_id, d1.visit_concept_id, d1.visit_start_date, d1.visit_end_date, d1.care_site_id FROM (SELECT person_id, visit_concept_id, visit_start_date, visit_end_date,care_site_id FROM BIGDATADB.dbo.VISIT_OCCURRENCE WHERE BIGDATADB.dbo.VISIT_OCCURRENCE.person_id in('+person_list+')) d1, BIGDATADB.dbo.PERSON p1 WHERE d1.person_id = p1.person_id and d1.visit_start_date >= '+date_start);
          visit_info= make_result_visit_to_list(result_visit.recordset); //console_println(visit_info);

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
          let distribution_condition      = make_distribution_condition(result1.condition_info);
          let distribution_drug           = make_distribution_drug(result1.drug_info);
          let distribution_measurement    = make_distribution_measurement(result1.measurement_info);

          // 삭제
          lp('query_delete_distribution','')
          let query_delete_distribution = await pool.request()
              .input('protocol_id',sql.Int, protocol_id)
              .query('DELETE FROM protocol_result1 WHERE protocol_id = @protocol_id')
              console.log(query_delete_distribution);
          
          // 삽입
          lp('query_insert_distribution','')
          let query_insert_distribution = await pool.request()
              .input('protocol_id',sql.Int, protocol_id)
              .input('input_person', sql.NVarChar, JSON.stringify(distribution_person))
              .input('input_condition', sql.NVarChar, JSON.stringify(distribution_condition))
              .input('input_drug', sql.NVarChar, JSON.stringify(distribution_drug))
              .input('input_measurement', sql.NVarChar, JSON.stringify(distribution_measurement))
              .query('INSERT INTO protocol_detail_result VALUES( @protocol_id, @input_person, @input_condition, @input_drug, @input_measurement)')
              console.log(query_insert_distribution);



              // 99.실행 버튼 클릭 시 '진행' 상태 변경하기.
          console.log('99.상태 업데이트')
          result = await pool.request() 
           .input('input_paramiter1', sql.Int, protocol_id)
           .query(`UPDATE protocol SET protocol_status = N'Done' WHERE protocol_id = @input_paramiter1`)
           //console.log(`UPDATE protocol SET protocol_status = N'InProgress' WHERE protocol_id = @input_paramiter`);
           util_console_print({"99.상태 업데이트": result});

      res.send({resunt:{query_insert_distribution:query_insert_distribution}});
      sql.close();    
      }catch(err){
          util_console_print({error: err});
          res.send({error:'2.result is 0'});

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
          var isNull;
          
          var result =[];
          console.log(condition_info.length);
          for(i=0;i<condition_info.length;i++){        
              //i=0
              if(i==0){
                  result_condition_name.push(rows[i].condition_concept_id);
                  result_condition_value.push(1);
              }else{
                  isNull = result_condition_name.indexOf(rows[i].condition_concept_id) >-1;
                  if(!isNull){
                      result_condition_name.push(rows[i].condition_concept_id);
                      result_condition_value.push(1);
                  }else{
                      result_condition_value[result_condition_name.indexOf(rows[i].condition_concept_id)] +=1
                  }
              }
          }// end for

          // 객체에 넣기
          var chk =0;
          for(i=0;i<result_condition_name.length;i++){
              result.push(class_distribution_result(result_condition_name[i], result_condition_value[i]));
              chk += result_condition_value[i];
          }
          console.log(chk);
          return result;
          
      } //make_distribution_condition
      function make_distribution_drug(drug_info){
          lp("make_distribution_drug","");
          var rows = drug_info;

          var result_drug_name =[];
          var result_drug_value =[];
          var isNull;
          
          var result =[];
          console.log(rows.length);
          for(i=0;i<rows.length;i++){        
              //i=0
              if(i==0){
                  result_drug_name.push(rows[i].drug_concept_id);
                  result_drug_value.push(1);
              }else{
                  isNull = result_drug_name.indexOf(rows[i].drug_concept_id) >-1;
                  if(!isNull){
                      result_drug_name.push(rows[i].drug_concept_id);
                      result_drug_value.push(1);
                  }else{
                      result_drug_value[result_drug_name.indexOf(rows[i].drug_concept_id)] +=1
                  }
              }
          }// end for

          // 객체에 넣기
          var chk=0; // 잘 저장됐는지 검토하기.
          for(i=0;i<result_drug_name.length;i++){
              result.push(class_distribution_result(result_drug_name[i], result_drug_value[i]));
              chk += result_drug_value[i];
          }
          console.log(chk)
          return result;

      } //make_distribution_drug     
      function make_distribution_measurement(measurement_info){
          lp("make_distribution_measurement","");
          var rows = measurement_info;

          var result_measurement_name =[];
          var result_measurement_value =[];
          var isNull;
          
          var result =[];
          console.log(rows.length);
          for(i=0;i<rows.length;i++){        
              //i=0
              if(i==0){
                  result_measurement_name.push(rows[i].measurement_concept_id);
                  result_measurement_value.push(1);
              }else{
                  isNull = result_measurement_name.indexOf(rows[i].measurement_concept_id) >-1;
                  if(!isNull){
                      result_measurement_name.push(rows[i].measurement_concept_id);
                      result_measurement_value.push(1);
                  }else{
                      result_measurement_value[result_measurement_name.indexOf(rows[i].measurement_concept_id)] +=1
                  }
              }
          }// end for

          // 객체에 넣기
          var chk=0; // 잘 저장됐는지 검토하기.
          for(i=0;i<result_measurement_name.length;i++){
              result.push(class_distribution_result(result_measurement_name[i], result_measurement_value[i]));
              chk += result_measurement_value[i];
          }
          console.log(chk)
          return result;
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
              if(rows[i].strength==1 || rows[i].strength==null ){
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
              if(rows[i].criteria_addition.length >0){
                  val += " " +rows[i].criteria_addition;
              }

              c_id = rows[i].criteria_id;
              //console.log("SELECT person_id FROM "+ cdm[table].tableName + " WHERE "+cdm[table].fieldSet[field]+" "+condition[con]+ " " + val );
              //condition확인
              //console.log(table);
              switch (Number(table)) {
                  //person
                  case 0:
                      if(field==0){ //0 젠더
                          result.person.push("( SELECT person_id, provider_id FROM bigdatadb.person WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                      }else{
                          result.person.push("( SELECT person_id, provider_id FROM bigdatadb.person WHERE person_id  >0 " +rows[i].criteria_addition+ " )");
                      }
                      
                      break;
                  //drug
                  case 1:
                      result.drug.push("( SELECT person_id, provider_id FROM bigdatadb.drug_exposure WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                      break;
                  //condition
                  case 2:
                      result.condition.push("( SELECT person_id, provider_id FROM bigdatadb.condition_occurrence WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " )");
                      break;
                  //measurement
                  case 3:
                  if(rows[i].measurement_count!=''||rows[i].measurement_method!=''){
                      result.measurement.push("( SELECT person_id, provider_id FROM bigdatadb.measurement WHERE person_id IN (  ( SELECT person_id FROM ( SELECT A.person_id  "+ measurement_method2( rows[i].measurement_method) +" FROM ( SELECT ROW_NUMBER() OVER(partition by person_id ORDER BY measurement_date DESC ) AS num, person_id, measurement_date, value_as_number FROM dbo.MEASUREMENT WHERE measurement_concept_id = "+ rows[i].criteria_detail_value + " "+ measurement_date(rows[i].date_addition_value)+ " ) A " +measurement_num(rows[i].measurement_count )+ " GROUP BY A.person_id ) B "+measurement_min_max(rows[i].value_addition_min, rows[i].value_addition_max,rows[i].min_condition, rows[i].max_condition) +")))" );
                  }else{
                      result.measurement.push("( SELECT person_id, provider_id FROM bigdatadb.measurement WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val +")");
                  }
                      
                      break;
                  //mtdb 약2
                  case 5:
                      // SELECT concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE SMALL = "%Gout%" 
                      result.drug.push(`( SELECT person_id, provider_id FROM bigdatadb.drug_exposure WHERE drug_concept_id IN (SELECT distinct concept_id FROM bigdatadb.atc_rxnorm_all WHERE ${select_lms_atc[field-1]} LIKE '%${title}%'))`);
                      break;

                  //mtdb 병력
                  case 6:
                      // SELECT concept_id FROM mtdb.dbo.icd10_snomed WHERE SMALL = "%Gout%" 
                      result.condition.push(`( SELECT person_id, provider_id FROM  bigdatadb.CONDITION_OCCURRENCE WHERE condition_concept_id IN (SELECT distinct concept_id FROM bigdatadb.icd10_snomed WHERE ${select_lms[field-1]} LIKE '%${title}%'))`);
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
  
              var where_f = ", bigdatadb.person p1 WHERE "
  
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
              query = "SELECT DISTINCT A.person_id FROM ( " + inclusion + " ) A  "
          } else {
              query = "SELECT DISTINCT A.person_id FROM FROM ( " + inclusion + " ) A LEFT JOIN ( " + exclusion + " ) B ON A.person_id = B.person_id WHERE B.person_id IS NULL"
          }
          return query;
      }
      function make_query_insert_1(result, protocol_id){
          var q_execute = []; var q_cnt = 0; // 1000개 이상일 경우 
          if (result != null) {

              let rows = result.rows
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
                          q_execute.push("( "+rows[i].person_id+", "+protocol_id+" )");
                  }
                  //console.log("q_execute");
                  //console.log(q_execute)
                  //res.send(q_execute);
                  return q_execute;
              }else {
                  console.log({ result: 0 })
              }
              console.log(err);
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
                  q_final.push("INSERT INTO protocol_result VALUES " + query + " ;");
                  cnt_1000 = 0;
                  query = "";
              } else {
                  if (i + 1 != q_execute.length) {
                      query += ", ";
                  }
              }
          }
  
          q_final.push("INSERT INTO protocol_result VALUES " + query + " ;");
          //res.send(q_final);
          console.log("q_final.length: " + q_final.length);
          q_list1 = "";
          for (i = 0; i < q_final.length; i++) {
              q_list1 += q_final[i];
          }
          return q_list1;
      }//make_query_insert_5

  }//async
});//test-site-execute/:study_id/:query_id






// 1092374098123498378241234124124324123412341242142134
// 웹 서버를 실행
http.createServer(app).listen(52273, function(req, res){
  console.log('Server Running at http://127.0.0.1:52273');
  console.log();
});
// 1092374098123498378241234124124324123412341242142134
///////////////////////////////////////////////////////////////////////////////////////////


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