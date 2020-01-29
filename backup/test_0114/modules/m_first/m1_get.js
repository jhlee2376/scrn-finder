const {Lab, Atc_code,Icd10_code,
    class_drug, 
    class_measurement, 
    class_condition, class_visit,class_person,class_distribution_result,class_protocol_condition,class_protocol_measurement,class_distribution_result_measurement } = require('./class.js');

const {dbConfig, CdmDbConfig} = require('../config/dbconfig');


let     chalk       = require('chalk');

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



async function Get_condition_list(study_id, query_id){
    console.log(chalk.red(`-----------------Start: Get_condition_list--------------------`));
    console.log();
    try{
        let result = '';
        let sql = require('mssql');
        let pool = await sql.connect(dbConfig);

        console.log(chalk.cyan('START----1.상태업데이트------'));

        result = await pool.request()
        .input('input1',sql.Int,study_id)
        .input('input2',sql.Int,query_id)
        .query(`UPDATE query SET query_status ='In progress' WHERE study_id = @input1 and query_id = @input2`);
        console.log({result:result})
        console.log(chalk.cyan('END----1.상태업데이트------'));
        console.log();

        console.log(chalk.cyan('START----2.조건 가져오기------'));
        result = await pool.request()
        .input('input1',sql.Int,study_id)
        .input('input2',sql.Int,query_id)
        .query(`SELECT * FROM criteria_detail WHERE study_id = @input1 and query_id = @input2`)
        console.log(chalk.cyan('END----2.조건 가져오기------'));
        console.log();

        pool.close();
        sql.close();
        return result;
    }catch(err){
        console.log({get_config_ERROR : err});
        return 0;
        //sql.close();
    }finally{
        console.log(chalk.red(`----------------End: Get_condition_list--------------------`));
        console.log();
    }
   


}//get_config
async function Make_query(result, study_id, query_id ){
    console.log();
    console.log(chalk.red(`-----------------Start: Make_query--------------------`));
    console.log();
   

    // 카테고리화 문제 때문에 
    // table 값이 6인경우 mtdb에서 해당 id값을 가져와야함 
    // criteria_table 에 텍스트 값을 비교하여 6: 진단, 5: 약을 구분 
    // criteria_detail_attribute , {1,2,3}, {대, 중, 소}
    // SELECT concept_id FROM mtdb.dbo.icd10_snomed WHERE SMALL = "%Gout%"
    try{
        let select_table = ['atc_rxnorm_all', 'icd10_snomed']; // -5
        let select_lms = ['LARGE', 'MID', 'SMALL']; // -1 
        let select_lms_atc = ['FIRST', 'SECOND', 'THIRD', 'FORTH', 'FIFTH'];
        let condition =["=","=","=","=","="];

        let output =[];
        let rows = result.recordset;
        let final_out=''
        console.log({rows:rows.length});
        
        if(rows.length ==0){//결과값이 없을 경우 
            return '';
        }else{
            for(i=0;i<rows.length;i++){
                output.push(rows[i]);
                table   = rows[i].criteria_detail_table; //0
                field   = rows[i].criteria_detail_attribute; //0
                con     = rows[i].criteria_detail_condition;
                val     = rows[i].criteria_detail_value;
                title   = rows[i].criteria_title;

                c_add=''
                if(rows[i].criteria_addition.length >0 && rows[i].criteria_addition!='null' ){
                    val += " " +rows[i].criteria_addition;
                    c_add = rows[i].criteria_addition;
                }

                switch(Number(table)){
                    //person
                    case 0:
                        if(field == 0 ){ //gender
                            output[i].query = " SELECT DISTINCT person_id, provider_id FROM person WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " ";                    
                        }else{
                            output[i].query = " SELECT DISTINCT person_id, provider_id FROM person WHERE 1=1 " +rows[i].criteria_addition+ " ";
                        }
                        break;
        
                    //measurement
                    case 3:
                        let q_measurement_buf = ''
                        let q_measurement = ''

                        //검사 카운트 별 avg, min, max
                        if(rows[i].measurement_count!=''||rows[i].measurement_method!=''){
                            q_measurement_buf = " SELECT person_id, provider_id, measurement_date  FROM measurement WHERE person_id IN (  ( SELECT person_id FROM ( SELECT A.person_id  "+ measurement_method2( rows[i].measurement_method) +" FROM ( SELECT ROW_NUMBER() OVER(partition by person_id ORDER BY measurement_date DESC ) AS num, person_id, measurement_date, value_as_number FROM dbo.MEASUREMENT WHERE measurement_concept_id = "+ rows[i].criteria_detail_value + " "+ measurement_date(rows[i].date_addition_value)+ " ) A " +measurement_num(rows[i].measurement_count )+ " GROUP BY A.person_id ) B "+measurement_min_max(rows[i].value_addition_min, rows[i].value_addition_max,rows[i].min_condition, rows[i].max_condition) +"))"
                            
                        }else{
                            q_measurement_buf =  " SELECT person_id, provider_id, measurement_date FROM measurement WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val +"";
                        }
        
                        if(con ==0){
                            q_measurement = ` SELECT person_id, provider_id FROM (${q_measurement_buf}) ff1 `
                        }else if(con==1){
                            q_measurement = ` SELECT person_id, provider_id FROM ( SELECT ff1.person_id, ff1.provider_id, ff1.measurement_date FROM ${q_measurement_buf} ff1 WHERE ff1.measurement_date = ( SELECT MIN(ff2.measurement_date) FROM ${q_measurement_buf} ff2 WHERE ff2.person_id = ff1.person_id) ${c_add} ) m1`
                        }else{
                            q_measurement = ` SELECT person_id, provider_id FROM ( SELECT ff1.person_id, ff1.provider_id, ff1.measurement_date FROM ${q_measurement_buf} ff1 WHERE ff1.measurement_date = ( SELECT MAX(ff2.measurement_date) FROM ${q_measurement_buf} ff2 WHERE ff2.person_id = ff1.person_id) ${c_add} ) m1`
        
                        }// con
                        // console.log({"검사_테스트":q_measurement})
                        output[i].query =q_measurement
                    break;
                        
                  
                    case 4:
                      //VISIT
                        output[i].query =` SELECT DISTINCT person_id, provider_id FROM VISIT_OCCURRENCE WHERE 1=1 ${rows[i].criteria_addition} `;
                        break;
        
                    //mtdb 약2
                    case 5:
                        //console.log(`'%${title}%'`);
                        // SELECT concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE SMALL = "%Gout%" 
                        mtdb_drug_buf='';
                        mtdb_drug_out='';
                        if(con ==0){
                            mtdb_drug_buf = ` SELECT DISTINCT person_id, provider_id, drug_exposure_start_date FROM drug_exposure WHERE drug_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE ${select_lms_atc[field-1]} LIKE '%${title}%') ${c_add}`;
                            mtdb_drug_out = mtdb_drug_buf;
                        }else if(con==1){   //min
                            mtdb_drug_buf = ` SELECT DISTINCT person_id, provider_id, drug_exposure_start_date FROM drug_exposure WHERE drug_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE ${select_lms_atc[field-1]} LIKE '%${title}%') `;
                            mtdb_drug_out = ` SELECT * FROM ( ${mtdb_drug_buf} ) f2 WHERE drug_exposure_start_date = ( SELECT MIN(drug_exposure_start_date) FROM ( ${mtdb_drug_buf} ) f3 WHERE f3.person_id = f2.person_id)  ${c_add} `
                        }else{              //max
                            mtdb_drug_buf = ` SELECT DISTINCT person_id, provider_id, drug_exposure_start_date FROM drug_exposure WHERE drug_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE ${select_lms_atc[field-1]} LIKE '%${title}%') `;
                            mtdb_drug_out = ` SELECT * FROM ( ${mtdb_drug_buf} ) f2 WHERE drug_exposure_start_date = ( SELECT MIN(drug_exposure_start_date) FROM ( ${mtdb_drug_buf} ) f3 WHERE f3.person_id = f2.person_id)  ${c_add} `
                        }
                        
                        mtdb_drug_out = `SELECT DISTINCT o1.person_id, o1.provider_id FROM (${mtdb_drug_out}) o1`;
                        output[i].query =mtdb_drug_out;
                        //console.log({c_add:rows[i].criteria_addition})
                        break;

                    case 6:
                        //mtdb 병력                  
                        // SELECT concept_id FROM mtdb.dbo.icd10_snomed WHERE SMALL = "%Gout%" 
                        //console.log(`'${title}'`)
                        mtdb_condition_buf = '';
                        mtdb_condition_out = '';
                        if(con==0){
                            mtdb_condition_buf = ` SELECT DISTINCT person_id, provider_id, condition_start_date FROM CONDITION_OCCURRENCE WHERE condition_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.icd10_snomed_category WHERE ${select_lms[field-1]} LIKE '%${title}%' ) ${c_add}`;
                            mtdb_condition_out = mtdb_condition_buf;
                        }else if(con==1) {
                            mtdb_condition_buf = ` SELECT DISTINCT person_id, provider_id, condition_start_date FROM CONDITION_OCCURRENCE WHERE condition_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.icd10_snomed_category WHERE ${select_lms[field-1]} LIKE '%${title}%' ) `
                            mtdb_condition_out = ` SELECT  * FROM (${mtdb_condition_buf}) f2 WHERE condition_start_date = ( SELECT MIN(condition_start_date ) FROM (${mtdb_condition_buf}) f3 WHERE f3.person_id = f2.person_id ) ${c_add} `
                        }else{
                            mtdb_condition_buf = ` SELECT DISTINCT person_id, provider_id, condition_start_date FROM CONDITION_OCCURRENCE WHERE condition_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.icd10_snomed_category WHERE ${select_lms[field-1]} LIKE '%${title}%' ) `
                            mtdb_condition_out = ` SELECT  * FROM (${mtdb_condition_buf}) f2 WHERE condition_start_date = ( SELECT MAX(condition_start_date ) FROM (${mtdb_condition_buf}) f3 WHERE f3.person_id = f2.person_id ) ${c_add} `
                        }//if con 

                        mtdb_condition_out = ` SELECT DISTINCT o1.person_id, o1.provider_id FROM (${mtdb_condition_out}) o1`;
                        output[i].query =mtdb_condition_out
                        //console.log({c_add:rows[i].criteria_addition})
                        break;

                    case 9:
                        //user Define
                        //table = 9, detail_value = user_define_id
                        try{
                            console.log();
                            console.log("UD");
                            let query_list_user_define = [];
                            let sql_ud = require('mssql');
                            let pool_ud = await sql_ud.connect(dbConfig);
                            id_ud = val;
                            //ud = user_define
                            result_ud = await pool_ud.request()
                                .input('input_param', sql_ud.Int, id_ud)
                                .query(`SELECT * FROM user_criteria_detail WHERE user_define_id = @input_param`)
                            //console.log({result_ud:result_ud.recordset});
                            pool_ud.close()
                            sql_ud.close();
                            // console.log({result_ud:result_ud})
                            ud_out = await make_query_for_UD(result_ud);
                            // console.log({query_list_ud:ud_out});
                            // query = '';
                            // for (k = 0; k < query_list_ud.length; k++) {
                            //     query += ` ${query_list_ud[k]} `;
                            //     if (k + 1 != query_list_ud.length) {
                            //         query += ` UNION `
                            //     }
                            // }
                            //query = query_list_to_query_3(query_list_ud)
                            //console.log({query_UD:query.last});
                            output[i].query =ud_out;
        
                            break;
                        }catch(err){
                            console.log({"user_define_ERROR":err});
                        }//catch
                }// switch 

            }//for


            //준비된 쿼리리스트로 최종 쿼리만들기.
            final_out = [];
            final_in = '';
            final_ex = '';
            final_last = '';
            if(output.length==1){
                final_out.push(output[0].query) ;
            }else{
                for(i=0;i<output.length;i++){
                    if(i==0){
                        final_in += ` SELECT DISTINCT f_i0.person_id, f_i0.provider_id FROM ( ${output[i].query}) f_i0 ` ;

                    }else if(i==1){
                        if(output[i].criteria_state==1){
                            final_in += ` INNER JOIN (${output[i].query}) f_i1 ON f_i0.person_id = f_i1.person_id `
                            
                        }else{
                            final_ex += output[i].query;
                        }
                    }else if(i>1){
                        if(output[i].criteria_state==1){
                            final_in += ` INNER JOIN (${output[i].query}) f_i${i} ON f_i${i-1}.person_id = f_i${i}.person_id `;
                            //final_last = ` SELECT person_id, provider_id FROM ( ${output[i].query} ) f_i${i} WHERE EXISTS ( SELECT 1 FROM ( ${ final_last }) f_i${i-1} WHERE f_i${i}.person_id = f_i${i-1}.person_id) `;
                        }else{
                            if(final_ex.length!=0){
                                final_ex += ` UNION ${output[i].query} `;
                            }else{
                                final_ex += output[i].query;
                            }
                        }
                    }
                    //
                    if(final_ex.length!=0){
                        final_out.push(` SELECT DISTINCT f0.person_id, f0.provider_id FROM ( ${final_in} ) f0 LEFT JOIN ( SELECT DISTINCT person_id, provider_id FROM (${final_ex}) ex0 ) f1 ON f0.person_id = f1.person_id WHERE f1.person_id IS NULL`);
                    }else{
                        final_out.push(` SELECT DISTINCT f0.person_id, f0.provider_id FROM ( ${final_in} ) f0 `);
                    }
                    
                }//for
                let idx = 0;
                for(i=0;i<output.length;i++){
                    if(output[i].criteria_detail_table==6){
                        idx = i;
                        break;
                    }
                }

                if(final_ex.length!=0){
                    final_last=` SELECT DISTINCT f0.person_id, f0.provider_id FROM ( ${output[idx].query}) f0 INNER JOIN ( ${final_out[final_out.length-1]}) fi_0 ON f0.person_id = fi_0.person_id LEFT JOIN ( SELECT DISTINCT person_id, provider_id FROM (${final_ex}) ex0 ) f1 ON f0.person_id = f1.person_id WHERE f1.person_id IS NULL`;
                     
                }else{
                    final_last=` SELECT DISTINCT f0.person_id, f0.provider_id FROM ( ${output[idx].query}) f0 INNER JOIN ( ${final_out[final_out.length-1]}) fi_0 ON f0.person_id = fi_0.person_id `;
                }
                console.log({final_last:final_last})
            }//else
        }//if output.length
        //console.log({Make_query:final_out});
        let sql_provider_id = require('mssql');
        let pool_provider_id = await sql_provider_id.connect(dbConfig);
       
        let result_provider_id = await pool_provider_id.request()
        .input('input1',sql_provider_id.Int, study_id )
        .query(`SELECT invest_num as provider_id FROM study WHERE study_id = @input1`);

        provider_id = result_provider_id.recordset[0].provider_id;
        console.log({provider_id : result_provider_id},{study_id:study_id});
        
        pool_provider_id.close();
        sql_provider_id.close();
        

        out = {
            rows: rows,
            final_out: final_out, 
            final_last: final_last,
            provider_id : provider_id,
        }
        //console.log(out.rows);
        return out;
    }catch(err){
        console.log({Make_query_ERROR:err})
    }//try, catch 
    finally{
        console.log(chalk.red(`-----------------END: Make_query--------------------`));
        console.log();
    }

} // Make_query
async function make_query_for_UD(result) {
    try{
        console.log();
        console.log(chalk.blue("START-----make_query_for_UD"));
        console.log();

        //console.log({result:result.recordset})
        //console.log({cdm:cdm});
        out_query_list_ud =[];
        // 카테고리화 문제 때문에 
        // table 값이 6인경우 mtdb에서 해당 id값을 가져와야함 
        // criteria_table 에 텍스트 값을 비교하여 6: 진단, 5: 약을 구분 
        // criteria_detail_attribute , {1,2,3}, {대, 중, 소}
        // SELECT concept_id FROM mtdb.dbo.icd10_snomed WHERE SMALL = "%Gout%" 
        var select_table = ['atc_rxnorm_all', 'icd10_snomed']; // -5
        var select_lms = ['LARGE', 'MID', 'SMALL','concept_name']; // -1 
        var select_lms_atc = ['FIRST', 'SECOND', 'THIRD', 'FORTH', 'FIFTH'];
    
        let rows = result.recordset;
        
        for(i_ud=0;i_ud<rows.length;i_ud++){
            //console.log(i_ud);
            table   = rows[i_ud].criteria_detail_table; //0
            field   = rows[i_ud].criteria_detail_attribute; //0
            con     = rows[i_ud].criteria_detail_condition;
            val     = rows[i_ud].criteria_detail_value;
            title   = rows[i_ud].criteria_title;
           
            if(rows[i_ud].criteria_addition.length >0 && rows[i_ud].criteria_addition!='null' ){
                val += " " +rows[i_ud].criteria_addition;
            }// if
            c_add = rows[i_ud].criteria_addition;
            q_buf=''
            switch(Number(table)){
                 case 0:
                    //person
                    if(field == 0 ){ //gender
                        output[i].query = " SELECT DISTINCT person_id, provider_id FROM person WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " ";                    
                    }else{
                        output[i].query = " SELECT DISTINCT person_id, provider_id FROM person WHERE 1=1 " +rows[i].criteria_addition+ " ";
                    }
                 break;
 
                    
                case 3:
                    //measurement
                    let q_measurement_buf = ''
                    let q_measurement = ''

                    //검사 카운트 별 avg, min, max
                    if(rows[i].measurement_count!=''||rows[i].measurement_method!=''){
                     q_measurement_buf = " SELECT person_id, provider_id, measurement_date  FROM measurement WHERE person_id IN (  ( SELECT person_id FROM ( SELECT A.person_id  "+ measurement_method2( rows[i].measurement_method) +" FROM ( SELECT ROW_NUMBER() OVER(partition by person_id ORDER BY measurement_date DESC ) AS num, person_id, measurement_date, value_as_number FROM dbo.MEASUREMENT WHERE measurement_concept_id = "+ rows[i].criteria_detail_value + " "+ measurement_date(rows[i].date_addition_value)+ " ) A " +measurement_num(rows[i].measurement_count )+ " GROUP BY A.person_id ) B "+measurement_min_max(rows[i].value_addition_min, rows[i].value_addition_max,rows[i].min_condition, rows[i].max_condition) +"))"
                     
                    }else{
                     q_measurement_buf =  " SELECT person_id, provider_id, measurement_date FROM measurement WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val +"";
                    }
 
                    if(con ==0){
                        q_measurement = ` SELECT person_id, provider_id FROM (${q_measurement_buf}) ff1 `
                    }else if(con==1){
                        q_measurement = ` SELECT person_id, provider_id FROM ( SELECT ff1.person_id, ff1.provider_id, ff1.measurement_date FROM ${q_measurement_buf} ff1 WHERE ff1.measurement_date = ( SELECT MIN(ff2.measurement_date) FROM ${q_measurement_buf} ff2 WHERE ff2.person_id = ff1.person_id) ${c_add} ) m1`
                    }else{
                        q_measurement = ` SELECT person_id, provider_id FROM ( SELECT ff1.person_id, ff1.provider_id, ff1.measurement_date FROM ${q_measurement_buf} ff1 WHERE ff1.measurement_date = ( SELECT MAX(ff2.measurement_date) FROM ${q_measurement_buf} ff2 WHERE ff2.person_id = ff1.person_id) ${c_add} ) m1`
    
                    }// con
                    //console.log({"검사_테스트":q_measurement})
                    output[i].query =q_measurement
                    break;

             case 4:
               //VISIT
                 output[i].query =` SELECT DISTINCT person_id, provider_id FROM VISIT_OCCURRENCE WHERE 1=1 ${rows[i].criteria_addition} `;
                 break;
 
            
             case 5:
                //mtdb 약2
                 //console.log(`'%${title}%'`);
                 // SELECT concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE SMALL = "%Gout%" 
                 mtdb_drug_buf='';
                 mtdb_drug_out='';
                 if(con ==0){
                     mtdb_drug_buf = ` SELECT DISTINCT person_id, provider_id, drug_exposure_start_date FROM drug_exposure WHERE drug_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE ${select_lms_atc[field-1]} LIKE '%${title}%') ${c_add}`;
                     mtdb_drug_out = mtdb_drug_buf;
                 }else if(con==1){   //min
                     mtdb_drug_buf = ` SELECT DISTINCT person_id, provider_id, drug_exposure_start_date FROM drug_exposure WHERE drug_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE ${select_lms_atc[field-1]} LIKE '%${title}%') `;
                     mtdb_drug_out = ` SELECT * FROM ( ${mtdb_drug_buf} ) f2 WHERE drug_exposure_start_date = ( SELECT MIN(drug_exposure_start_date) FROM ( ${mtdb_drug_buf} ) f3 WHERE f3.person_id = f2.person_id)  ${c_add} `
                 }else{              //max
                     mtdb_drug_buf = ` SELECT DISTINCT person_id, provider_id, drug_exposure_start_date FROM drug_exposure WHERE drug_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE ${select_lms_atc[field-1]} LIKE '%${title}%') `;
                     mtdb_drug_out = ` SELECT * FROM ( ${mtdb_drug_buf} ) f2 WHERE drug_exposure_start_date = ( SELECT MIN(drug_exposure_start_date) FROM ( ${mtdb_drug_buf} ) f3 WHERE f3.person_id = f2.person_id)  ${c_add} `
                 }
                 
                 mtdb_drug_out = `SELECT DISTINCT o1.person_id, o1.provider_id FROM (${mtdb_drug_out}) o1`;
                 output[i].query =mtdb_drug_out;
                 //console.log({c_add:rows[i].criteria_addition})
                 break;

             case 6:
                 //mtdb 병력                  
                 // SELECT concept_id FROM mtdb.dbo.icd10_snomed WHERE SMALL = "%Gout%" 
                 //console.log(`'${title}'`)
                 mtdb_condition_buf = '';
                 mtdb_condition_out = '';
                 if(con==0){
                     mtdb_condition_buf = ` SELECT DISTINCT person_id, provider_id, condition_start_date FROM CONDITION_OCCURRENCE WHERE condition_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.icd10_snomed_category WHERE ${select_lms[field-1]} LIKE '%${title}%' ) ${c_add}`;
                     mtdb_condition_out = mtdb_condition_buf;
                 }else if(con==1) {
                     mtdb_condition_buf = ` SELECT DISTINCT person_id, provider_id, condition_start_date FROM CONDITION_OCCURRENCE WHERE condition_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.icd10_snomed_category WHERE ${select_lms[field-1]} LIKE '%${title}%' ) `
                     mtdb_condition_out = ` SELECT  * FROM (${mtdb_condition_buf}) f2 WHERE condition_start_date = ( SELECT MIN(condition_start_date ) FROM (${mtdb_condition_buf}) f3 WHERE f3.person_id = f2.person_id ) ${c_add} `
                 }else{
                     mtdb_condition_buf = ` SELECT DISTINCT person_id, provider_id, condition_start_date FROM CONDITION_OCCURRENCE WHERE condition_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.icd10_snomed_category WHERE ${select_lms[field-1]} LIKE '%${title}%' ) `
                     mtdb_condition_out = ` SELECT  * FROM (${mtdb_condition_buf}) f2 WHERE condition_start_date = ( SELECT MAX(condition_start_date ) FROM (${mtdb_condition_buf}) f3 WHERE f3.person_id = f2.person_id ) ${c_add} `
                 }//if con 

                 mtdb_condition_out = ` SELECT DISTINCT o1.person_id, o1.provider_id FROM (${mtdb_condition_out}) o1`;
                 output[i].query =mtdb_condition_out
                 //console.log({c_add:rows[i].criteria_addition})
                 break;

                case 7:
                    //SQL
                    q_buf =` SELECT distinct person_id, provider_id FROM (${c_add = rows[i_ud].criteria_addition}) o1`
                
            
            }// switch 

            rows[i_ud].query = q_buf;
            out_query_list_ud.push(rows[i_ud]);
        }//for

        console.log(chalk.cyan('USER_DEFINE 퀄리리스트 조합하기'));
        out = '';
        chk_state_ud=''
        out_in = [], out_ex = [];
        
        //없으면 리턴 0
        if(out_query_list_ud.length==0){
            return '';
        }

        // in, ex 나누기 
        for(i_ud=0;i_ud<out_query_list_ud.length;i_ud++){
            if(out_query_list_ud[i_ud].criteria_state==1){
                out_in.push(out_query_list_ud[i_ud].query);
            }else{
                out_ex.push(out_query_list_ud[i_ud].query);
            }
        }
        // console.log({out_in,out_ex});


        // inclusion 쿼리 만들기. 
        final_out_in = ''
        if(out_in.length!=0){
            for(i_ud=0;i_ud<out_in.length;i_ud++){
                if(i_ud==0){
                        final_out_in += ` SELECT * FROM (${out_in[i_ud]} ) in_0 ` ;
                }else{
                        final_out_in += ` INNER JOIN  ( SELECT * FROM (${out_in[i_ud]}) ) in_${i_ud} ON in_${i_ud-1}.person_id = in_${i_ud}.person_id `
                }
            }
        }
        
        
        // exclusion 쿼리 만들기 
        final_out_ex=''
        if(out_ex.length!=0){
            for(i_ud=0;i_ud<out_ex.length;i_ud++){
                if(i_ud==0){
                    final_out_ex += ` SELECT * FROM ${out_ex[i_ud]} ex_0` ;
                }else{
                    final_out_ex += ` UNION  SELECT * FROM (${out_ex[i_ud]}) ex_${i_ud}`
                }
            }
        }
        if(final_out_ex.length!=0){
            out = `SELECT DISTINCT person_id, provider_id FROM ( ${final_out_in} )  f1 LEFT JOIN ( SELECT DISTINCT person_id, provider_id FROM ( ${final_out_ex}) ex1 ) f2 ON f1.person_id = f2.person_id WHERE f2.person_id IS NULL`
        }else{
            out = `SELECT DISTINCT person_id, provider_id FROM ( ${final_out_in} )  f1`;
        }

    }catch(err){
        console.log({"make_query_2_ERROR": err});
    }finally{
        console.log(chalk.blue("END-----make_query_for_UD"));
        console.log();
        return out;
    }
}// make_query_for_UD

async function Execute_query(result, study_id, query_id){
    console.log();
    console.log(chalk.red(`-----------------Start: Execute_query--------------------`));
    console.log();
    
    try{
        let q_list = result.final_out;
        let out_count   = [];
        let out_site    = [];
        let out_patient = [];
        let out_protocol = [];

            
        let sql = require('mssql');
        let pool = await sql.connect(CdmDbConfig);

        //쿼리 별 환자수 쿼리
        for(i_exe=0;i_exe<q_list.length;i_exe++){
            q_buf = `SELECT COUNT(DISTINCT person_id) cnt FROM (${q_list[i_exe]}) exe1`
            result_exe = await pool.request()
            .query(q_buf)

            cnt = result_exe.recordset[0].cnt;
            out_count.push({q_buf,cnt});
        }
        //console.log({out_count:out_count});

        
        //마지막 쿼리 SITE
        q_site = `SELECT s1.provider_id, s1.person_id, p1.year_of_birth, p1.gender_concept_id FROM (${result.final_last}) s1, person p1 WHERE s1.person_id = p1.person_id  `;
        console.log({q_site:q_site});
        result_exe = await pool.request()
        .query(q_site)

        out_site = Make_query_insert_exe(result_exe, study_id, query_id);
        //console.log({out_site:out_site})
        
        //프로토콜 삽입 문 정리 
        out_protocol = Make_query_insert_protocol(result_exe, study_id, query_id);
        //console.log({out_protocol:out_protocol})

        //환자 리스트 뽑기
        out_patient = Make_query_insert_patient(result_exe, study_id, query_id);


        // let out_count   = [];
        // let out_site    = [];
        // let out_patient = [];
        // let out_protocol = [];
        out = {
            rows: result.rows,
            out_count:out_count,
            out_site:out_site,
            out_patient:out_patient,
            out_protocol:out_protocol,
            provider_id : result.provider_id,
            final_out : q_list
        }
        pool.close();
        sql.close();
        return out;
    }catch(err){
        console.log({Execute_query_ERROR:err});
        console.log();
    }finally{
        console.log();
        console.log(chalk.red(`-----------------End: Execute_query--------------------`));
        console.log();
        //return '';
    }

}//Execute_query

async function Update_result(result, study_id, query_id, provider_id){
    console.log(chalk.cyan('------------START: Update_result-----------------------'))
    console.log();
    //console.log({result:result});
    try{
        let q_update = ''
        let sql_update = require('mssql');
    
        //site 
        q_update = result.out_site;
        let pool_update = await sql_update.connect(dbConfig);
        
        let result1 = await pool_update.request()
        .query(`DELETE FROM doctor_result WHERE study_id = ${study_id} and qeury_id= ${query_id}`)
        console.log({result_SITE_DELETE: result1});
        
        console.log({q_update:q_update});
        result1 = await pool_update.request()
        .query(q_update)
        console.log({result_SITE_UPDATE: result1});
    
        //patient
        q_update = result.out_site;
        result1 = await pool_update.request()
        .query(`DELETE FROM patient_result WHERE study_id = ${study_id} and query_id= ${query_id} AND provider_id= ${provider_id}`)
        console.log({result_Patient_DELETE: result1});
     
        result1 = await pool_update.request()
        .query(q_update)
        console.log({result_Patient_UPDATE: result1});
    
        //count
        q_update = result.out_count;
        for(i=0;i<q_update.length;i++){
            result1 = await pool_update.request()
            .input(`input1`, sql_update.Int, result.rows[i].criteria_detail_id)
            .input('input2',sql_update.Int, q_update.cnt )
            .query(`UPDATE criteria_detail SET count = @input2  WHERE criteria_detail_id = @input1`)
            //console.log({result_COUNT_UPDATE: result1});
        }
        //sql_update.close();

        //protocol.
        // result.out_protocol
        result1 = await pool_update.request()
        .input(`input1`, sql_update.Int, study_id)
        .input('input2',sql_update.Int, query_id )
        .query(`DELETE FROM protocol_result WHERE study_id = ${study_id} and query_id= ${query_id}`)
        console.log({result_Patient_DELETE: result1});

        result1 = await pool_update.request()
        .query(result.out_protocol);
        console.log({result_Patient_UPDATE: result1});
        
        pool_update.close();
        sql_update.close();

        result1 ={
            person_info: [],
            drug_info: [],
            measurement_info: [],
            condition_info: [],
            visit_info:[]
        };

        // 통계값구하기
        q_protocol_person_list = ` SELECT distinct person_id FROM ( ${result.final_out[result.final_out.length-1]} ) pp1 `
        console.log({q_protocol_person_list:q_protocol_person_list})
        
        let sql_protocol = require('mssql');
        let pool_protocol = await sql_protocol.connect(CdmDbConfig);
        
        //// 환자
        let result_person1 = await pool_protocol.request()
        .query('SELECT gender_concept_id, year_of_birth FROM dbo.PERSON WHERE person_id in ( '+q_protocol_person_list+' )')
        person_info = make_result_person_to_list(result_person1.recordset);
        //pool_protocol.close();
        //console.log({result_person1:result_person1});
        result1.person_info = JSON.parse(JSON.stringify(person_info));
        person_info ='';
        
         ////약.
         q_drug = 'SELECT f1.person_id, f1.gender_concept_id, f1.drug_concept_id, f1.drug_exposure_start_date, f1.drug_exposure_end_date, f1.quantity, f1.days_supply, f1.dose_unit_source_value,c1.concept_name, a1.SECOND as atc2, a1.THIRD as atc3, a1.FORTH as atc4, a1.FIFTH as atc5 FROM (SELECT p1.person_id, p1.gender_concept_id, d1.drug_concept_id, d1.drug_exposure_start_date, d1.drug_exposure_end_date, d1.quantity, d1.days_supply, d1.dose_unit_source_value FROM (SELECT person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, quantity, days_supply, dose_unit_source_value FROM dbo.DRUG_EXPOSURE WHERE dbo.DRUG_EXPOSURE.person_id in('+q_protocol_person_list+')) d1, dbo.PERSON p1 WHERE d1.person_id = p1.person_id ) f1, concept c1, mtdb.dbo.atc_rxnorm_all a1 WHERE f1.drug_concept_id = c1.concept_id and f1.drug_concept_id = a1.concept_id and f1.drug_exposure_start_date >=\'2014-01-01\' '
         console.log({q_drug:q_drug})
         let result_drug = await pool_protocol.request()
        .query(q_drug);
         // durg 결과 내용을 리스트로 만들기.
         //console.log({result_drug:result_drug});
         drug_info = make_result_drug_to_list(result_drug.recordset); 
         //pool_protocol.close();
        // console.log({drug_info_0:drug_info[0]});
        console.log("쓰기 ");
        var fs = require('fs');
        fs.writeFileSync('drug_info.json', drug_info,'utf8',function(err){
            console.log('동기적 파일 쓰기완료')
        })

         //result1.drug_info = JSON.parse(JSON.stringify(drug_info));
         drug_info ='';


        ////Lab
        m_q1 = 'SELECT f1.person_id, f1.gender_concept_id, f1.measurement_concept_id, f1.measurement_date, f1.value_as_number, f1.range_low, f1.range_high, f1.unit_source_value, c1.concept_name FROM (SELECT  d1.person_id, p1.gender_concept_id, d1.measurement_concept_id, d1.measurement_date, d1.value_as_number, d1.range_low, d1.range_high, d1.unit_source_value FROM (SELECT person_id, measurement_concept_id, measurement_date, value_as_number, range_low, range_high, unit_source_value FROM dbo.MEASUREMENT WHERE dbo.MEASUREMENT.person_id in('+q_protocol_person_list+')) d1, dbo.PERSON p1 WHERE d1.person_id = p1.person_id ) f1, concept c1 WHERE f1.measurement_concept_id = c1.concept_id';
        m_q2 = `SELECT DISTINCT v1.person_id, v1.measurement_concept_id, v1.value_as_number, v1.concept_name, v1.range_low, v1.range_high FROM ( ${m_q1} ) v1 WHERE v1.measurement_date = ( SELECT MAX(measurement_date) FROM ( ${ m_q1} ) v2 WHERE v2.person_id = v1.person_id)`
        console.log({m_q2:m_q2})
        let result_measurement = await pool_protocol.request()
          .query(m_q2);
         measurement_info= make_result_measurement_to_list(result_measurement.recordset);    //console_println(measurement_info.length);
        console.log({measurement_info:measurement_info[0]});
        result1.measurement_info = JSON.parse(JSON.stringify(measurement_info));
        measurement_info ='';

            ////Condition
        q_con = 'SELECT  f1.person_id, f1.gender_concept_id, f1.condition_concept_id, f1.condition_start_date, c1.concept_name, i1.mid as icd10_mid, i1.small as icd10_small, i1.CONCEPT_NAME as icd10_concept_name  FROM (SELECT d1.person_id, p1.gender_concept_id, d1.condition_concept_id, d1.condition_start_date FROM (SELECT person_id, condition_concept_id, condition_start_date FROM dbo.CONDITION_OCCURRENCE WHERE dbo.CONDITION_OCCURRENCE.person_id in('+q_protocol_person_list+')) d1, dbo.PERSON p1 WHERE d1.person_id = p1.person_id ) f1, concept c1 , mtdb.dbo.icd10_snomed_category i1 WHERE f1.condition_concept_id = c1.concept_id AND f1.condition_concept_id = i1.concept_id';
        console.log({q_protocol_condition:q_con});
        let result_condition = await pool_protocol.request()
            .query(q_con);
        condition_info= make_result_condition_to_list(result_condition.recordset);  //console_println(condition_info.length);
        console.log(condition_info[0]);
        result1.condition_info = JSON.parse(JSON.stringify(condition_info));
        condition_info ='';

         ////Visit
         visit_q = 'SELECT p1.person_id, p1.gender_concept_id, d1.visit_concept_id, d1.visit_start_date, d1.visit_end_date, d1.care_site_id, d1.visit_occurrence_id FROM (SELECT person_id, visit_concept_id, visit_start_date, visit_end_date,care_site_id, visit_occurrence_id FROM dbo.VISIT_OCCURRENCE WHERE dbo.VISIT_OCCURRENCE.person_id in( '+q_protocol_person_list+' )) d1, dbo.PERSON p1 WHERE d1.person_id = p1.person_id' 
           
         last_q = `SELECT DISTINCT v1.person_id, v1.visit_concept_id FROM ( ${visit_q} ) v1 WHERE v1.visit_occurrence_id = ( SELECT MAX(visit_occurrence_id) FROM (${ visit_q }) v2 WHERE v2.person_id  = v1.person_id)`
         let result_visit = await pool_protocol.request()
         .query(last_q);
        visit_info= make_result_visit_to_list(result_visit.recordset); //console_println(visit_info);
        result1.visit_info = JSON.parse(JSON.stringify(visit_info));
        visit_info ='';
         //console_println(result);

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
            sql_protocol.close();
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

    }catch(err){
        console.log({Update_result_ERROR: err});
    }finally{
        console.log(chalk.cyan('------------END: Update_result-----------------------'))
        console.log();
    }
}

//////////////////////////////함수들

function measurement_method2(method){
    if(method.length!=0){
        return `, ${method}(A.value_as_number) as value_as_number `;
    }
    return '';
}//measurement_method2
function measurement_date(value_date){
    // console.log("hihi"+value_date.length);
     if(value_date.length==0){
         return '';
     }
     var split_date      = value_date.split(" - ");
     var start_date      = split_date[0];
     var end_date        = split_date[1];
     var start_split     = start_date.split("/");
     var end_split       = end_date.split("/");
     var start_data      = start_split[2]+"-"+start_split[1]+"-"+start_split[0];
     var end_data        = end_split[2]+"-"+end_split[1]+"-"+end_split[0];
     
     date_start  = "and measurement_date >= '"+start_data+"' ";
     date_end    = "and measurement_date <= '"+end_data+"' ";
 
     return date_start + date_end
 }// measurement_date
function measurement_num(num){
    if(num.length==0||num==null){
        return '';
    }else{
        return "WHERE num < " + num;
    }
}//measurement_num
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


 ////
 
 function Make_query_insert_exe(result, study_id, query_id){
    var q_execute = []; var q_cnt = 0; // 1000개 이상일 경우 
    try{
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
                console.log({study_id, query_id});
                for (i = 0; i < q_list.length; i++) {
                    if (q_list[i].total > 0) {
                        q_execute.push("(2, " + q_list[i].id + ", " + q_list[i].total + ", " + query_id + ", " + study_id + ", N'" + output_string(q_list[i].male) + "', N'" + output_string(q_list[i].female) + "')");
                    }
                }
    
                //쿼리리스트 하나로 만들기 
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
            } else {
                console.log({ make_query_insert_1_result: 0 })
                //console.log(err);
                return q_execute;
            }
        } else {
            console.log(err);
            //res.send(err);
        }

    }catch(err){
        console.log({Make_query_insert_exe:err});
    }
    
}//make_query_insert_1
function obj_provider(id){
    var obj={
        id : id,
        total : 0,
        male: {d9:0,d10:0,d20:0,d30:0, d40:0, d50:0, d60:0,d70:0},
        female: {d9:0,d10:0,d20:0,d30:0, d40:0, d50:0, d60:0,d70:0}
    }
    return obj;
}//obj_provider
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
}//obj_provider_update
function output_string(obj) {
    var out = obj.d9 + ", " + obj.d10 + ", " + obj.d20 + ", " + obj.d30 + ", " + obj.d40 + ", " + obj.d50 + ", " + obj.d60 + ", " + obj.d70;
    return out;
}//output_string

////
///protocol
function Make_query_insert_protocol(result, study_id, query_id){
    console.log(chalk.cyan('----------------Start: Make_query_insert_protocol-------------'));
    console.log();
    try{
        var q_execute = [];

        //1. 준비 작업
        if(result.recordset !=0){
            let rows = result.recordset;
            let q_list = [];
            let chk_num = -1;

            for (i = 0; i < rows.length; i++) {
                q_execute.push("( "+rows[i].person_id+", "+study_id+","+query_id+" )");
            }

            //2. 삽입 코드 만들기 
            let cnt_1000 = 0;
            let query = "";
            let q_final = [];
            for (i = 0; i < q_execute.length; i++) {
                query += q_execute[i];
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
            }// for 
            
            //2-1. 한쿼리로 만들기 
            q_final.push("INSERT INTO protocol_result(person_id, study_id, query_id) VALUES " + query + " ;");
            q_list1 = "";
            for (i = 0; i < q_final.length; i++) {
                q_list1 += q_final[i];
            }
            return q_list1;   
        }

        

    }catch(err){
        console.log({Make_query_insert_protocol_ERROR : err});
        console.log();
    }finally{
        console.log(chalk.cyan('----------------END: Make_query_insert_protocol-------------'));
        console.log();
    }


}//Make_query_insert_protocol
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
function make_result_person_to_list(recordset){
    let result = [];
    for(i=0;i<recordset.length;i++){
        result.push(class_person(recordset[i].gender_concept_id, recordset[i].year_of_birth));
    }
    return result;
} 
//durg 결과 내용을 리스트로 만들기. 
function make_result_drug_to_list(recordset){
// a1.SECOND as atc2, a1.THIRD as atc3, a1.FORTH as atc4, a1.FIFTH as atc5 
let result =[];
for(i_d=0;i_d<recordset.length;i_d++){
    result.push(class_drug(
        recordset[i_d].person_id,
        recordset[i_d].gender_concept_id,
        recordset[i_d].drug_concept_id,
        new Date(recordset[i_d].drug_exposure_start_date).format("yyyy-MM-dd"),
        new Date(recordset[i_d].drug_exposure_end_date).format("yyyy-MM-dd"),
        recordset[i_d].quantity,
        recordset[i_d].days_supply,
        recordset[i_d].dose_unit_source_value,
        recordset[i_d].concept_name,
        Atc_code(recordset[i_d].atc2,recordset[i_d].atc3,recordset[i_d].atc4, recordset[i_d].atc5)
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
////

///
function Make_query_insert_patient(result, study_id, query_id){
    try{
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
                
                // 쿼리 합치기 
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
            }
        }

    }catch(err){
        console.log({Make_query_insert_patient_ERROR:err});
    }
}

///
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



///////////////////////////////////

module.exports ={
    Get_condition_list,
    Make_query,
    Execute_query,
    Update_result
}