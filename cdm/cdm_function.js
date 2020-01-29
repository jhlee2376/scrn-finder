
let sql = require('mssql');
const {dbConfig, CdmDbConfig} = require('../setting/server_config.js');
const { cdm } = require('../cdm/cdm_table'); //console.log(cdm);

async function make_query_list(result){
   try{
    var state = result.recordset.length;
    if(state == 0){
        console.log({result:null});
    }else{
        //1. 강한, 약한 나누기 조건으로 나누기. 
        result_1 = await make_query(result);
        
    }

   }catch(err){
    console.log({"make_query_list_error": err});
   }finally{
       return result_1;
   }

    
}

async function make_query(result) {
    out_query_list =[];
    // 카테고리화 문제 때문에 
    // table 값이 6인경우 mtdb에서 해당 id값을 가져와야함 
    // criteria_table 에 텍스트 값을 비교하여 6: 진단, 5: 약을 구분 
    // criteria_detail_attribute , {1,2,3}, {대, 중, 소}
    // SELECT concept_id FROM mtdb.dbo.icd10_snomed WHERE SMALL = "%Gout%" 
    var select_table = ['atc_rxnorm_all', 'icd10_snomed']; // -5
    var select_lms = ['LARGE', 'MID', 'SMALL']; // -1 
    var select_lms_atc = ['FIRST', 'SECOND', 'THIRD', 'FORTH', 'FIFTH'];
    var condition =["=","=","=","=","="];
    let rows = result.recordset;
    for(i=0;i<rows.length;i++){
        out_query_list.push(rows[i]);
        table = rows[i].criteria_detail_table; //0
        field = rows[i].criteria_detail_attribute; //0
        con = rows[i].criteria_detail_condition;
        val = rows[i].criteria_detail_value;
        title = rows[i].criteria_title;
        
        c_add=''
        if(rows[i].criteria_addition.length >0 && rows[i].criteria_addition!='null' ){
            val += " " +rows[i].criteria_addition;
            c_add = rows[i].criteria_addition;
        }
    
        switch(Number(table)){
            //person
            case 0:
                if(field == 0 ){ //gender
                    out_query_list[i].query = " SELECT DISTINCT person_id, provider_id FROM person WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " ";                    
                }else{
                    out_query_list[i].query = " SELECT DISTINCT person_id, provider_id FROM person WHERE 1=1 " +rows[i].criteria_addition+ " ";
                }
                break;

            //measurement
            case 3:
                let q_measurement_buf = ''
                let q_measurement = ''

                if(rows[i].measurement_count!=''||rows[i].measurement_method!=''){
                    q_measurement_buf = " SELECT person_id, provider_id, measurement_date  FROM measurement WHERE person_id IN (  ( SELECT person_id FROM ( SELECT A.person_id  "+ measurement_method2( rows[i].measurement_method) +" FROM ( SELECT ROW_NUMBER() OVER(partition by person_id ORDER BY measurement_date DESC ) AS num, person_id, measurement_date, value_as_number FROM dbo.MEASUREMENT WHERE measurement_concept_id = "+ rows[i].criteria_detail_value + " "+ measurement_date(rows[i].date_addition_value)+ " ) A " +measurement_num(rows[i].measurement_count )+ " GROUP BY A.person_id ) B "+measurement_min_max(rows[i].value_addition_min, rows[i].value_addition_max,rows[i].min_condition, rows[i].max_condition) +")))"
                    
                }else{
                    q_measurement_buf =  "( SELECT person_id, provider_id, measurement_date FROM measurement WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val +")";
                }

                if(con ==0){
                    q_measurement = ` SELECT person_id, provider_id FROM ${q_measurement_buf} ff1 WHERE 1=1 `
                }else if(con==1){
                    q_measurement = ` SELECT person_id, provider_id FROM ( SELECT ff1.person_id, ff1.provider_id, ff1.measurement_date FROM ${q_measurement_buf} ff1 WHERE ff1.measurement_date = ( SELECT MIN(ff2.measurement_date) FROM ${q_measurement_buf} ff2 WHERE ff2.person_id = ff1.person_id) ${c_add} ) m1`
                }else{
                    q_measurement = ` SELECT person_id, provider_id FROM ( SELECT ff1.person_id, ff1.provider_id, ff1.measurement_date FROM ${q_measurement_buf} ff1 WHERE ff1.measurement_date = ( SELECT MAX(ff2.measurement_date) FROM ${q_measurement_buf} ff2 WHERE ff2.person_id = ff1.person_id) ${c_add} ) m1`

                }// con
                console.log({"검사_테스트":q_measurement})
                out_query_list[i].query =q_measurement
            
            break;

            case 4:
                out_query_list[i].query =` SELECT DISTINCT person_id, provider_id FROM VISIT_OCCURRENCE WHERE 1=1 ${rows[i].criteria_addition} `;
                break;

            //mtdb 약2
            case 5:
                //console.log(`'%${title}%'`);
                // SELECT concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE SMALL = "%Gout%" 
               
                out_query_list[i].query =` SELECT DISTINCT person_id, provider_id FROM drug_exposure WHERE drug_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE ${select_lms_atc[field-1]} LIKE '%${title}%') ${c_add}`;
                 console.log({c_add:rows[i].criteria_addition})
                
                
                break;

            //mtdb 병력
            case 6:
           
                // SELECT concept_id FROM mtdb.dbo.icd10_snomed WHERE SMALL = "%Gout%" 
                //console.log(`'${title}'`)
                out_query_list[i].query =` SELECT DISTINCT person_id, provider_id FROM CONDITION_OCCURRENCE WHERE condition_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.icd10_snomed WHERE ${select_lms[field-1]} LIKE '%${title}%' ) ${c_add}`;
                console.log({c_add:rows[i].criteria_addition})
                
                
                break;

            //user Define
            //table = 9, detail_value = user_define_id
            case 9:
                try{
                    console.log();
                    console.log("UD");
                    let query_list_user_define = [];
                    let pool_ud = await sql.connect(dbConfig);
                    id_ud = val;
                    //ud = user_define
                    result_ud = await pool_ud.request()
                        .input('input_param', sql.Int, id_ud)
                        .query(`SELECT * FROM user_criteria_detail WHERE user_define_id = @input_param`)
                    //console.log({result_ud:result_ud.recordset});
                    pool_ud.close()
                    sql.close();

                    query_list_ud = await make_query_2(result_ud);
                    console.log({query_list_ud:query_list_ud});
                    // query = '';
                    // for (k = 0; k < query_list_ud.length; k++) {
                    //     query += ` ${query_list_ud[k]} `;
                    //     if (k + 1 != query_list_ud.length) {
                    //         query += ` UNION `
                    //     }
                    // }
                    //query = query_list_to_query_3(query_list_ud)
                    console.log({query_UD:query.last});
                    out_query_list[i].query =query.last;

                    break;
                }catch(err){
                    console.log({"user_define_ERROR":err});
                }//catch
        }// switch 
    }//for
    
    return out_query_list;
}// make_query

async function make_query_2(result) {
    try{
        console.log();
        console.log("make_query_2");
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
                //person
                case 0:
                    if(field == 0 ){ //gender
                        q_buf= " SELECT DISTINCT person_id, provider_id FROM person WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " ";                    
                    }else{
                        q_buf=" SELECT DISTINCT person_id, provider_id FROM person WHERE 1=1 " +rows[i_ud].criteria_addition+ " ";
                    }
                    break;
    
                //measurement
                case 3:
                    if(rows[i_ud].measurement_count!=''||rows[i_ud].measurement_method!=''){
                        q_buf=" SELECT DISTINCT person_id, provider_id FROM measurement WHERE person_id IN (  ( SELECT person_id FROM ( SELECT A.person_id  "+ measurement_method2( rows[i].measurement_method) +" FROM ( SELECT ROW_NUMBER() OVER(partition by person_id ORDER BY measurement_date DESC ) AS num, person_id, measurement_date, value_as_number FROM dbo.MEASUREMENT WHERE measurement_concept_id = "+ rows[i].criteria_detail_value + " "+ measurement_date(rows[i].date_addition_value)+ " ) A " +measurement_num(rows[i].measurement_count )+ " GROUP BY A.person_id ) B "+measurement_min_max(rows[i].value_addition_min, rows[i].value_addition_max,rows[i].min_condition, rows[i].max_condition) +"))" ;
                    }else{
                        q_buf=" SELECT DISTINCT person_id, provider_id FROM measurement WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val +"";
                    }
                    break;
    
                case 4:
                q_buf=` SELECT DISTINCT person_id, provider_id FROM VISIT_OCCURRENCE WHERE 1=1 ${rows[i_ud].criteria_addition} `;
                    break;
    
                //mtdb 약2
                case 5:
                    //console.log(`'%${title}%'`);
                    // SELECT concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE SMALL = "%Gout%" 
                    q_buf=` SELECT DISTINCT person_id, provider_id FROM drug_exposure WHERE drug_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.atc_rxnorm_all WHERE ${select_lms_atc[field-1]} LIKE '%${title}%') ${c_add}`;
                    //console.log({"m2":rows[i].criteria_addition})
                    break;
    
                //mtdb 병력
                case 6:
               
               
                    // SELECT concept_id FROM mtdb.dbo.icd10_snomed WHERE SMALL = "%Gout%" 
                    //console.log(`'${title}'`)
                    q_buf=` SELECT DISTINCT person_id, provider_id FROM CONDITION_OCCURRENCE WHERE condition_concept_id IN (SELECT distinct concept_id FROM mtdb.dbo.icd10_snomed WHERE ${select_lms[field-1]} LIKE '%${title}%') ${c_add}`;
                    //console.log({"m2":rows[i].criteria_addition})
                   
                    break;
            
            }// switch 

            rows[i_ud].query = q_buf;
            console.log(rows[i_ud].query);
        }//for
        
       

    }catch(err){
        console.log({"make_query_2_ERROR": err});
    }finally{
        //console.log("m_q_ end");
        return out_query_list_ud;
    }
   
}// make_query

function measurement_method2(method){
    if(method.length!=0){
        return `, ${method}(A.value_as_number) as value_as_number `;
    }
    return '';
}

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


///////////////////3번
function query_list_to_query_3(query_list){
    out=[];
    let in_q ='', ex_q=''
    let q
    try{
        for(i=0;i<query_list.length;i++){

            if(i==0){
                in_q = `(${query_list[i].query}) a${i}`
            }else{
                //선정 제외 조건
                if(query_list[i].criteria_state==1){
                    in_q +=  `(${query_list[i].query}) a${i} ON a${i-1}.person_id = a${i}.person_id `
                }else{
                    ex_q += query_list[i].query
                }
            }
           
                if(query_list[i].criteria_state==1){
                    in_q +=  ` INNER JOIN `
                }else{
                    if(ex_q.length!=0){
                        ex_q += ` UNION `
                    }
                    
                }
            
            q = ` SELECT COUNT( DISTINCT a0.person_id ) as cnt FROM ${in_q.substring(0,in_q.length-11)}  `;
            
            if(ex_q.length!=0){
                q +=  ` LEFT JOIN ( SELECT DISTINCT person_id FROM (${ex_q.substring(0,ex_q.length-6)}) l1 ) b1 ON a0.person_id = b1.person_id WHERE  b1.person_id IS NULL `;
            }
            //console.log({q:q});
            out.push(q);
            // result = await pool.request()
            // .query(`SELECT COUNT(DISTINCT person) FROM (${in_q}) a1 LEFT JOIN ( ${ex_q} ) b1 ON a1.person_id = b1.person_id WHERE  b1.person IS NULL`)
            last =''
            if(i+1>= query_list.length){
                last = ` SELECT a0.provider_id, a0.person_id FROM ${in_q.substring(0,in_q.length-11)}  `
                if(ex_q.length!=0){
                    last+= ` LEFT JOIN ( SELECT DISTINCT person_id FROM (${ex_q.substring(0,ex_q.length-6)}) l1 ) b1 ON a0.person_id = b1.person_id WHERE  b1.person_id IS NULL `;
                }
            }
        }//for


        out = {
            in_query : in_q.substring(0,in_q.length-11),
            ex_query : ex_q.substring(0,ex_q.length-6), 
            list : out,
            last : last
        }
    }catch(err){
        console.log()
        console.log({"query_list_to_query_3":err})
    }finally{
        return out;
    }
    
    
}// query_list_to_query_3

module.exports = {
    make_query_list,
    query_list_to_query_3
}