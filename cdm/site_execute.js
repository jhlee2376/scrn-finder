
const {dbConfig, CdmDbConfig} = require('../setting/server_config.js');
const {make_query_list, query_list_to_query_3} = require(`./cdm_function.js`);
var sql = require('mssql');

const site_condition = 1;
var main_query=''
async function site_execute(study_id, query_id, res){

    console.log(`----Start: site_execute----`);
    try{
        let pool =  await sql.connect(dbConfig); //scrn_cloud
        console.log(`1.상태 업데이트`);
        let result = await pool.request() 
        .input('input_paramiter1', sql.Int, study_id)
        .input('input_paramiter2', sql.Int, query_id)
        .query(`UPDATE query SET query_status = N'In Progress' WHERE study_id = @input_paramiter1 and query_id = @input_paramiter2`)
        //console.log({"1_결과": result});   console.log();

        console.log(`2.DB에서 설정 조건값 가져와서 쿼리값 만들기`);
        result = await pool.request()
        .input('input_paramiter1', sql.Int, study_id)
        .input('input_paramiter2', sql.Int, query_id)
        .query(`SELECT * FROM criteria_detail WHERE study_id = @input_paramiter1 and query_id = @input_paramiter2`)
        //console.log({"2_결과":result});  console.log();
        pool.close();
        sql.close();
        var query_list =  await make_query_list(result)
        console.log({query_list:query_list});

        if(query_list.length==0){
            res.send('null');
            return 0;
        }
        console.log()
        console.log(`3.쿼리리스트 execute하기`);
        
        q_list = query_list_to_query_3(query_list);
        console.log({q_list:q_list});

        q_result_count_person=[];
        pool =  await sql.connect(CdmDbConfig);
        for(i=0;i<q_list.list.length;i++){
            result = await pool.request()
            .query(q_list.list[i])
            q_result_count_person.push(result.recordset[0]);
        }
        console.log({q_result_count_person:q_result_count_person.length});
        //console.log({main_query:main_query});
        main_query = q_list.list[q_list.list.length-1];
        console.log({main_query:main_query});
        sql.close();
        
        
        //query_list 
        pool= await sql.connect(dbConfig);
        for(i=0;i<q_result_count_person.length;i++){
            result = await pool.request()
            .input(`input_param1`,sql.Int,query_list[i].criteria_detail_id)
            .input('input_param2',sql.Int,q_result_count_person[i].cnt)
            .query(`UPDATE criteria_detail SET count = @input_param2  WHERE criteria_detail_id = @input_param1`)
            console.log({result: result});
        }


         //pool.close();
         sql.close();
         return q_list.last;
    }catch(err){
        console.log({site_execute_error: err});
        console.log();
        sql.close();
    }finally{
        //res.send('ok');
        
    }
}



module.exports = site_execute;