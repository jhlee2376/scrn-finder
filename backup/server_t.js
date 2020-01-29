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
var obj;
fs.readFile('files/test.json',function(err, data){
    obj = JSON.parse(data);
    console.log('readFile ok')
})

var con = 0;

var sql = require('mssql');
    //설정
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

var cdmDbConfig = {
    user: 'sa',
    password: 'scrn123!!',
    server: '127.0.0.1',
    port: '1433',
    database: 'BIGDATADB',
    connectionTimeout: 300000,
    requestTimeout: 300000,
    pool:{
        idleTimeoutMillis :1500000,
        requestTimeout: 1500000
    }
}


//웹 서버를 생성
var app = express();
app.use(cors());
app.use(express.static('public'));
app.use(bodyParser.json());


var re =[];

// 변수 선언
var items = [{
    name: '우유',
    price: '2000'
},{
    name: '홍차',
    price: '3000'
}];

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


app.get("/test",(req,res)=>{
    lp("/test",obj.length);
    res.send(obj);
});


// app.use(bodyParser.urlencoded({extended : true}));

//app.use(express.bodyParser());
//app.use(app.router); //라우터 안써도 됨.이제 

//라우트합니다.
app.all('/data.html', function(request, response){
    var output ='html로 뿌리기 ';
    
    response.send(output);

});

app.all('/data.json', function (request, response){
    response.send(items);
});
app.all('/data.xml', function(request, response){});

//동적라우터
app.all('/parameter/:id', function(request, response){
    var id = request.param('id');

    response.send('<h1>'+id+'</h1>');
});

app.get('/parameter/',function(request, response){
   
    response.send(item);
});

app.get('/products', function (request, response){
    response.send(items);
});

app.get('/products/:id', function (request, response){
    var id = Number(request.param('id'));
    response.send(items[id]);
});

var list;
app.get('/query/:queryId',function(request, res){
    console.log("get, /query/:queryId");
//    sql.connect(dbConfig, function(){
//        var req = new sql.Request();
//        req.query("select * from query", function(err, recordset){
//             if(err) console.log(err);
//             var a = JSON.stringify(recordset);
//             res.writeHead(200,{'Content-Type':'text/html:charset=UTF-8'});
//             res.end(a);
//        })
//    })
    var id = Number(request.param('queryId'));
    new sql.ConnectionPool(dbConfig).connect().then(pool => {
        return pool.request().query("select * from query where study_id = " +id);
    }).then(result => {
        let rows = result.recordset
        //console.log(rows);
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.status(200).json(rows);
        sql.close();
    }).catch(err => {
        res.status(500).send({ message: "${err}" })
        sql.close();
    });
   
});
//{"query_id":1,"study_id":1,"title":"나이 조건 검색",
//  "criteriaSet":[{"table":"Person","field":"birth","condition":3,"value":"1900"},{"table":"Person","field":"birth","condition":3,"value":"2500"}]}

app.delete('/query', function (req, res) {
    //query 삭제 시작
    //criteria 삭제. 
    //criteria_detail 삭제.
    console.log("delete, /query")
    re = req.body;
    //console.log(re);
    var deleteList = "";

    for (var i = 0; i < re.originSet.length; i++) 
    {
        deleteList += "( query_id = " + re.originSet[i].query_id + " and study_id = " + re.info.study_id + ")";
        if (i + 1 != re.originSet.length) {
            deleteList += " or ";

        }
    }
        //console.log("1:"+deleteList)
        var query = "DELETE from query WHERE " + deleteList+"; DELETE from criteria WHERE "+ deleteList +" ; DELETE FROM criteria_detail WHERE "+ deleteList +" ; ";
        const pool1 = new sql.ConnectionPool(dbConfig, err => 
            {
            // ... error checks
            // Query
            console.log(query);
            pool1.request() // or: new sql.Request(pool1)
                .query(query, (err, result) => {
                    // ... error checks
                    //console.dir(result)
                    //console.log("pool1 in err: " + err);

                    var insertList = "";

                    
                    for (var i = 0; i < re.newSet.length; i++) {
                        insertList += "(" +
                            re.newSet[i].query_id + ", N'" + re.newSet[i].query_title + "', N'" + re.newSet[i].query_create_date + "', N'" + re.newSet[i].query_modify_date + "', N'" + re.newSet[i].query_exe_date + "', " + re.newSet[i].query_status + ", N'" + re.newSet[i].query_creator + "', N'" + re.newSet[i].query_last_editor + "', " + re.info.study_id + ")";
                        if (i + 1 != re.newSet.length) {
                            insertList += " , "
                        }
                    }
                    var query1 = "INSERT INTO query VALUES " + insertList;
                    sql.close();
                     console.log(query1);
                    const pool2 = new sql.ConnectionPool(dbConfig, err => {
                        // ... error checks

                        // Query
                        //console.log(query1);
                        pool2.request() // or: new sql.Request(pool1)
                            .query(query1, (err, result) => {
                                // ... error checks
                                //console.dir(result)
                                console.log("pool2 in err: " + err);

                            })

                    })


                })

        })

        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })

    });


    
var set = require('./serverSource/classList');
app.get('/criteria/:queryId/:study_id',function(request,res){
    printLog("get /criteria/:study_id/:query_id");
    var criteriaSet =[];
    var study_id = Number(request.param('study_id'));
    var query_id = Number(request.param('queryId'));
 
    
    const pool1 = new sql.ConnectionPool(dbConfig, err => {
        // ... error checks
        // Query
        var q = "select criteria_id, criteria_title, criteria_state from criteria where query_id = " + query_id + " and study_id = "+study_id;
        console.log(q)
        pool1.request() // or: new sql.Request(pool1)
        .query(q, (err, result) => {
            // ... error checks
            //let rows = result.recordset
    
            let rows = result.recordset


            for(var i=0;i<rows.length;i++){
                criteriaSet.push(set.Criteria(rows[i].criteria_id, rows[i].criteria_title,rows[i].criteria_state));
                //console.log(rows[i]);
            }

            

            const pool2 = new sql.ConnectionPool(dbConfig, err => {
                // ... error checks
             
                // Query
             
                pool2.request() // or: new sql.Request(pool1)
                .query('select * from criteria_detail where query_id = '+ query_id +" and study_id = "+study_id, (err, result1) => {
                    // ... error checks
                    //let rows = result.recordset
                    //console.dir(result)
                    let rows1 = result1.recordset
                    //console.log(rows1);
                    
                    console.log("rows len:"+rows1.length);
                    //console.log("criteriaSet len:"+criteriaSet.length);
                    for(var i=0;i<rows1.length;i++)
                    {
                        for(var j=0;j<criteriaSet.length;j++){
                            if(criteriaSet[j].criteria_id==rows1[i].criteria_id&& criteriaSet[j].criteria_state==rows1[i].criteria_state)
                            {   
                                //console.log(criteriaSet[j].criteria_id+", "+rows1[i].criteria_id);
                                criteriaSet[j].criteria_detail_set.push(set.Criteria_detail(
                                    rows1[i].criteria_detail_id,
                                    rows1[i].criteria_detail_table,
                                    rows1[i].criteria_detail_attribute,
                                    rows1[i].criteria_detail_condition,
                                    rows1[i].criteria_detail_value,
                                    rows1[i].criteria_id)
                                )
                            }
                        }
                    }
                ;
                
                res.send(criteriaSet);
                })
            //criteriaSet[0].criteria_detail_set.push(set.Criteria_detail(1,"hi","hi",1,"hi",1));
            
            })
            pool2.on('error', err => {
                // ... error handler
            })
        
        })
    })
    pool1.on('error', err => {
        // ... error handler
    })

    
    // new sql.ConnectionPool(dbConfig).connect().then(pool => {
    //     return pool.request().query("select criteria_id, criteria_title from criteria where query_id = " + id);
    // }).then(result => {
    //     let rows = result.recordset


    //     for(var i=0;i<rows.length;i++){
    //         criteriaSet.push(set.Criteria(rows[i].criteria_id, rows[i].criteria_title));
    //         console.log(rows[i]);
    //     }

    //         console.log("쿼리1_시작");
    //         new sql.ConnectionPool(dbConfig).connect().then(pool => {
    //             return pool.request().query("select * from criteria_detail where query_id = " + id);
    //         }).then(result => {
    //             console.log("쿼리2_시작");
    //             let rows1 = result.recordset
    //             console(log(rows1));
    //             var chk;
    //             for(var i=0;i<rows.length;i++){
    //                 criteriaSet = set.addCriteriaDetail(criteriaSet, rows1[i]);
    //                 console.log(rows1[i]);
    //             }
    //             console.log(rows1);
    //             sql.close();
    //         }).catch(err => {
                
    //             sql.close();
    //         });
    //         console.log("쿼리1_종료");
        
    //     // res.setHeader('Access-Control-Allow-Origin', '*');
    //     res.status(200).json(criteriaSet);
    //     // sql.close();

    //     sql.close();
    // }).catch(err => {

    //     // res.status(500).send({ message: "${err}" })
    //     // sql.close();
    //     sql.close();
    // });
   
    // new sql.ConnectionPool(dbConfig).connect().then(pool => {
    //     return pool.request().query("select criteria_id, criteria_title from criteria where query_id = "+query_id)
    // }).then(result => {
    //     let rows = result.recordset
    //     for(var i=0;i<rows.length;i++){
    //         criteriaSet.push(new makeCriteria(rows[i].criteria_id,rows[i].criteria_title));
    //     }
    //     console.log(criteriaSet.length);
    //     res.setHeader('Access-Control-Allow-Origin', '*');
    //     res.status(200).json(criteriaSet);
    //     sql.close();
    // }).catch(err => {
    //     res.status(500).send({ message: "${err}" })

    // });
})

//put 방식
app.get('/criteria/',function(request,re ){

});

app.post('/updateCriteriaSet', function (req, res) {
    re = req.body;
    console.log(re);
    //res.send("ok");
    var deleteList ="";
    for (var i=0;i<re.criteriaSet.length;i++){
        deleteList += "( criteria_id = "+re.criteriaSet[i].criteria_id+" and criteria_state = "+re.criteriaSet[i].criteria_state+ " and query_id = "+re.queryIdx+" and study_id = "+re.studyId+")";
        if(i+1 != re.criteriaSet.length){
            deleteList += " or ";
        }
    }
    //console.log("1:"+deleteList)
    var query
    if(deleteList!=""){
        query = "DELETE from criteria WHERE " + deleteList;
    }else{
        query = "SELECT TOP(1) * from criteria";
    }
    
    const pool1 = new sql.ConnectionPool(dbConfig, err => {
        // ... error checks

        // Query
        console.log(query);
        pool1.request() // or: new sql.Request(pool1)
            .query(query, (err, result) => {
                // ... error checks
                //console.dir(result)
                //console.log("err: "+err);

                var insertList = "";

                for (var i = 0; i < re.newCriteriaSet.length; i++) {
                    insertList += "(" + re.newCriteriaSet[i].criteria_id + ", N'" + re.newCriteriaSet[i].criteria_title + "', " + re.queryIdx + ", " + re.newCriteriaSet[i].criteria_state +", "+re.studyId +")";
                    if (i + 1 != re.newCriteriaSet.length) {
                        insertList += " , ";
                    }
                }
                var query1 = "INSERT INTO criteria VALUES " + insertList;
                sql.close();
                console.log(query1);
                const pool2 = new sql.ConnectionPool(dbConfig, err => {
                    // ... error checks

                    // Query
                    //console.log(query1);
                    pool2.request() // or: new sql.Request(pool1)
                        .query(query1, (err, result) => {
                            // ... error checks
                            //console.dir(result)
                            //console.log("pool2 err: "+err);

                        })

                })


            })

    })

    pool1.on('error', err => {
        // ... error handler
        console.log("pool1: "+err);
    })


    const pool3 = new sql.ConnectionPool(dbConfig, err => {
        // ... error checks

        // Query
        var deleteList1 ="";
        var insertList1 ="";
        var sub;

        //console.log(re.criteriaSet.length);
        for(var i=0;i<re.criteriaSet.length;i++){
             sub = re.criteriaSet[i];
            for(var j=0;j<sub.criteria_detail_set.length;j++){
                deleteList1 += "(criteria_detail_id = "+ sub.criteria_detail_set[j].criteria_detail_id +" and criteria_id = "+ sub.criteria_id +" and query_id = " + re.queryIdx + " and criteria_state = " + sub.criteria_state+" and study_id = "+re.studyId+")";
                if(j+1 != sub.criteria_detail_set.length){
                    deleteList1 += " or ";
                }
            }
            //console.log("i: "+i);
            if(i+1 != re.criteriaSet.length){
                deleteList1 += " or ";
            }
        }

        for(var i=0;i<re.newCriteriaSet.length;i++){
            sub = re.newCriteriaSet[i];
            for(var j=0;j<sub.criteria_detail_set.length;j++){
                insertList1 += "("+sub.criteria_detail_set[j].criteria_detail_id+", "+sub.criteria_detail_set[j].criteria_detail_table+", "+sub.criteria_detail_set[j].criteria_detail_field +", "+sub.criteria_detail_set[j].criteria_detail_condition+", N'"+sub.criteria_detail_set[j].criteria_detail_value+"', "+sub.criteria_id +", "+re.queryIdx+", "+sub.criteria_state+", "+re.studyId+")"
                if(j+1 != sub.criteria_detail_set.length){
                    insertList1 += " , ";
                }
            }
            //console.log("i: "+i);
            if(i+1 != re.newCriteriaSet.length){
                insertList1 += " , ";
            }
        }



        var query3;
        if(deleteList1.length!=0){
            query3="DELETE from criteria_detail WHERE " + deleteList1;
        }else{
            query3="SELECT TOP(1) * FROM criteria_detail;";
        }
         
        var query4 ="INSERT INTO criteria_detail VALUES " +insertList1;
        //console.log("pool3: "+query3);
        console.log("pool4: "+query4);

        pool3.request() // or: new sql.Request(pool1)
            .query(query3, (err, result) => {
                // ... error checks
                //console.dir(result)
                console.log("pool3 in: "+err);
                //console.log("pool4: in"+query4);
                
                //Save pool4
                const pool4 = new sql.ConnectionPool(dbConfig, err => {
                    // ... error checks
            
                    // Query
                    //console.log("pool4: "+query4);
            
                    pool4.request() // or: new sql.Request(pool1)
                        .query(query4, (err, result) => {
                            // ... error checks
                            //console.dir(result)
                            console.log("pool4: in "+err);
                            //pool4.response().send("ok");
                            res.send("ok");
                        })
            
                })
                pool4.on('error', err => {
                    // ... error handler
                    console.log("pool4_err : "+err);
                })
            })

    })
    pool3.on('error', err => {
        // ... error handler
        console.log("pool3_err : "+err);
    })

    //query = "INSERT * from criteria VALUES " +insertList;
    
    //executeQuery (res, sql);
    


});


app.post('/save-querySet-toDB',function(req, res){
    var data = req.body;
    
    printLog("/save-querySet-toDB",con);
    printLog(data,con);
    var study_id = data.study_id;
    var data = data.querySet;
    var insertList =  
    "("+data.query_id+", N'"+data.query_title+"', N'"+data.query_create_date+"', N'"+data.query_modify_date+"', N'"+data.query_exe_date+"', "+data.query_status+", N'"+data.query_creator+"', N'"+data.query_last_editor+"', "+study_id+")";
   
    var query = "INSERT INTO query  VALUES " + insertList;
    

    const pool1 = new sql.ConnectionPool(dbConfig, err => {
        // ... error checks

        // Query
       // console.log(query);
        pool1.request() // or: new sql.Request(pool1)
            .query(query, (err, result) => {
                // ... error checks
                //console.dir(result)
            console.log("save-querySet-toDB pool err: "+err+insertList);

                res.send("ok");
               console.log(query);
            })

    })

    pool1.on('error', err => {
        // ... error handler
        //console.log("pool1: "+err);
    })

});

app.post('/excute', function (req, res) {
    console.log(chalk.red("/excute_: "));
    re = req.body;
   // printLog(re, con);

    //변수 선언
    var cIdx = -1, nIdx = -1, cnt = 0, ncnt = 0
    var includelist = "", notIncludelst = "";
    var detailSet = [];
    var q1 = "", q2 = "", finalquery = "";
    var q1w = "", q2w = "";

    // 수신된 데이터 객체화 시키기.
    // includeSet =includelist, notIncludelst

    for (var i = 0; i < re.querySet.length; i++) {
        //선정조건
        if (re.querySet[i].criteria_state == 1) {
            for (var j = 0; j < re.querySet[i].criteria_detail_set.length; j++) {
                detailSet = re.querySet[i].criteria_detail_set[j];
                if (detailSet.criteria_detail_table != 0) {
                    //4 = condition. 
                    if (detailSet.criteria_detail_table == 4) {
                        cIdx = cnt;
                    }
                    includelist +=
                        "(SELECT provider_id, person_id FROM " + cdm[detailSet.criteria_detail_table].tableName +
                        " WHERE " +
                        cdm[detailSet.criteria_detail_table].fieldSet[detailSet.criteria_detail_field] + " " + condition[detailSet.criteria_detail_condition - 1] + " " + detailSet.criteria_detail_value
                        + ") a" + cnt + ", ";
                    cnt += 1;
                } else {
                    includelist += "(SELECT person_id FROM " + cdm[detailSet.criteria_detail_table].tableName +
                        " WHERE " +
                        cdm[detailSet.criteria_detail_table].fieldSet[detailSet.criteria_detail_field] + " " + condition[detailSet.criteria_detail_condition - 1] + " " + detailSet.criteria_detail_value
                        + ") a" + cnt + ", ";
                    cnt += 1;
                }
            }
        } else {
            //비선정조건
            for (var j = 0; j < re.querySet[i].criteria_detail_set.length; j++) {
                detailSet = re.querySet[i].criteria_detail_set[j];
                if (detailSet.criteria_detail_table != 0) {
                    if (detailSet.criteria_detail_table == 4) {
                        nIdx = ncnt;
                    }
                    notIncludelst +=
                        "(SELECT  person_id FROM " + cdm[detailSet.criteria_detail_table].tableName +
                        " WHERE " +
                        cdm[detailSet.criteria_detail_table].fieldSet[detailSet.criteria_detail_field] + " " + condition[detailSet.criteria_detail_condition - 1] + " " + detailSet.criteria_detail_value
                        + ") UNION ";
                    ncnt += 1;
                } else {
                    notIncludelst += "(SELECT person_id FROM " + cdm[detailSet.criteria_detail_table].tableName +
                        " WHERE " +
                        cdm[detailSet.criteria_detail_table].fieldSet[detailSet.criteria_detail_field] + " " + condition[detailSet.criteria_detail_condition - 1] + " " + detailSet.criteria_detail_value
                        + ") UNION ";
                    ncnt += 1;
                }
            }
        }

    }


    for (var i = 0; i < cnt; i++) {
        if (i + 1 != cnt) {
            if (i != cIdx) {
                q1w += "(a" + cIdx + ".person_id = a" + i + ".person_id)";
                q1w += " and ";
            }
        }
    }

    // 쿼리문 작성
    q1 = "SELECT DISTINCT a" + cIdx + ".provider_id, a" + cIdx + ".person_id " +
        " FROM " + includelist.slice(0, includelist.length - 2) +
        " WHERE " + q1w.slice(0, q1w.length - 4);

    q2 = notIncludelst.slice(0, notIncludelst.length - 6)

    finalquery = "SELECT a.provider_id, count(a.person_id) as cnt FROM ( " + q1 + " ) a LEFT JOIN ( " + q2 + " ) b ON a.person_id = b.person_id WHERE b.person_id IS NULL GROUP BY a.provider_id";
    //console.log(q1);
    //console.log(q2);

    query_select_personList = "SELECT a.provider_id, a.person_id  FROM ( " + q1 + " ) a LEFT JOIN ( " + q2 + " ) b ON a.person_id = b.person_id WHERE b.person_id IS NULL";

   //lp("finalquery", finalquery);
    lp("query_select_personList", query_select_personList);

    result = query_select_personList;
    
    //조건에 맞는 환자를 찾아서 의사별 환자수 넣기. (select -> delete -> insert)
    query1_select(finalquery, res);


    // const pool11 = new sql.ConnectionPool(cdmDbConfig, err => {
    //     // ... error checks

    //     // Query
    //     // console.log(query);
    //     pool11.request() // or: new sql.Request(pool1)
    //         .query(query_select_personList, (err, result) => {
    //             // ... error checks
    //             //console.dir(result)
    //             if (result != null) {
    //                 let rows = result.recordset
    //                 console.log("query_select_personList: " + rows.length);
    //                 //console.log("save-querySet-toDB pool err: "+err+ ", "+rows);

    //                 // res.send("ok");

    //                 var outInsert = "";
    //                 var outDelete = "";

    //                 // re -> { info: { study_id: 1, query_id: 0 },
    //                 for (var i = 0; i < rows.length; i++) {
    //                     outInsert += "( " + rows[i].provider_id + ", " + rows[i].person_id + ", " + re.info.study_id + ", " + re.info.query_id + ")";
    //                     if (i + 1 != rows.length) {
    //                         outInsert += ", ";
    //                     }
    //                 }
    //                 outDelete += "DELETE FROM doctor_result_person_list WHERE query_id = " + re.info.query_id + " and study_id = " + re.info.study_id + "; ";


    //                 //console.log("/excute_pool1: " +outInsert);
    //                 var query_insert_excute_result = outDelete + "INSERT INTO doctor_result_person_list VALUES " + outInsert;
    //                 lp("query_insert_excute_result", query_insert_excute_result);

    //                 const pool22 = new sql.ConnectionPool(dbConfig, err => {
    //                     // ... error checks

    //                     // Query
    //                     // console.log(query);
    //                     pool22.request() // or: new sql.Request(pool1)
    //                         .query(query_insert_excute_result, (err, result) => {
    //                             // ... error checks
    //                             //console.dir(result)
    //                             //res.send("select Ok");
    //                             console.log("excute_pool11: " + err);
    //                             res.send("execute OK");
    //                         })

    //                 })

    //                 pool22.on('error', err => {
    //                     // ... error handler
    //                     //console.log("pool1: "+err);
    //                 })
    //             }



    //         })

    // })

    // pool11.on('error', err => {
    //     // ... error handler
    //     console.log("pool1: " + err);
    // })

    function query1_select(query1, res) {
        lp(chalk.green("query1_select()"), query1);

        const pool1 = new sql.ConnectionPool(cdmDbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(query1, (err, result) => {
                    console.log("err:"+ err!=null);
                    if ( result.recordset != 0) {
                        let rows = result.recordset
                        console.log("query1_select()_result: " + rows.length);
                        var outInsert = "";
                        var outDelete = "";

                        // re -> { info: { study_id: 1, query_id: 0 },
                        for (var i = 0; i < rows.length; i++) {
                            outInsert += "(N'전북대학교', " + rows[i].provider_id + ", " + rows[i].cnt + ", " + re.info.query_id + ", " + re.info.study_id + ")";
                            if (i + 1 != rows.length) {
                                outInsert += ", ";
                            }
                        }
                        outDelete += "DELETE FROM doctor_result WHERE query_id = " + re.info.query_id + " and study_id = " + re.info.study_id + "; ";


                        //console.log("/excute_pool1: " +outInsert);
                        var query_insert_excute_result = outDelete + "INSERT INTO doctor_result VALUES " + outInsert;
                        console.log("/excute_pool1: " + query_insert_excute_result);
                        query1_1_insert(query_insert_excute_result);
                    }else{
                       res.send({message: "0"});
                    }
                })
        })

        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
    }

    function query1_1_insert(query1_1) {
        const pool2 = new sql.ConnectionPool(dbConfig, err => {
            pool2.request() // or: new sql.Request(pool1)
                .query(query1_1, (err, result) => {
                    console.log("excute_pool1: " + err);
                })
        })
        pool2.on('error', err => {
            // ... error handler
            //console.log("pool1: "+err);
        })
    }
});// end



app.post('/excute-patient',function(req,res){
    printLog("/excute-patient:",con);
    re= req.body;
    printLog(re,con);

 var cIdx=-1, nIdx=-1, cnt=0, ncnt=0
 var includelist ="", notIncludelst="";
 var detailSet =[];
 var q1="", q2="", finalquery="";
 var q1w="", q2w="";
for(var i=0;i<re.querySet.length;i++)
{
    //선정조건
    if(re.querySet[i].criteria_state==1)
    {
        for(var j=0;j<re.querySet[i].criteria_detail_set.length;j++)
        {
            detailSet= re.querySet[i].criteria_detail_set[j];
            if(detailSet.criteria_detail_table!=0){
                if(detailSet.criteria_detail_table==4){
                    cIdx=cnt;
                }
                includelist += 
                    "(SELECT provider_id, person_id FROM "+cdm[detailSet.criteria_detail_table].tableName +
                    " WHERE "+ 
                    cdm[detailSet.criteria_detail_table].fieldSet[detailSet.criteria_detail_field] +" "+ condition[detailSet.criteria_detail_condition-1] + " "+detailSet.criteria_detail_value
                    +") a"+cnt+", ";
                    cnt +=1;
            }else{
                includelist += "(SELECT person_id FROM "+cdm[detailSet.criteria_detail_table].tableName +
                        " WHERE "+ 
                        cdm[detailSet.criteria_detail_table].fieldSet[detailSet.criteria_detail_field] +" "+ condition[detailSet.criteria_detail_condition-1]+" " + detailSet.criteria_detail_value
                        +") a"+cnt+", ";
                        cnt +=1;
            }
        }
   
    }else{
        //비선정조건
        for(var j=0;j<re.querySet[i].criteria_detail_set.length;j++)
        {
            detailSet= re.querySet[i].criteria_detail_set[j];
            if(detailSet.criteria_detail_table!=0){
                if(detailSet.criteria_detail_table==4){
                    nIdx=ncnt;
                }
                notIncludelst += 
                    "(SELECT  person_id FROM "+cdm[detailSet.criteria_detail_table].tableName +
                    " WHERE "+ 
                    cdm[detailSet.criteria_detail_table].fieldSet[detailSet.criteria_detail_field] +" "+ condition[detailSet.criteria_detail_condition-1] + " "+detailSet.criteria_detail_value
                    +") UNION ";
                    ncnt +=1;
            }else{
                notIncludelst += "(SELECT person_id FROM "+cdm[detailSet.criteria_detail_table].tableName +
                        " WHERE "+ 
                        cdm[detailSet.criteria_detail_table].fieldSet[detailSet.criteria_detail_field] +" "+ condition[detailSet.criteria_detail_condition-1]+" " + detailSet.criteria_detail_value
                        +") UNION ";
                        ncnt +=1;
            }
        }
    }
 
}
for(var i=0;i<cnt;i++){
    if(i+1!=cnt) {
        if(i!=cIdx){
            q1w += "(a"+cIdx+".person_id = a"+i+".person_id)";
            q1w += " and ";
        }
    }
}
q1 ="SELECT DISTINCT a"+cIdx+".provider_id, a"+cIdx+".person_id "+
            " FROM "+includelist.slice(0,includelist.length-2)+
            " WHERE "+ q1w.slice(0, q1w.length-4);

q2 = notIncludelst.slice(0,notIncludelst.length-6)
lp('q1', q1);
lp('q2',q2);

finalquery = "SELECT DISTINCT a.person_id FROM ( "+q1+" ) a LEFT JOIN ( "+q2+" ) b ON a.person_id = b.person_id WHERE b.person_id IS NULL";
lp("finalquery_1", finalquery);

finalquery = "SELECT person_id, year_of_birth, gender_concept_id FROM person a where person_id IN ("+finalquery+")";
//console.log(q1);
//console.log(q2);
lp("excute-patient",finalquery);

const pool1 = new sql.ConnectionPool(cdmDbConfig, err => {
    // ... error checks

    // Query
   // console.log(query);
    pool1.request() // or: new sql.Request(pool1)
        .query(finalquery, (err, result) => {
            // ... error checks
            //console.dir(result)
            let rows = result.recordset
            console.log("query result: "+rows.length);
            //console.log("save-querySet-toDB pool err: "+err+ ", "+rows);
           
            // res.send("ok");

            var outInsert ="";
            var outDelete ="";
            
            // re -> { info: { study_id: 1, query_id: 0 },
            // person_id, year_of_birth, gender_concept_id
            for(var i=0;i<rows.length;i++){
                outInsert += "("+re.info.study_id+", "+rows[i].person_id+", "+rows[i].year_of_birth+", N'"+ rows[i].gender_concept_id+"', N'1', "+ re.info.query_id+", 10)"; //의사번호가 없음우선 10
                if(i+1!=rows.length){
                    outInsert += ", ";
                }
            }
            outDelete += "DELETE FROM patient_result WHERE query_id = "+re.info.query_id +" and study_id = "+re.info.study_id+"; ";

            //console.log("/excute_pool1: " +outInsert);
            var query_insert_excute_result = outDelete +"INSERT INTO patient_result VALUES "+outInsert;
            console.log("/excute_pool1: "+query_insert_excute_result);
            
            const pool2 = new sql.ConnectionPool(dbConfig, err => {
                // ... error checks
                
                // Query
               // console.log(query);
                pool2.request() // or: new sql.Request(pool1)
                    .query(query_insert_excute_result, (err, result) => {
                        // ... error checks
                        //console.dir(result)
                        //res.send("select Ok");
                        console.log("excute_pool1: "+err);
                       
                    })
        
            })
        
            pool2.on('error', err => {
                // ... error handler
                //console.log("pool1: "+err);
            })

          
        })

})

pool1.on('error', err => {
    // ... error handler
    console.log("pool1: "+err);
})





});
app.get('/doctor-result-person-list/:study_id/:query_id',function(req,res){
    lp("get, /doctor-result-person-list","start");
     var study_id = Number(req.param('study_id'));
     var query_id = Number(req.param('query_id'));

    // study_id = 3; query_id=2;
    //스터디, 쿼리 해당 환자 뽑기. 
    var query = "SELECT DISTINCT person_id FROM doctor_result_person_list WHERE study_id = "+study_id +" and query_id = "+query_id +" ;"; 
    lp("query", query)
    const pool1 = new sql.ConnectionPool(dbConfig, err => {
        pool1.request() // or: new sql.Request(pool1)
            .query(query, (err, result) => {
                // ... error checks
                //console.dir(result)
                if (result != null) {
                    let rows = result.recordset
                    console.log("result: "+rows.length);
                    //res.send("ok");
                    // res.send('ok');
                    var personList = person_set_by_rows_string(rows);
                    console.log(personList);
                    lp("personList",personList);

                    var pool2_query = "SELECT  person_id, gender_concept_id, year_of_birth FROM person where person_id in ( "+ personList + " )";
                    lp("pool2_query",pool2_query)
                    const pool2 = new sql.ConnectionPool(cdmDbConfig, err => {
                        pool2.request() // or: new sql.Request(pool1)
                            .query(pool2_query, (err, result) => {
                                // ... error checks
                                //console.dir(result)
                                if (result != null) {
                                    let rows = result.recordset
                                    console.log("pool2_query_result: " + rows.length);
                                    var set = person_set_by_rows(rows);

                                    res.send(set);
                                    
                                }
                            })
                    })
                    pool2.on('error', err => {
                        // ... error handler
                        console.log("pool2: " + err);
                    })
                }
            })
    })
    pool1.on('error', err => {
        // ... error handler
        console.log("pool1: " + err);
    })
})

app.post('/persons-detail', function (req, res) {
    lp('persons-detail','');
    var re = req.body;

    var person_list = '';
    for (i = 0; i < re.person_list.length; i++) {
        person_list += re.person_list[i];
        if (i + 1 != re.person_list.length) {
            person_list += ", ";
        }
    }
    
    logPrint(person_list);

    var query_condition = "SELECT condition_start_date as date, person_id as id, a.domain_id as type , a.concept_name as name FROM condition_occurrence, concept a \
    WHERE\
    person_id in ( "+ person_list + " ) and \
    condition_concept_id = a.concept_id and a.domain_id='Condition'\
    order by person_id;"


    var query_drug = "SELECT drug_exposure_start_date as date, person_id as id, a.domain_id as type , a.concept_name as name FROM drug_exposure, concept a \
    WHERE\
    person_id in ( "+ person_list + " ) and \
    drug_concept_id = a.concept_id and a.domain_id='drug'\
    order by person_id;"

  
    logPrint(query_condition);
    logPrint(query_drug);

    const pool1 = new sql.ConnectionPool(cdmDbConfig, err => {
        pool1.request() // or: new sql.Request(pool1)
            .query(query_condition, (err, result) => {
                // ... error checks
                //console.dir(result)
                if (result != null) {
                    let rows = result.recordset
                    console.log("query result: " + rows.length);
                    var person_detail = add_personList(rows);
                    logPrint(person_detail.length);
                    // res.send('ok');
                    const pool2 = new sql.ConnectionPool(cdmDbConfig, err => {
                        pool2.request() // or: new sql.Request(pool1)
                            .query(query_drug, (err, result) => {
                                // ... error checks
                                //console.dir(result)
                                if (result != null) {
                                    let rows = result.recordset
                                    console.log("query_drug result: " + rows.length);
                                    person_detail = person_detail.concat(add_personList(rows))
                                    logPrint(person_detail.length);
                                    res.send(person_detail);
                                    
                                }
                            })
                    })
                    pool2.on('error', err => {
                        // ... error handler
                        console.log("pool2: " + err);
                    })
                }
            })
    })
    pool1.on('error', err => {
        // ... error handler
        console.log("pool1: " + err);
    })
   
})


app.get('/test-query/:study_id/:query_id', (req, res) => {
    var study_id = Number(req.param("study_id"));
    var query_id = Number(req.param("query_id"));
    //실행 쿼리 테스트 
    //DB에 저장된 자료 가져오기 
    //가져오고난뒤 쿼리문 만들어서 수행 시작 후 결과값 반납하기.

    //update query_status
    update_status_running(study_id, query_id);

    function update_status_running(study_id, query_id) {
        var query = "UPDATE query SET query_status = N'In Progress' WHERE study_id = " + study_id + " and query_id = " + query_id + " ;";
        lp("update_status_running()", query);
        const pool1 = new sql.ConnectionPool(dbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(query, (err, result) => {
                    if (result != null) {
                        let rows = result.recordset
                        console.log("query result");
                        console.log(result);
                        console.log();
                        // 쿼리문 만들기.
                        res.send({ status: "update is ok" });
                        get_detail(study_id, query_id);;
                    } else {
                        console.log(err);
                        res.send(err);
                    }
                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
    }
    function get_detail(study_id, query_id, res1) {
        var query1 = "SELECT * FROM criteria_detail WHERE study_id = " + study_id + " and query_id = " + query_id;
        lp("get_detail()", query1);
        const pool1 = new sql.ConnectionPool(dbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(query1, (err, result) => {
                    if (result != null) {
                        let rows = result.recordset
                        console.log("query result: " + rows.length);
                        // 쿼리문 만들기.
                        if (rows.length != 0) {
                            //1= inclusion, 2=exclusion 
                            var in_rows = [], ex_rows = [];
                            for (i = 0; i < rows.length; i++) {
                                if (rows[i].criteria_state == 1) {
                                    in_rows.push(rows[i]);
                                } else {
                                    ex_rows.push(rows[i]);
                                }
                            }
                            //console.log(in_rows);
                            //res1.send(make_query(in_rows, 1));


                            //두번째
                            var inclusion = make_query(in_rows, 1);
                            var exclusion = make_query(ex_rows, 0);

                            //console.log(inclusion);

                            make_query_execute(inclusion, exclusion, study_id, query_id);
                            //console.log("get_detail err: " +err);
                        } else {
                            //res1.send("0");
                            console.log("rows.length =0");
                        }


                    } else {
                        console.log("get_detail err: " + err);
                        //res1.send(err);
                    }
                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
    }
    // rows값, condition 조건 1 - inclusion, 0- exclusion;
    function make_query(rows, in_or_ex) {
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
                    result.measurement.push("( SELECT person_id, provider_id FROM measurement WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " and provider_id IS NOT NULL)");
                    break;
            }
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
            // condition 내용 유무
            console.log("condition유무: " + idx);

            //1 = inclusion, 0=exclusion

            var buf_tables = [];
            for (i = 0; i < tables.length; i++) {
                if (f_q[tables[i]].length != 0) {
                    buf_tables.push(tables[i]);
                }
            }
            console.log("Inserted tables:");
            console.log(buf_tables);
            console.log();

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
                        console.log("--final query--" + buf_tables[i]);
                        console.log(final);
                        console.log();
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
            console.log("f_where.length: " + f_where.length);
            console.log();

            if(f_where.length !=1){
                final += where_f + "and b" + f_where[0] + ".person_id = b" + f_where[f_where.length - 1] + ".person_id and b" + idx + ".person_id = p1.person_id";
            }else{
                final += where_f + " b" + f_where[0] + ".person_id = b" + f_where[f_where.length - 1] + ".person_id and b" + idx + ".person_id = p1.person_id";
            }
            
            console.log("last final");
            console.log(final);
            console.log();

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
        return final;
    }
    function make_query_execute(inclusion, exclusion, study_id, query_id) {
        var query;
        if (inclusion.length == 0 || exclusion == 0) {
            query = "SELECT provider_id, A.person_id, A.year_of_birth, A.gender_concept_id FROM ( " + inclusion + " ) A  ;"
        } else {
            query = "SELECT provider_id, A.person_id, A.year_of_birth, A.gender_concept_id FROM ( " + inclusion + " ) A LEFT JOIN ( " + exclusion + " ) B ON A.person_id = B.person_id WHERE B.person_id IS NULL;"
        }
        lp("make_query_execute()", query);
        console.log("inclusion : " + inclusion.length);
        console.log("exclusion : " + exclusion.length);
        console.log();
        // console.log()
        // console.log(inclusion);
        // console.log()
        // console.log(exclusion);
        console.log("make_query_execute_query_start!!");
        const pool1 = new sql.ConnectionPool(cdmDbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(query, (err, result) => {
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
                            }
                            //console.log(q_list);
                            //res.send(q_list);


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
                            make_query_delete(q_execute, study_id, query_id);
                        } else {
                            console.log({ result: 0 })
                        }
                        console.log(err);
                    } else {
                        console.log(err);
                        //res.send(err);
                    }

                })// query
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
        return query;
    }
    function output_string(obj) {
        var out = obj.d9 + ", " + obj.d10 + ", " + obj.d20 + ", " + obj.d30 + ", " + obj.d40 + ", " + obj.d50 + ", " + obj.d60 + ", " + obj.d70;
        return out;
    }
    function make_query_execute2(rows1, query) {
        const pool1 = new sql.ConnectionPool(cdmDbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(query, (err, result) => {
                    var q_execute = []; var q_cnt = 0; // 1000개 이상일 경우 
                    if (result != null) {
                        let rows = result.recordset
                        console.log("query result: " + rows.length);
                        //결과값 0 체크
                        if (rows.length != 0) {
                            for (i = 0; i < rows.length; i++) {
                                q_execute.push("(2, " + rows[i].provider_id + ", " + rows[i].cnt + ", " + query_id + ", " + study_id + ")");
                            }
                            //console.log("q_execute");
                            //console.log(q_execute)
                            //res.send(q_execute);
                            make_query_delete(q_execute, study_id, query_id);
                        } else {
                            res.send({ result: 0 });
                        }
                        console.log(err);
                    } else {
                        //console.log(err);
                        res.send(err);
                    }

                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
    }
    function make_query_delete(q_execute, study_id, query_id) {
        lp("make_query_delete", "study_id: " + study_id + ", query_id: " + query_id);
        var query = "DELETE FROM doctor_result WHERE study_id = " + study_id + " and query_id = " + query_id;
        const pool1 = new sql.ConnectionPool(dbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(query, (err, result) => {
                    console.log("make_query_delete_result_log: " + result);
                    make_queyr_insert(q_execute, study_id, query_id);
                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
    }

    function make_queyr_insert(q_execute, study_id, query_id) {
        lp("make_queyr_insert", "study_id: " + study_id + ", query_id: " + query_id);
        //파라미타 리스트 값을 가지고 1000 개씩 끊어서 쿼리문 만들어주기, 

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
        console.log("q_list1[0]");
        console.log(q_list1);
        console.log();
        const pool1 = new sql.ConnectionPool(dbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(q_list1, (err, result) => {
                    console.log(result);
                    //res.send(q_final);
                    //Update status =2
                    query_update_set_status_2(study_id, query_id);
                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
    }

    function query_update_set_status_2(study_id, query_id) {
        var query = "UPDATE query SET query_status = N'Done' WHERE study_id = " + study_id + " and query_id = " + query_id + " ;";
        lp("query_update_set_status_2()", query);
        const pool1 = new sql.ConnectionPool(dbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(query, (err, result) => {
                    if (result != null) {
                        let rows = result.recordset
                        console.log("query result");/////no-check
                        console.log(result);
                        console.log();
                        // 쿼리문 만들기.
                        //res.send({status:"update is ok"});
                        //get_detail(study_id, query_id);;
                    } else {
                        console.log(err);
                        //res.send(err);
                    }
                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
    }
})// end execute

app.get('/test-protocol-query/:protocol_id/',(req, res)=>{
    // 1. 파라미타값 가져오기
    // 2. Action 값 변경하기.
    // 쿼리조건 가져오기 
    // 쿼리 만들기
    // 결과값 person_id 저장하기

    // 1.
    var protocol_id = Number(req.param("protocol_id"));
    
    //2. 
    update_status_running(protocol_id);

    function update_status_running(protocol_id){
        var query = "UPDATE protocol SET protocol_status = N'In Progress' WHERE protocol_id = 111"+protocol_id+" ;";
        lp("update_status_running()", query);
        const pool1 = new sql.ConnectionPool(dbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(query, (err, result) => {
                    if (result != null) {
                        let rows = result.recordset
                        console.log("query result");
                        console.log(result);
                        console.log();
                        // 쿼리문 만들기.
                        //res.send({status:"In Progress"});
                        get_detail(protocol_id);;
                    }else{
                        console.log(err);
                        res.send(err);
                    }
                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
    }
    function get_detail(protocol_id) {
        var query1 = "SELECT * FROM protocol_criteria_detail WHERE protocol_id = " + protocol_id+" ;";
        lp("get_detail()", query1);
        const pool1 = new sql.ConnectionPool(dbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(query1, (err, result) => {
                    if (result != null) {
                        let rows = result.recordset
                        console.log("query result: " + rows.length);
                        // 쿼리문 만들기.
                        if (rows.length != 0) {
                            //1= inclusion, 2=exclusion 
                            var in_rows = [], ex_rows = [];
                            for (i = 0; i < rows.length; i++) {
                                if (rows[i].criteria_state == 1) {
                                    in_rows.push(rows[i]);
                                } else {
                                    ex_rows.push(rows[i]);
                                }
                            }
                            //console.log(in_rows);
                            //res.send(make_query(in_rows, 1));


                            //두번째
                            var inclusion = make_query(in_rows, 1);
                            var exclusion = make_query(ex_rows, 0);

                            console.log(inclusion);

                            make_query_execute(inclusion, exclusion, protocol_id);
                            console.log("get_detail err: " +err);
                        } else {
                            res.send("0");
                            console.log("rows.length =0");
                        }

                    } else {
                        console.log("get_detail err: " + err);
                        //res1.send(err);
                    }
                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
    }
    // rows값, condition 조건 1 - inclusion, 0- exclusion;
    function make_query(rows, in_or_ex) {
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
                    result.measurement.push("( SELECT person_id, provider_id FROM measurement WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " and provider_id IS NOT NULL)");
                    break;
            }
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
            // condition 내용 유무
            console.log("condition유무: " + idx);

            //1 = inclusion, 0=exclusion

            var buf_tables = [];
            for (i = 0; i < tables.length; i++) {
                if (f_q[tables[i]].length != 0) {
                    buf_tables.push(tables[i]);
                }
            }
            console.log("Inserted tables:");
            console.log(buf_tables);
            console.log();

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
                        console.log("--final query--" + buf_tables[i]);
                        console.log(final);
                        console.log();
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
            console.log("f_where.length: " + f_where.length);
            console.log();

            if(f_where.length!=1){
                final += where_f + " and  b" + f_where[0] + ".person_id = b" + f_where[f_where.length - 1] + ".person_id and b" + idx + ".person_id = p1.person_id";
            }else{
                final += where_f + " b" + f_where[0] + ".person_id = b" + f_where[f_where.length - 1] + ".person_id and b" + idx + ".person_id = p1.person_id";
            }
            
            console.log("last final");
            console.log(final);
            console.log();

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
        return final;
    }
    function make_query_execute(inclusion, exclusion, protocol_id) {
        var query;
        if (inclusion.length == 0 || exclusion == 0) {
            query = "SELECT DISTINCT A.person_id FROM ( " + inclusion + " ) A  ;"
        } else {
            query = "SELECT DISTINCT A.person_id FROM ( " + inclusion + " ) A LEFT JOIN ( " + exclusion + " ) B ON A.person_id = B.person_id WHERE B.person_id IS NULL;"
        }
        lp("make_query_execute()", query);
        console.log("inclusion : " + inclusion.length);
        console.log("exclusion : " + exclusion.length);
        console.log();
        // console.log()
        // console.log(inclusion);
        // console.log()
        // console.log(exclusion);
        console.log("make_query_execute_query_start!!");
        const pool1 = new sql.ConnectionPool(cdmDbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(query, (err, result) => {
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
                            
                            //make_query_execute2(rows);
                            // --원본--
                            console.log("q_execute start")

                            for (i = 0; i < rows.length; i++) {
                                    q_execute.push("( "+rows[i].person_id+", "+protocol_id+" )");
                            }
                            //console.log("q_execute");
                            //console.log(q_execute)
                            //res.send(q_execute);
                            make_query_delete(q_execute, protocol_id);
                        } else {
                            console.log({ result: 0 })
                        }
                        console.log(err);
                    } else {
                        console.log(err);
                        //res.send(err);
                    }

                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
        return query;
    }
    function make_query_delete(q_execute, protocol_id) {
        lp("make_query_delete", "protocol_id: " + protocol_id);
        var query = "DELETE FROM protocol_result WHERE protocol_id = " + protocol_id +" ;";
        const pool1 = new sql.ConnectionPool(dbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(query, (err, result) => {
                    console.log("make_query_delete_result_log: " + result);
                    make_queyr_insert(q_execute, protocol_id);
                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
    }
    function make_queyr_insert(q_execute, protocol_id) {
        lp("make_queyr_insert", "protocol_id: " + protocol_id);
        //파라미타 리스트 값을 가지고 1000 개씩 끊어서 쿼리문 만들어주기, 

        var cnt_1000 = 0;
        var query = "";
        var q_final = [];
        //console.log(q_execute);
        for (i = 0; i < q_execute.length; i++) {
            query += q_execute[i];
            //console.log(q_execute[i]);
            cnt_1000 += 1;
            if (cnt_1000 + 1 == 1001) {
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
        console.log("q_list1");
        console.log(q_list1);
        console.log();
        const pool1 = new sql.ConnectionPool(dbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(q_list1, (err, result) => {
                    console.log({log: result, error: err});
                    //res.send(q_final);
                    //Update status =2
                    query_update_set_status_2(protocol_id);
                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
    }
    function query_update_set_status_2(protocol_id) {
        var query = "UPDATE protocol SET protocol_status = N'Done' WHERE protocol_id = 111" + protocol_id + " ;";
        lp("query_update_set_status_2()", query);
        const pool1 = new sql.ConnectionPool(dbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(query, (err, result) => {
                    if (result != null) {
                        let rows = result.recordset
                        console.log("query result");/////no-check
                        console.log(result);
                        console.log();
                        // 쿼리문 만들기.
                        
                        //get_detail(study_id, query_id);;



                        //임시 시작
                        lp('Req: /test-protocol-result/:protocol_id', 'protocol_id: '+protocol_id);
                        asyncMain(protocol_id,res, dbConfig);
                        //res.send("ok");
                        async function asyncMain(protocol_id, res, dbConfig ) {
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
                            let person_info =[];

                            let result_person = await pool.request()
                                .input('input_paramiter', sql.Int, protocol_id)
                                .query('SELECT person_id FROM dbo.protocol_result WHERE protocol_id = @input_paramiter')
                            person_list = make_list_to_string(result_person);  //console_println(person_list); //log찍기
                            
                            //// 환자
                            let result_person1 = await pool.request()
                                .query('SELECT gender_concept_id, year_of_birth FROM BIGDATADB.dbo.PERSON WHERE person_id in ( '+person_list+' );')
                            person_info = make_result_person_to_list(result_person1.recordset);

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
                                person_info: person_info,
                                drug_info: drug_info,
                                measurement_info: measurement_info,
                                condition_info: condition_info,
                                visit_info:visit_info
                            }; //console_println(result);

                            // 환자통계, 진단통계, 약통계, 랩통계 정리해보자.
                            // 각 내용을 JSON으로 받는다.
                            // 각 내용을 컬럼에 맞게 디비를 저장해준다.
                            let distribution_person         = make_distribution_person(result.person_info, result.visit_info);
                            let distribution_condition      = make_distribution_condition(result.condition_info);
                            let distribution_drug           = make_distribution_drug(result.drug_info);
                            let distribution_measurement    = make_distribution_measurement(result.measurement_info);
                            
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
                                .query('INSERT INTO protocol_result1 VALUES( @protocol_id, @input_person, @input_condition, @input_drug, @input_measurement)')
                                console.log(query_insert_distribution);
                            

                            //res.send(JSON.stringify(distribution)); 
                            res.send({result: 'ok'});
                        }catch(err){
                            console.log({error: err});
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
                            
                            // 나이 및 성별 분포
                            for(i=0;i<person.length;i++){
                                //gender
                                if(person[i].gender_concept_id==8507){
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
                        }

                        //임시 끝





                    } else {
                        console.log(err);
                        //res.send(err);
                    }
                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
    }


});///test-protocol-query/:study_id/:query_id'


// 임시 함수 만들기.. 



app.get('/test-protocol-result/:protocol_id/', (req, res) => {
    var protocol_id = Number(req.param("protocol_id"));
    lp('Req: /test-protocol-result/:protocol_id', 'protocol_id: '+protocol_id);
    asyncMain(protocol_id,res, dbConfig);
    //res.send("ok");
    async function asyncMain(protocol_id, res, dbConfig ) {
        try {
            let pool = await sql.connect(dbConfig);
           // let result = [];
    
            // 해당 사람 리스트 가져오기.
            // drug, condition. measurement, visit 순으로 가져오기
    
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
                person_info: person_info,
                drug_info: drug_info,
                measurement_info: measurement_info,
                condition_info: condition_info,
                visit_info:visit_info
            }; //console_println(result);

            // 환자통계, 진단통계, 약통계, 랩통계 정리해보자.
            // 각 내용을 JSON으로 받는다.
            // 각 내용을 컬럼에 맞게 디비를 저장해준다.
            let distribution_person         = make_distribution_person(result.person_info, result.visit_info);
            let distribution_condition      = make_distribution_condition(result.condition_info);
            let distribution_drug           = make_distribution_drug(result.drug_info);
            let distribution_measurement    = make_distribution_measurement(result.measurement_info);
            
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
                .query('INSERT INTO protocol_result1 VALUES( @protocol_id, @input_person, @input_condition, @input_drug, @input_measurement)')
                console.log(query_insert_distribution);
          

            //res.send(JSON.stringify(distribution)); 
            res.send({result: 'ok'});
            console.log('done');
        }catch(err){
            console.log({error: err});
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
            
            // 나이 및 성별 분포
            for(i=0;i<person.length;i++){
                //gender
                if(person[i].gender_concept_id==8507){
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
    }
    
});

app.get('/test-query-patient-result/:study_id/:query_id',(req, res)=>{
    var study_id = Number(req.param("study_id"));
    var query_id = Number(req.param("query_id"));
    //실행 쿼리 테스트 
    //DB에 저장된 자료 가져오기 
    //가져오고난뒤 쿼리문 만들어서 수행 시작 후 결과값 반납하기.
    
    //update query_status
    update_status_running(study_id,query_id);

    function update_status_running(study_id, query_id) {
        var query = "UPDATE query SET query_status = N'In Progress' WHERE study_id = " + study_id + " and query_id = " + query_id + " ;";
        lp("update_status_running()", query);
        const pool1 = new sql.ConnectionPool(dbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(query, (err, result) => {
                    if (result != null) {
                        let rows = result.recordset
                        console.log("query result");
                        console.log(result);
                        console.log();
                        // 쿼리문 만들기.
                        res.send({ status: "update is ok" });
                        get_detail(study_id, query_id);;
                    } else {
                        console.log(err);
                        res.send(err);
                    }
                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
    }
    function get_detail(study_id, query_id, res1) {
        var query1 = "SELECT * FROM criteria_detail WHERE study_id = " + study_id + " and query_id = " + query_id;
        lp("get_detail()", query1);
        const pool1 = new sql.ConnectionPool(dbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(query1, (err, result) => {
                    if (result != null) {
                        let rows = result.recordset
                        console.log("query result: " + rows.length);
                        // 쿼리문 만들기.
                        if (rows.length != 0) {
                            //1= inclusion, 2=exclusion 
                            var in_rows = [], ex_rows = [];
                            for (i = 0; i < rows.length; i++) {
                                if (rows[i].criteria_state == 1) {
                                    in_rows.push(rows[i]);
                                } else {
                                    ex_rows.push(rows[i]);
                                }
                            }
                            //console.log(in_rows);
                            //res1.send(make_query(in_rows, 1));


                            //두번째
                            var inclusion = make_query(in_rows, 1);
                            var exclusion = make_query(ex_rows, 0);

                            //console.log(inclusion);

                            make_query_execute(inclusion, exclusion, study_id, query_id);
                            //console.log("get_detail err: " +err);
                        } else {
                            //res1.send("0");
                            console.log("rows.length =0");
                        }


                    } else {
                        console.log("get_detail err: " + err);
                        //res1.send(err);
                    }
                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
    }
    // rows값, condition 조건 1 - inclusion, 0- exclusion;
    function make_query(rows, in_or_ex) {
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
                    result.measurement.push("( SELECT person_id, provider_id FROM measurement WHERE " + cdm[table].fieldSet[field] + " " + condition[con] + " " + val + " and provider_id IS NOT NULL)");
                    break;
            }
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
            // condition 내용 유무
            console.log("condition유무: " + idx);

            //1 = inclusion, 0=exclusion

            var buf_tables = [];
            for (i = 0; i < tables.length; i++) {
                if (f_q[tables[i]].length != 0) {
                    buf_tables.push(tables[i]);
                }
            }
            console.log("Inserted tables:");
            console.log(buf_tables);
            console.log();

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
                        console.log("--final query--" + buf_tables[i]);
                        console.log(final);
                        console.log();
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
            console.log("f_where.length: " + f_where.length);
            console.log();


            final += where_f + "and b" + f_where[0] + ".person_id = b" + f_where[f_where.length - 1] + ".person_id and b" + idx + ".person_id = p1.person_id";
            console.log("last final");
            console.log(final);
            console.log();

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
        return final;
    }
    function make_query_execute(inclusion, exclusion, study_id, query_id) {
        var query;
        if (inclusion.length == 0 || exclusion == 0) {
            query = "SELECT provider_id, A.person_id, A.year_of_birth, A.gender_concept_id FROM ( " + inclusion + " ) A  ;"
        } else {
            query = "SELECT provider_id, A.person_id, A.year_of_birth, A.gender_concept_id FROM ( " + inclusion + " ) A LEFT JOIN ( " + exclusion + " ) B ON A.person_id = B.person_id WHERE B.person_id IS NULL;"
        }
        lp("make_query_execute()", query);
        console.log("inclusion : " + inclusion.length);
        console.log("exclusion : " + exclusion.length);
        console.log();
        // console.log()
        // console.log(inclusion);
        // console.log()
        // console.log(exclusion);
        console.log("make_query_execute_query_start!!");
        const pool1 = new sql.ConnectionPool(cdmDbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(query, (err, result) => {
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
                            make_query_delete(q_execute, study_id, query_id);
                        } else {
                            console.log({ result: 0 })
                        }
                        console.log(err);
                    } else {
                        console.log(err);
                        //res.send(err);
                    }

                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
        return query;
    }
    function output_string(obj) {
        var out = obj.d9 + ", " + obj.d10 + ", " + obj.d20 + ", " + obj.d30 + ", " + obj.d40 + ", " + obj.d50 + ", " + obj.d60 + ", " + obj.d70;
        return out;
    }
    function make_query_execute2(rows1, query) {
        const pool1 = new sql.ConnectionPool(cdmDbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(query, (err, result) => {
                    var q_execute = []; var q_cnt = 0; // 1000개 이상일 경우 
                    if (result != null) {
                        let rows = result.recordset
                        console.log("query result: " + rows.length);
                        //결과값 0 체크
                        if (rows.length != 0) {
                            for (i = 0; i < rows.length; i++) {
                                q_execute.push("(2, " + rows[i].provider_id + ", " + rows[i].cnt + ", " + query_id + ", " + study_id + ")");
                            }
                            //console.log("q_execute");
                            //console.log(q_execute)
                            //res.send(q_execute);
                            make_query_delete(q_execute, study_id, query_id);
                        } else {
                            res.send({ result: 0 });
                        }
                        console.log(err);
                    } else {
                        //console.log(err);
                        res.send(err);
                    }

                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
    }
    function make_query_delete(q_execute, study_id, query_id) {
        lp("make_query_delete", "study_id: " + study_id + ", query_id: " + query_id);
        var query = "DELETE FROM doctor_result WHERE study_id = " + study_id + " and query_id = " + query_id;
        const pool1 = new sql.ConnectionPool(dbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(query, (err, result) => {
                    console.log("make_query_delete_result_log: " + result);
                    make_queyr_insert(q_execute, study_id, query_id);
                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
    }

    function make_queyr_insert(q_execute, study_id, query_id) {
        lp("make_queyr_insert", "study_id: " + study_id + ", query_id: " + query_id);
        //파라미타 리스트 값을 가지고 1000 개씩 끊어서 쿼리문 만들어주기, 

        var cnt_1000 = 0;
        var query = "";
        var q_final = [];
        //console.log(q_execute);
        for (i = 0; i < q_execute.length; i++) {
            query += q_execute[i];
            //console.log(q_execute[i]);
            cnt_1000 += 1;
            if (cnt_1000 + 1 == 1000) {
                q_final.push("INSERT INTO doctor_result VALUES " + query + " ;");
                cnt_1000 = 0;
                query = "";
            } else {
                if (i + 1 != q_execute.length) {
                    query += ", ";
                }

            }
        }

        q_final.push("INSERT INTO doctor_result VALUES " + query + " ;");
        //res.send(q_final);
        console.log("q_final.length: " + q_final.length);
        q_list1 = "";
        for (i = 0; i < q_final.length; i++) {
            q_list1 += q_final[i];
        }
        console.log("q_list1[0]");
        console.log(q_list1);
        console.log();
        const pool1 = new sql.ConnectionPool(dbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(q_list1, (err, result) => {
                    console.log(result);
                    //res.send(q_final);
                    //Update status =2
                    query_update_set_status_2(study_id, query_id);
                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
    }

    function query_update_set_status_2(study_id, query_id) {
        var query = "UPDATE query SET query_status = N'Done' WHERE study_id = " + study_id + " and query_id = " + query_id + " ;";
        lp("query_update_set_status_2()", query);
        const pool1 = new sql.ConnectionPool(dbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(query, (err, result) => {
                    if (result != null) {
                        let rows = result.recordset
                        console.log("query result");/////no-check
                        console.log(result);
                        console.log();
                        // 쿼리문 만들기.
                        //res.send({status:"update is ok"});
                        //get_detail(study_id, query_id);;
                    } else {
                        console.log(err);
                        //res.send(err);
                    }
                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
    }
})// end execute



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


function makeCriteria(criteria_id, criteria_title){
    this.id = criteria_id;
    this.title = criteria_title;
    this.criteriaDetailSet = [];
}

function makeQuery(id, title, createDate, modifyDate, exeDate, status, queryCreator, lastEditor){
    this.id             = id;
    this.title          = title;
    this.createDate     = createDate;
    this.modifyDate     = modifyDate;
    this.exeDate        = exeDate;
    this.status         = status;
    this.queryCreator   = queryCreator;
    this.lastEditor     = lastEditor;
}

function add(arr, value){
    arr.push(value);
}


function printLog(mes, con){
    if(con==1){
        return ;
    }
    console.log(mes);
}

function personList(date, id, type, name){
   var obj ={
       date : date,
       id : id,
       type : type,
       name : name
   }
   return obj;
}

function add_personList(rows){
    var obj =[];
    for(i=0;i<rows.length;i++){
        obj.push(personList(new Date(rows[i].date).format("yyyy-MM-dd"), rows[i].id, rows[i].type, rows[i].name));
    }
    return obj;
}




// query
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


function logPrint(mes){
    console.log();
    console.log();
    console.log(mes);
    console.log();
}

function lp(title,mes){
    console.log();
    console.log("---"+title+"---");
    console.log(mes);
    console.log();
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
        list.push(make_person_set(i, rows[i].gender_concept_id, (now_year-rows[i].year_of_birth)+1));
    }

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

function calucate_result_execute(rows){

}

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

//로그를 찍어내는 함수.
function console_println(mes){
    console.log(mes);
    console.log();
}
