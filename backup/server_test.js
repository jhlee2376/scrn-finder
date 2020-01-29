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
})
console.log('out')


var con = 0;



var sql = require('mssql');
    //설정
var dbConfig = {
    user: 'sa',
    password: 'admin123!!',
    server: 'localhost',
    port: '1433',
    database: 'SCRN_CLOUD',
}

var cdmDbConfig = {
    user: 'sa',
    password: 'admin123!!',
    server: 'localhost',
    port: '1433',
    database: 'BIGDATADB',
}

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

var condition =["<","<=","=",">",">="];

//웹 서버를 생성
var app = express();
app.use(cors());
app.use(express.static('public'));
app.use(bodyParser.json());

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

app.post('/excute_test',(req,res)=>{
    var re = req.body;
    if(re.length==0){

    }
})

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

    //스터디, 쿼리 해당 환자 뽑기. 
    var query = "SELECT DISTINCT person_id FROM doctor_result_person_list WHERE study_id = "+study_id +" and query_id = "+query_id +" ;"; 
    lp("queyr", query)
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


app.get('/test-query/:study_id/:query_id',(req, res)=>{
    var study_id = Number(req.param("study_id"));
    var query_id = Number(req.param("query_id"));
    //실행 쿼리 테스트 
    //DB에 저장된 자료 가져오기 
    //가져오고난뒤 쿼리문 만들어서 수행 시작 후 결과값 반납하기.
    get_detail(study_id, query_id, res);
    
    function get_detail(study_id, query_id, res1){        
        var query1 ="SELECT * FROM criteria_detail WHERE study_id = "+study_id +" and query_id = "+ query_id;
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
                            var inclusion = make_query(in_rows, 1);
                            var exclusion = make_query(ex_rows, 0);

                            //
                            //두번째. 
                            make_query_execute(inclusion, exclusion, study_id, query_id);

                        }else{
                            res1.send("0");
                        }
                        
                       
                    }else{
                        console.log(err);
                        res1.send(err);
                    }
                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
    }
    // rows값, condition 조건 1 - inclusion, 0- exclusion;
    function make_query(rows,in_or_ex){
        var result = {
            person : [],
            drug : [],
            condition : [],
            measurement : []
        }
        var table=0, field=0;
        var c_id =0;
        for(i=0;i<rows.length;i++){
            table = rows[i].criteria_detail_table;
            field = rows[i].criteria_detail_attribute;
            con = rows[i].criteria_detail_condition-1;
            val = rows[i].criteria_detail_value;
            c_id = rows[i].criteria_id;
            //console.log("SELECT person_id FROM "+ cdm[table].tableName + " WHERE "+cdm[table].fieldSet[field]+" "+condition[con]+ " " + val );
            //condition확인
            //console.log(table);
            switch(Number(table)){
                //person
                case 0:
                    result.person.push("( SELECT person_id, provider_id FROM person WHERE " + cdm[table].fieldSet[field]+" "+condition[con]+ " " + val+" )"  );
                    break;
                //drug
                case 1: 
                    result.drug.push("( SELECT person_id, provider_id FROM drug_exposure WHERE " + cdm[table].fieldSet[field]+" "+condition[con]+ " " + val+" )"  );
                    break;
                //condition
                case 2: 
                    result.condition.push("( SELECT person_id, provider_id FROM condition_occurrence WHERE " + cdm[table].fieldSet[field]+" "+condition[con]+ " " + val+" )"  );
                    break;
                //measurement
                case 3: 
                    result.measurement.push("( SELECT person_id, provider_id FROM measurement WHERE " + cdm[table].fieldSet[field]+" "+condition[con]+ " " + val+" and provider_id IS NOT NULL)"  );
                    break;
            }
        }
        //최종 쿼리 f_q
        //person
        var tables = ["person","drug","condition","measurement"];
        var f_q = {
            person :"", 
            drug :"",
            condition :"",
            measurement :""
        }
        var where ="";
        var final ="";
        if(in_or_ex==1){
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
                    for(j=0;j<result[tables[i]].length-1;j++){
                        if(j+1!=result[tables[i]].length-1){
                            where += "a"+j+".person_id = a"+(j+1)+".person_id and ";
                        }else{
                            where += "a"+j+".person_id = a"+(j+1)+".person_id ";
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
            var idx =0;
            if(result[tables[2]].length!=0){
                idx = 2;
            }else if(result[tables[3]].length!=0){
                idx = 3
            }else if(result[tables[1]].length!=0){
                idx = 1;
            }
    
            //1 = inclusion, 0=exclusion
            
           
               
                var buf_tables=[];
                for(i=0;i<tables.length;i++){
                    if(f_q[tables[i]].length!=0){
                        buf_tables.push(tables[i]);
                    }
                }
                console.log(buf_tables);
                
                idx = buf_tables.indexOf(tables[idx]);

                final += "SELECT b"+idx+".provider_id, b"+idx+".person_id FROM "

                for(i=0;i<buf_tables.length;i++){
                    if(f_q[buf_tables[i]].length!=0){
                        
                        final += "( "+f_q[buf_tables[i]]+ " ) b"+i;
                        if(i+1!=buf_tables.length){
                            final += ", ";
                        }else{
                            final += " ";
                        } 
                        console.log(final);
                    }
                }
                //final += " WHERE b0.person_id = b1.person_id and b1.person_id = b2.person_id and b2.person_id = b3.person_id and b3.person_id = b1.person_id";
                //각 항목에 내용 여부 체크
                var f_where =[];
                for(i=0;i<4;i++){
                    if(f_q[tables[i]].length!=0){
                        f_where.push(i);
                    }
                }
                var where_f = "WHERE "
                for(i=0;i<f_where.length-1;i++){
                    if(i+1!=f_where.length-1){
                        where_f += "b"+f_where[i]+".person_id = b"+f_where[i+1]+".person_id and ";
                    }else{
                        where_f += "b"+f_where[i]+".person_id = b"+f_where[i+1]+".person_id ";
                    }
                }
    
                final += where_f +"and b"+f_where[0]+".person_id = b"+f_where[f_where.length-1]+".person_id";

        }else{
            // exclusion
              
            for(i=0;i<tables.length;i++){
                //없는 각 테이블 버리기
                if(result[tables[i]].length!=0){
                    for(j=0;j<result[tables[i]].length;j++){
                        // console.log(i+", "+j)
                        final += result[tables[i]][j] + " UNION "
                    }
                }
            }
            final =  "SELECT c0.person_id FROM ( "+ final.substring(0, final.length-6)+" ) c0";
        }
        //result, f_q, final
        return final;
    }

    function make_query_execute(inclusion, exclusion, study_id, query_id){
        var query = "SELECT provider_id, count(*) as cnt FROM ( "+inclusion+" ) A LEFT JOIN ( "+ exclusion +" ) B ON A.person_id = B.person_id WHERE B.person_id IS NULL GROUP BY provider_id ORDER BY cnt DESC;" 
        lp("make_query_execute()", query);
        // console.log()
        // console.log(inclusion);
        // console.log()
        // console.log(exclusion);
       

        const pool1 = new sql.ConnectionPool(cdmDbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(query, (err, result) => {
                    var q_execute =[]; var q_cnt =0; // 1000개 이상일 경우 
                    if (result != null) {
                        let rows = result.recordset
                        console.log("query result: " + rows.length);
                        //결과값 0 체크
                        if(rows.length!=0){
                            for(i=0;i<rows.length;i++){
                                q_execute.push("(N'전북대학교', "+rows[i].provider_id+", "+rows[i].cnt+", "+query_id+", "+study_id+")");
                            }
                            //console.log("q_execute");
                            //console.log(q_execute)
                            //res.send(q_execute);
                            make_query_delete(q_execute,study_id,query_id);
                        }else{
                            res.send({result : 0});
                        } 
                        console.log(err);
                    }else{
                        //console.log(err);
                        res.send(err);
                    }

                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
        return query;
    }

    function make_query_delete(q_execute, study_id, query_id){
        lp("make_query_delete", "study_id: "+study_id+", query_id: "+query_id);
        var query = "DELETE FROM doctor_result WHERE study_id = "+ study_id + " and query_id = " + query_id;
        const pool1 = new sql.ConnectionPool(dbConfig, err => {
            pool1.request() // or: new sql.Request(pool1)
                .query(query, (err, result) => {
                   console.log(result);
                   make_queyr_insert(q_execute, study_id, query_id);
                })
        })
        pool1.on('error', err => {
            // ... error handler
            console.log("pool1: " + err);
        })
    }

    function make_queyr_insert(q_execute, study_id, query_id){
        lp("make_queyr_insert", "study_id: "+study_id+", query_id: "+query_id);
        //파라미타 리스트 값을 가지고 1000 개씩 끊어서 쿼리문 만들어주기, 

        var cnt_1000 =0;
        var query = "";
        var q_final ="";
        //console.log(q_execute);
        for(i=0;i<q_execute.length;i++){
            query +=  q_execute[i];
            //console.log(q_execute[i]);
            cnt_1000 += 1;
            if(cnt_1000+1==1000){
                q_final += "INSERT INTO doctor_result VALUES "+query+" ;";
                cnt_1000 =0;
                query ="";
            }else{
                if(i+1!=q_execute.length){
                    query += ", ";
                }
                
            }
        }
        q_final += "INSERT INTO doctor_result VALUES "+query+" ;";
        res.send(q_final);

        // const pool1 = new sql.ConnectionPool(dbConfig, err => {
        //     pool1.request() // or: new sql.Request(pool1)
        //         .query(query, (err, result) => {
        //             console.log(result);
        //             make_queyr_insert(q_execute, study_id, queyr_id);
        //         })
        // })
        // pool1.on('error', err => {
        //     // ... error handler
        //     console.log("pool1: " + err);
        // })
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




