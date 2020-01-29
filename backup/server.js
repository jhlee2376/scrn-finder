// 모듈을 추출
var dao = require('./serverSource/dao');


var http = require('http');
var express = require('express');
var cors = require('cors');
const utf8 = require('utf8');
var bodyParser = require('body-parser');

var fs = require('fs');
var obj;
fs.readFile('files/test.json', function (err, data) {
    obj = JSON.parse(data);
})
console.log('out')


var con = 0;



var sql = require('mssql');
//설정
var dbConfig = {
    user: 'sa',
    password: 'scrn123!!',
    server: 'localhost',
    port: '1433',
    database: 'SCRN_CLOUD',
}

var cdmDbConfig = {
    user: 'sa',
    password: 'scrn123!!',
    server: 'localhost',
    port: '1433',
    database: 'BIGDATADB',
}

var re = [];

// 변수 선언
var items = [{
    name: '우유',
    price: '2000'
}, {
    name: '홍차',
    price: '3000'
}];

var cdm = { "table": [{ "tableName": "person", "fieldSet": ["person_id", "gender_concept_id", "year_of_birth", "month_of_birth", "day_of_birth", "time_of_birth", "race_concept_id", "ethnicity_concept_);id", "location_id", "provider_id", "care_site_id", "person_source_value", "gender_source_value", "gender_source_concept_id", "race_source_value", "race_source_concept_id", "ethnicity_source_value", "ethnicity_source_concept_id"] }, { "tableName": "specimen", "fieldSet": ["specimen_id", "person_id", "specimen_concept_id", "specimen_type_concept_id", "specimen_date", "specimen_time", "quantity", "unit_concept_id", "anatomic_site_concept_id", "disease_status_concept_id", "specimen_source_id", "specimen_source_value", "unit_source_value", "anatomic_site_source_value", "disease_status_source_value"] }, { "tableName": "visit_occurence", "fieldSet": ["visit_concept_id", "visit_start_date", "visit_start_time", "visit_end_date", "visit_end_time", "visit_type_concept_id", "provider_id", "care_site_id", "visit_source_value", "visit_source_concept_id"] }, { "tableName": "procedure_occurrence", "fieldSet": ["procedure_concept_id", "procedure_date", "procedure_type_concept_id", "modifier_concept_id", "quantity", "provider_id", "visit_occurrence_id", "procedure_source_value", "procedure_source_concept_id", "qualifier_source_value"] }, { "tableName": "condition_occurrence", "fieldSet": ["person_id", "condition_concept_id", "condition_start_date", "condition_end_date", "condition_type_concept_id", "stop_reason", "provider_id", "visit_occurrence_id", "condition_source_value", "condition_source_concept_id"] }, { "tableName": "drug_exposure", "fieldSet": ["drug_concept_id", "quantity", "drug_exposure_start_date", "drug_exposure_end_date"] }] }
cdm = cdm.table;

var condition = ["<", "<=", "=", ">", ">="];

//웹 서버를 생성
var app = express();
app.use(cors());
app.use(express.static('public'));
app.use(bodyParser.json());

app.get("/test", (req, res) => {
    lp("/test", obj.length);
    res.send(obj);
});


// app.use(bodyParser.urlencoded({extended : true}));

//app.use(express.bodyParser());
//app.use(app.router); //라우터 안써도 됨.이제 

//라우트합니다.
app.all('/data.html', function (request, response) {
    var output = 'html로 뿌리기 ';

    response.send(output);

});

app.all('/data.json', function (request, response) {
    response.send(items);
});
app.all('/data.xml', function (request, response) { });

//동적라우터
app.all('/parameter/:id', function (request, response) {
    var id = request.param('id');

    response.send('<h1>' + id + '</h1>');
});

app.get('/parameter/', function (request, response) {

    response.send(item);
});

app.get('/products', function (request, response) {
    response.send(items);
});

app.get('/products/:id', function (request, response) {
    var id = Number(request.param('id'));
    response.send(items[id]);
});

var list;
app.get('/query/:queryId', function (request, res) {
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
        return pool.request().query("select * from query where study_id = " + id);
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

    for (var i = 0; i < re.originSet.length; i++) {
        deleteList += "( query_id = " + re.originSet[i].query_id + " and study_id = " + re.info.study_id + ")";
        if (i + 1 != re.originSet.length) {
            deleteList += " or ";

        }
    }
    //console.log("1:"+deleteList)
    var query = "DELETE from query WHERE " + deleteList + "; DELETE from criteria WHERE " + deleteList + " ; DELETE FROM criteria_detail WHERE " + deleteList + " ; ";
    const pool1 = new sql.ConnectionPool(dbConfig, err => {
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
app.get('/criteria/:queryId/:study_id', function (request, res) {
    printLog("get /criteria/:study_id/:query_id");
    var criteriaSet = [];
    var study_id = Number(request.param('study_id'));
    var query_id = Number(request.param('queryId'));


    const pool1 = new sql.ConnectionPool(dbConfig, err => {
        // ... error checks
        // Query
        var q = "select criteria_id, criteria_title, criteria_state from criteria where query_id = " + query_id + " and study_id = " + study_id;
        console.log(q)
        pool1.request() // or: new sql.Request(pool1)
            .query(q, (err, result) => {
                // ... error checks
                //let rows = result.recordset

                let rows = result.recordset


                for (var i = 0; i < rows.length; i++) {
                    criteriaSet.push(set.Criteria(rows[i].criteria_id, rows[i].criteria_title, rows[i].criteria_state));
                    //console.log(rows[i]);
                }



                const pool2 = new sql.ConnectionPool(dbConfig, err => {
                    // ... error checks

                    // Query

                    pool2.request() // or: new sql.Request(pool1)
                        .query('select * from criteria_detail where query_id = ' + query_id + " and study_id = " + study_id, (err, result1) => {
                            // ... error checks
                            //let rows = result.recordset
                            //console.dir(result)
                            let rows1 = result1.recordset
                            //console.log(rows1);

                            console.log("rows len:" + rows1.length);
                            //console.log("criteriaSet len:"+criteriaSet.length);
                            for (var i = 0; i < rows1.length; i++) {
                                for (var j = 0; j < criteriaSet.length; j++) {
                                    if (criteriaSet[j].criteria_id == rows1[i].criteria_id && criteriaSet[j].criteria_state == rows1[i].criteria_state) {
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
app.get('/criteria/', function (request, re) {

});

app.post('/updateCriteriaSet', function (req, res) {
    re = req.body;
    console.log(re);
    //res.send("ok");
    var deleteList = "";
    for (var i = 0; i < re.criteriaSet.length; i++) {
        deleteList += "( criteria_id = " + re.criteriaSet[i].criteria_id + " and criteria_state = " + re.criteriaSet[i].criteria_state + " and query_id = " + re.queryIdx + " and study_id = " + re.studyId + ")";
        if (i + 1 != re.criteriaSet.length) {
            deleteList += " or ";
        }
    }
    //console.log("1:"+deleteList)
    var query
    if (deleteList != "") {
        query = "DELETE from criteria WHERE " + deleteList;
    } else {
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
                    insertList += "(" + re.newCriteriaSet[i].criteria_id + ", N'" + re.newCriteriaSet[i].criteria_title + "', " + re.queryIdx + ", " + re.newCriteriaSet[i].criteria_state + ", " + re.studyId + ")";
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
        console.log("pool1: " + err);
    })


    const pool3 = new sql.ConnectionPool(dbConfig, err => {
        // ... error checks

        // Query
        var deleteList1 = "";
        var insertList1 = "";
        var sub;

        //console.log(re.criteriaSet.length);
        for (var i = 0; i < re.criteriaSet.length; i++) {
            sub = re.criteriaSet[i];
            for (var j = 0; j < sub.criteria_detail_set.length; j++) {
                deleteList1 += "(criteria_detail_id = " + sub.criteria_detail_set[j].criteria_detail_id + " and criteria_id = " + sub.criteria_id + " and query_id = " + re.queryIdx + " and criteria_state = " + sub.criteria_state + " and study_id = " + re.studyId + ")";
                if (j + 1 != sub.criteria_detail_set.length) {
                    deleteList1 += " or ";
                }
            }
            //console.log("i: "+i);
            if (i + 1 != re.criteriaSet.length) {
                deleteList1 += " or ";
            }
        }

        for (var i = 0; i < re.newCriteriaSet.length; i++) {
            sub = re.newCriteriaSet[i];
            for (var j = 0; j < sub.criteria_detail_set.length; j++) {
                insertList1 += "(" + sub.criteria_detail_set[j].criteria_detail_id + ", " + sub.criteria_detail_set[j].criteria_detail_table + ", " + sub.criteria_detail_set[j].criteria_detail_field + ", " + sub.criteria_detail_set[j].criteria_detail_condition + ", N'" + sub.criteria_detail_set[j].criteria_detail_value + "', " + sub.criteria_id + ", " + re.queryIdx + ", " + sub.criteria_state + ", " + re.studyId + ")"
                if (j + 1 != sub.criteria_detail_set.length) {
                    insertList1 += " , ";
                }
            }
            //console.log("i: "+i);
            if (i + 1 != re.newCriteriaSet.length) {
                insertList1 += " , ";
            }
        }



        var query3;
        if (deleteList1.length != 0) {
            query3 = "DELETE from criteria_detail WHERE " + deleteList1;
        } else {
            query3 = "SELECT TOP(1) * FROM criteria_detail;";
        }

        var query4 = "INSERT INTO criteria_detail VALUES " + insertList1;
        //console.log("pool3: "+query3);
        console.log("pool4: " + query4);

        pool3.request() // or: new sql.Request(pool1)
            .query(query3, (err, result) => {
                // ... error checks
                //console.dir(result)
                console.log("pool3 in: " + err);
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
                            console.log("pool4: in " + err);
                            //pool4.response().send("ok");
                            res.send("ok");
                        })

                })
                pool4.on('error', err => {
                    // ... error handler
                    console.log("pool4_err : " + err);
                })
            })

    })
    pool3.on('error', err => {
        // ... error handler
        console.log("pool3_err : " + err);
    })

    //query = "INSERT * from criteria VALUES " +insertList;

    //executeQuery (res, sql);



});


app.post('/save-querySet-toDB', function (req, res) {
    var data = req.body;

    printLog("/save-querySet-toDB", con);
    printLog(data, con);
    var study_id = data.study_id;
    var data = data.querySet;
    var insertList =
        "(" + data.query_id + ", N'" + data.query_title + "', N'" + data.query_create_date + "', N'" + data.query_modify_date + "', N'" + data.query_exe_date + "', " + data.query_status + ", N'" + data.query_creator + "', N'" + data.query_last_editor + "', " + study_id + ")";

    var query = "INSERT INTO query  VALUES " + insertList;


    const pool1 = new sql.ConnectionPool(dbConfig, err => {
        // ... error checks

        // Query
        // console.log(query);
        pool1.request() // or: new sql.Request(pool1)
            .query(query, (err, result) => {
                // ... error checks
                //console.dir(result)
                console.log("save-querySet-toDB pool err: " + err + insertList);

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
    printLog("/excute:", con);

    re = req.body;
    //printLog(re,con);

    // scrn사이트에서 받은 info, querySet
    // 쿼리 생성
    // 의사별 환자 수 저장, 환자 ip값 저장, 


    //cIdx 진단 기준 
    var cIdx = -1, nIdx = -1, cnt = 0, ncnt = 0
    var includelist = "", notIncludelst = "";
    var detailSet = [];
    var q1 = "", q2 = "", finalquery = "";
    var q1w = "", q2w = "";
    for (var i = 0; i < re.querySet.length; i++) {
        //선정조건
        if (re.querySet[i].criteria_state == 1) {
            for (var j = 0; j < re.querySet[i].criteria_detail_set.length; j++) {
                detailSet = re.querySet[i].criteria_detail_set[j];
                // 환자 테이블 유무 검사.
                if (detailSet.criteria_detail_table != 0) {
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
    q1 = "SELECT DISTINCT a" + cIdx + ".provider_id, a" + cIdx + ".person_id " +
        " FROM " + includelist.slice(0, includelist.length - 2) +
        " WHERE " + q1w.slice(0, q1w.length - 4);

    q2 = notIncludelst.slice(0, notIncludelst.length - 6)

    finalquery = "SELECT a.provider_id, count(a.person_id) as cnt FROM ( " + q1 + " ) a LEFT JOIN ( " + q2 + " ) b ON a.person_id = b.person_id WHERE b.person_id IS NULL GROUP BY a.provider_id";
    //console.log(q1);
    //console.log(q2);

    query_select_personList = "SELECT a.provider_id, a.person_id  FROM ( " + q1 + " ) a LEFT JOIN ( " + q2 + " ) b ON a.person_id = b.person_id WHERE b.person_id IS NULL";

    lp("finalquery", finalquery);
    lp("query_select_personList", query_select_personList);


    const pool1 = new sql.ConnectionPool(cdmDbConfig, err => {
        // ... error checks

        // Query
        // console.log(query);
        pool1.request() // or: new sql.Request(pool1)
            .query(finalquery, (err, result) => {
                // ... error checks
                //console.dir(result)
                if (result != null) {
                    let rows = result.recordset
                    console.log("query result: " + rows.length);
                    //console.log("save-querySet-toDB pool err: "+err+ ", "+rows);

                    // res.send("ok");

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

                    const pool2 = new sql.ConnectionPool(dbConfig, err => {
                        // ... error checks

                        // Query
                        // console.log(query);
                        pool2.request() // or: new sql.Request(pool1)
                            .query(query_insert_excute_result, (err, result) => {
                                // ... error checks
                                //console.dir(result)
                                //res.send("select Ok");
                                console.log("excute_pool1: " + err);
                                res.send("ok");
                            })

                    })

                    pool2.on('error', err => {
                        // ... error handler
                        //console.log("pool1: "+err);
                    })
                }



            })

    })

    pool1.on('error', err => {
        // ... error handler
        console.log("pool1: " + err);
    })

    const pool11 = new sql.ConnectionPool(cdmDbConfig, err => {
        // ... error checks

        // Query
        // console.log(query);
        pool11.request() // or: new sql.Request(pool1)
            .query(query_select_personList, (err, result) => {
                // ... error checks
                //console.dir(result)
                if (result != null) {
                    let rows = result.recordset
                    console.log("query_select_personList: " + rows.length);
                    //console.log("save-querySet-toDB pool err: "+err+ ", "+rows);

                    // res.send("ok");

                    var outInsert = "";
                    var outDelete = "";

                    // re -> { info: { study_id: 1, query_id: 0 },
                    for (var i = 0; i < rows.length; i++) {
                        outInsert += "( " + rows[i].provider_id + ", " + rows[i].person_id + ", " + re.info.study_id + ", " + re.info.query_id + ")";
                        if (i + 1 != rows.length) {
                            outInsert += ", ";
                        }
                    }
                    outDelete += "DELETE FROM doctor_result_person_list WHERE query_id = " + re.info.query_id + " and study_id = " + re.info.study_id + "; ";


                    //console.log("/excute_pool1: " +outInsert);
                    var query_insert_excute_result = outDelete + "INSERT INTO doctor_result_person_list VALUES " + outInsert;
                    lp("query_insert_excute_result", query_insert_excute_result);

                    const pool22 = new sql.ConnectionPool(dbConfig, err => {
                        // ... error checks

                        // Query
                        // console.log(query);
                        pool22.request() // or: new sql.Request(pool1)
                            .query(query_insert_excute_result, (err, result) => {
                                // ... error checks
                                //console.dir(result)
                                //res.send("select Ok");
                                console.log("excute_pool11: " + err);
                                res.send("execute OK");
                            })

                    })

                    pool22.on('error', err => {
                        // ... error handler
                        //console.log("pool1: "+err);
                    })
                }



            })

    })

    pool11.on('error', err => {
        // ... error handler
        console.log("pool1: " + err);
    })





});

app.post('/excute-patient', function (req, res) {
    printLog("/excute-patient:", con);
    re = req.body;
    printLog(re, con);

    var cIdx = -1, nIdx = -1, cnt = 0, ncnt = 0
    var includelist = "", notIncludelst = "";
    var detailSet = [];
    var q1 = "", q2 = "", finalquery = "";
    var q1w = "", q2w = "";
    for (var i = 0; i < re.querySet.length; i++) {
        //선정조건
        if (re.querySet[i].criteria_state == 1) {
            for (var j = 0; j < re.querySet[i].criteria_detail_set.length; j++) {
                detailSet = re.querySet[i].criteria_detail_set[j];
                if (detailSet.criteria_detail_table != 0) {
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
    q1 = "SELECT DISTINCT a" + cIdx + ".provider_id, a" + cIdx + ".person_id " +
        " FROM " + includelist.slice(0, includelist.length - 2) +
        " WHERE " + q1w.slice(0, q1w.length - 4);

    q2 = notIncludelst.slice(0, notIncludelst.length - 6)
    lp('q1', q1);
    lp('q2', q2);

    finalquery = "SELECT DISTINCT a.person_id FROM ( " + q1 + " ) a LEFT JOIN ( " + q2 + " ) b ON a.person_id = b.person_id WHERE b.person_id IS NULL";
    lp("finalquery_1", finalquery);

    finalquery = "SELECT person_id, year_of_birth, gender_concept_id FROM person a where person_id IN (" + finalquery + ")";
    //console.log(q1);
    //console.log(q2);
    lp("excute-patient", finalquery);

    const pool1 = new sql.ConnectionPool(cdmDbConfig, err => {
        // ... error checks

        // Query
        // console.log(query);
        pool1.request() // or: new sql.Request(pool1)
            .query(finalquery, (err, result) => {
                // ... error checks
                //console.dir(result)
                let rows = result.recordset
                console.log("query result: " + rows.length);
                //console.log("save-querySet-toDB pool err: "+err+ ", "+rows);

                // res.send("ok");

                var outInsert = "";
                var outDelete = "";

                // re -> { info: { study_id: 1, query_id: 0 },
                // person_id, year_of_birth, gender_concept_id
                for (var i = 0; i < rows.length; i++) {
                    outInsert += "(" + re.info.study_id + ", " + rows[i].person_id + ", " + rows[i].year_of_birth + ", N'" + rows[i].gender_concept_id + "', N'1', " + re.info.query_id + ", 10)"; //의사번호가 없음우선 10
                    if (i + 1 != rows.length) {
                        outInsert += ", ";
                    }
                }
                outDelete += "DELETE FROM patient_result WHERE query_id = " + re.info.query_id + " and study_id = " + re.info.study_id + "; ";

                //console.log("/excute_pool1: " +outInsert);
                var query_insert_excute_result = outDelete + "INSERT INTO patient_result VALUES " + outInsert;
                console.log("/excute_pool1: " + query_insert_excute_result);

                const pool2 = new sql.ConnectionPool(dbConfig, err => {
                    // ... error checks

                    // Query
                    // console.log(query);
                    pool2.request() // or: new sql.Request(pool1)
                        .query(query_insert_excute_result, (err, result) => {
                            // ... error checks
                            //console.dir(result)
                            //res.send("select Ok");
                            console.log("excute_pool1: " + err);

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
        console.log("pool1: " + err);
    })





});
app.get('/doctor-result-person-list/:protocol_id', function (req, res) {
    lp("get, /doctor-result-person-list", "start");
    var protocol_id = Number(req.param('protocol_id'));
   

    //스터디, 쿼리 해당 환자 뽑기. 
    var query = "SELECT DISTINCT person_id FROM protocol_result WHERE protocol_id = " + protocol_id + ";";
    lp("queyr", query)
    const pool1 = new sql.ConnectionPool(dbConfig, err => {
        pool1.request() // or: new sql.Request(pool1)
            .query(query, (err, result) => {
                // ... error checks
                //console.dir(result)
                if (result != null) {
                    let rows = result.recordset
                    console.log("result: " + rows.length);
                    //res.send("ok");
                    // res.send('ok');
                    var personList = person_set_by_rows_string(rows);
                    lp("personList", personList);

                    var pool2_query = "SELECT  person_id, gender_concept_id, year_of_birth FROM person where person_id in ( " + personList + " )";
                    lp("pool2_query", pool2_query)
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
app.post("/execute",(req,res)=>{
    re = res.bodyParser;
    console.log(re);
    res.send(re.length);

})

app.use((error, req, res, next) => {
    console.log('error: ' + error.message);
    res.json({ message: error.message });
})

// 웹 서버를 실행
http.createServer(app).listen(52273, function (req, res) {
    console.log('Server Running at http://127.0.0.1:52273');
});

function makeCriteria(criteria_id, criteria_title) {
    this.id = criteria_id;
    this.title = criteria_title;
    this.criteriaDetailSet = [];
}

function makeQuery(id, title, createDate, modifyDate, exeDate, status, queryCreator, lastEditor) {
    this.id = id;
    this.title = title;
    this.createDate = createDate;
    this.modifyDate = modifyDate;
    this.exeDate = exeDate;
    this.status = status;
    this.queryCreator = queryCreator;
    this.lastEditor = lastEditor;
}

function add(arr, value) {
    arr.push(value);
}


function printLog(mes, con) {
    if (con == 1) {
        return;
    }
    console.log(mes);
}

function personList(date, id, type, name) {
    var obj = {
        date: date,
        id: id,
        type: type,
        name: name
    }
    return obj;
}

function add_personList(rows) {
    var obj = [];
    for (i = 0; i < rows.length; i++) {
        obj.push(personList(new Date(rows[i].date).format("yyyy-MM-dd"), rows[i].id, rows[i].type, rows[i].name));
    }
    return obj;
}




// query
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


function logPrint(mes) {
    console.log();
    console.log();
    console.log(mes);
    console.log();
}

function lp(title, mes) {
    console.log();
    console.log("---" + title + "---");
    console.log(mes);
    console.log();
}

function person_set_by_rows_string(rows) {
    var obj = '';

    for (i = 0; i < rows.length; i++) {
        obj += rows[i].person_id;
        if (i + 1 != rows.length) {
            obj += ", ";
        }
    }
    return obj;
}

function person_set_by_rows(rows) {
    var list = [];
    var now_year = new Date().format('yyyy');
    for (i = 0; i < rows.length; i++) {
        list.push(make_person_set(i, rows[i].gender_concept_id, (now_year - rows[i].year_of_birth) + 1));
    }

    return list;
}

function make_person_set(person_id, gender, year) {
    var gen = '';
    if (gender == 8507) {
        gen = "Male";
    } else {
        gen = "Female";
    }

    var obj = {
        id: person_id,
        gender: gen,
        age: year
    }
    return obj;
}


