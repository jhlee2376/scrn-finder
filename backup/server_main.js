var http = require('http');
var express = require('express');
const utf8 = require('utf8');

var sql = require('mssql');

    //설정
    var dbConfig = {
        user: 'sa',
        password: 'admin123!!',
        server: '210.117.182.65',
        port: '1401',
        database: 'SCRN_CLOUD',
    }

    //웹 서버를 생성
var app = express();
app.use(express.static('public'));

app.get('/query',function(request, res)
{
        new sql.ConnectionPool(dbConfig).connect().then(pool => {
            return pool.request().query("select * from query")
        }).then(result => {
            let rows = result.recordset
            res.setHeader('Access-Control-Allow-Origin', '*');
            res.status(200).json(rows);
            sql.close();
        }).catch(err => {
            res.status(500).send({ message: "${err}" })
            sql.close();
        });
       
});



// 웹 서버를 실행
http.createServer(app).listen(52273, function(req, res){
    console.log('Server Running at http://127.0.0.1:52273');

});