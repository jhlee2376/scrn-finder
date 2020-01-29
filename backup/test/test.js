var fs = require('fs');

var oldFilename = './processId.txt';  
var newFilename = './processIdOld.txt';

fs.chmodSync(oldFilename, 777);  
console.log('complete chmod.');  
fs.renameSync(oldFilename, newFilename);  
console.log('complete rename.');  
var isSymLink = fs.lstatSync(newFilename).isSymbolicLink();  
console.log('complete symbolic check.');  
var fs = require('fs');

var oldFilename = './processId.txt';  
var newFilename = './processIdOld.txt';

fs.chmod(oldFilename, 777, function (err) 
{  
    console.log('complete chmod.');
    fs.rename(oldFilename, newFilename, function (err) 
    {
        console.log('complete rename.');
        fs.lstat(newFilename, function (err, stats) 
        {
            var isSymLink = stats.isSymbolicLink();
            console.log('complete symbolic check.');
        });
    });
    console.log("1");
});


const sql = require('mssql');

var dbConfig = {
    user: 'sa',
    password: 'admin123!!',
    server: '210.117.182.65',
    port: '1401',
    database: 'SCRN_CLOUD',
}
const pool1 = new sql.ConnectionPool(dbConfig, err => {
    // ... error checks
    // Query
    pool1.request() // or: new sql.Request(pool1)
    .query('select * from query', (err, result) => {
        // ... error checks
        //let rows = result.recordset
        console.dir(result)
        const pool2 = new sql.ConnectionPool(dbConfig, err => {
            // ... error checks
         
            // Query
         
            pool2.request() // or: new sql.Request(pool1)
            .query('select * from criteria', (err, result) => {
                // ... error checks
                //let rows = result.recordset
                console.dir(result)
            
            
            })
         
        })
        pool2.on('error', err => {
            // ... error handler
        })
    
    })
})
pool1.on('error', err => {
    // ... error handler
})
 

 
