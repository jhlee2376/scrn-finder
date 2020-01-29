var dbConfig = {
    user: 'sa',
    password: 'scrn123!!',
    // server: 'localhost',
    server: '192.1.170.205',
    port: 1433,
    database: 'SCRN_CLOUD',
    connectionTimeout: 3000000,
    requestTimeout: 3000000,
    pool:{
        idleTimeoutMillis :15000000,
        requestTimeout: 15000000
    }
}

var CdmDbConfig = {
    // user: 'sa',
    // password: 'scrn123!!',
    // server: 'localhost',
    user: 'bigmgr',
    password: 'bigmgrahdkr4n',
    server: '192.1.98.222',
    port: 1433,
    // database: 'BIGDATADB',
    database: 'BIGDATADB_BACK_20181120',
    connectionTimeout: 3000000,
    requestTimeout: 3000000,
    pool:{
        idleTimeoutMillis :15000000,
        requestTimeout: 15000000
    }
}

module.exports ={
    dbConfig, CdmDbConfig
}
