// SCRN 데이터베이스 환경설정
var dbConfig = {
    user: '',       // id
    password: '',   // password
    server: '',     // db server ip address
    port: '',       // db server port
    database: '',   // db schema
    connectionTimeout: 3000000,
    requestTimeout: 3000000,
    pool:{
        idleTimeoutMillis :15000000,
        requestTimeout: 15000000
    }
}

// CDM 데이터베이스 환경설정
var CdmDbConfig = {
    user: '',       // cdm server id
    password: '',   // cdm server password
    server: '',     // cdm server ip
    port: '',       // cdm server port
    database: '',   // cdm server schema
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
