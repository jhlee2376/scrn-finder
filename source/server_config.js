var dbConfig = {
    user: '',           // database id
    password: '',       // database password
    server: '',         // database server ip
    port: '',           // database port 
    database: '',       // database schema name
    connectionTimeout: 3000000,
    requestTimeout: 3000000,
    pool:{
        idleTimeoutMillis :15000000,
        requestTimeout: 15000000
    }
}

var CdmDbConfig = {
    user: '',       // CDM DB id
    password: '',   // CDM DB password
    server: '',     // CDM DB ip
    port: '',       // CDM DB port
    database: '0',
    connectionTimeout: 3000000,
    requestTimeout: 3000000,
    pool:{
        idleTimeoutMillis :15000000,
        requestTimeout: 15000000
    }
}

module.exports ={
    dbConfig,CdmDbConfig
}

