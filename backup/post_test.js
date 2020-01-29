
const { Pool, Client } = require('pg');

var http = require('http');
var express = require('express');

var CdmDbConfig =
  {
    user: 'postgres',
    host: '192.1.173.230',
    database: 'postgres',
    password: 'scrn123!!',
    port: 5432
  }

test();

async function test (){
          console.log("----3번시작")
          const client = new Client(CdmDbConfig)
          await client.connect()
          console.log('query start!!')
          result = await client.query('SELECT * FROM bigdatadb.person;');
          console.log(result);
          await client.end();
}

