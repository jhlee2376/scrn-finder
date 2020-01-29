
const { Get_condition_list, Make_query, Execute_query,Update_result } = require('./modules/m_first/m1_get');


// import
let     http        = require('http');
let     express     = require('express');
let     cors        = require('cors'); 
let     bodyParser  = require('body-parser');
const   fs          = require('fs');
let     chalk       = require('chalk');
//웹 서버 생성
var app = express();
app.use(cors());
app.use(express.static('public'));
app.use(bodyParser.json());


//Api

//Execute
app.get('/execute/:study_id/:query_id',(req,res)=>{
    let study_id = req.params.study_id;
    let query_id = req.params.query_id;
    console.log({input:{study_id, query_id}});
 
    Start(study_id, query_id, res); 
});// Execute

//Wijmo
app.post('/load/wijmo/:study_id/:query_id',(req,res)=>{

})//wijmo


app.use((error,req,res,next)=>{
    res.json({app_message_ERROR: error.message});
});

const ip_back_end = '127.0.0.1'
const port = 52274
http.createServer(app).listen(port, (res,req)=>{
    console.log(`Server Running at http://${ip_back_end}:${port}`)
})

//함수
async function Start(study_id, query_id,res){

    console.log("=========================================================================")
    let send = ''
    
    //1. 조건가져오기
    //2. 가져온 조건가지고 각 쿼리 만들기
    //3. 프로토콜, SITE ,Patient 쿼리에 따라 만들어 수행하기 하고 
    //4. 결과값 넣기. 
    //4. 종료
    try{
        let result      = await Get_condition_list(study_id, query_id); 
            result      = await Make_query(result,study_id, query_id); 
            result      = await Execute_query(result, study_id, query_id); 
            result      = await Update_result(result, study_id, query_id, result.provider_id);


        res.send(result);
    }catch(err){
        console.log({start_ERROR: err});
        res.send({ERROR:{code:001, message:err}});
    }
    
}//start
