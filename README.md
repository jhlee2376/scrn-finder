# scrn-finder v1.0.0
scrn-finder 은 scrn 플랫폼에서 검색하고픈 조건을 CDM 데이터베이스에서 해당 환자를 조회하는 scrn플랫폼의 back-end 서버이다.

## 주요 기능
+ scrn 플랫폼 조건을 기반으로 CDM 데이터베이스에서 환자검색
+ 검색된 환자에 대한 일반 통계자료 분석

## 사전설치사항
+ [Node v8.x or v10.x and npm v6.x or greater](https://nodejs.org/en/download/)

## 설치 및 실행

### 1. repo 복제하기
scrn-finder를 복제할 폴더에 복사:
```
git clone https://github.com/jbcp/scrn-finder.git
```

### 2. Node 모듈 패키지 설치, DB 설정
실행에 도움이 되는 패키지를 설치:
```
npm i 
```

DB정보 설정:
```
./setting/server_config.js 파일 열기
```

scrn DB 정보입력
```
user: '',       // id
password: '',   // password
server: '',     // db server ip address
port: '',       // db server port
database: '',   // db schema
```
CDM DB 정보 입력
```
user: '',       // cdm server id
password: '',   // cdm server password
server: '',     // cdm server ip
port: '',       // cdm server port
database: '',   // cdm server schema
```

### 3. scrn-finder 실행
```
node app.js
```

## Contact to developer(s)
SANGUN JEONG - swjeong@jbcp.kr
