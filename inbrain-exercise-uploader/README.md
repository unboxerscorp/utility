# Inbrain Exercise Uploader

JSON 파일에서 문제 데이터를 읽어 PostgreSQL DB에 업로드하거나 카테고리 정의를 업데이트하는 도구

## 사용법

### 디렉토리 처리 (m, g 타입)
폴더 내 모든 JSON 파일을 처리합니다.
```bash
go run main.go -type=g data/folder -host=localhost -port=5432 -db=mydb
go run main.go -type=m data/folder -host=localhost -port=5432 -db=mydb
```

### 단일 파일 처리 (모든 타입)
특정 JSON 파일 하나를 처리합니다.
```bash
# 기출/문제집 단일 파일
go run main.go -type=g single_file.json -host=localhost -port=5432 -db=mydb
go run main.go -type=m single_file.json -host=localhost -port=5432 -db=mydb

# 카테고리 정의 파일
go run main.go -type=d category_definition.json -host=localhost -port=5432 -db=mydb
```

## 플래그

- `-type`: 처리 타입
  - `m`: 문제집
  - `g`: 기출문제  
  - `d`: 카테고리 정의
- `<path>`: JSON 파일 또는 폴더 경로 (argument)
- `-host`: DB 호스트 (기본값: localhost)
- `-port`: DB 포트 (기본값: 5432)
- `-db`: DB 이름 (기본값: postgres)
- `-sslmode`: SSL 모드 (기본값: disable)

## 참고

- DB 사용자: app_user (고정)
- DB 비밀번호: AWS Secrets Manager에서 자동 조회
- 시크릿: `base-inbrain/production/DB_PASSWORD`