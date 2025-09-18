# Inbrain Exercise Uploader

JSON 파일에서 문제 데이터를 읽어 PostgreSQL DB에 업로드하는 도구

## 사용법

```bash
go run main.go -type=g -folder=data/folder -host=localhost -port=5432 -db=mydb
```

## 플래그

- `-type`: m(문제집) 또는 g(기출)
- `-folder`: JSON 파일 폴더 경로
- `-host`: DB 호스트 (기본값: localhost)
- `-port`: DB 포트 (기본값: 5432)
- `-db`: DB 이름 (기본값: postgres)
- `-sslmode`: SSL 모드 (기본값: disable)

## 참고

- DB 사용자: app_user (고정)
- DB 비밀번호: AWS Secrets Manager에서 자동 조회
- 시크릿: `base-inbrain/production/DB_PASSWORD`