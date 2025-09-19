# inbrain-session-creator

S3 콘텐츠를 파싱해 데이터베이스에 학습 세션을 생성하는 Go 프로그램

## 사용법

```bash
go run main.go -s3-prefix="공통수학2 Day1" -db-user="user" -db-password="pass"
```

## 필수 옵션

- `-s3-prefix`: S3 폴더명
- `-db-user`: 데이터베이스 사용자명  
- `-db-password`: 데이터베이스 비밀번호

## 선택 옵션

- `-session`: 세션명 (기본: s3-prefix 값)
- `-db-host`: DB 호스트 (기본: localhost)
- `-db-port`: DB 포트 (기본: 5432)
- `-s3-bucket`: S3 버킷 (기본: base-inbrain-resource)
- `-force-replace-video`: 기존 비디오 강제 교체

## 의존성

- ffmpeg, ffprobe 설치 필요
- PostgreSQL 연결 필요
- AWS 자격증명 설정 필요