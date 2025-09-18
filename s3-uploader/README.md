# S3 업로더

로컬 폴더를 S3에 업로드하는 Go 스크립트. NFD를 NFC로 변환하여 업로드.

## 사용법

```bash
go run main.go <로컬폴더> <S3경로>
```

예시:
```bash
# Mac/Linux
./s3-uploader './공수 1강' 'base-inbrain-resource/lectures/'

# AWS 키 세팅 및 실행 (Mac/Linux)
AWS_ACCESS_KEY_ID=your_key AWS_SECRET_ACCESS_KEY=your_secret ./s3-uploader './공수 1강' 'base-inbrain-resource/lectures/'

# Windows
s3-uploader-windows.exe "공수 1강" "base-inbrain-resource/lectures/"

# AWS 키 세팅 및 실행 (Windows)
set AWS_ACCESS_KEY_ID=your_key && set AWS_SECRET_ACCESS_KEY=your_secret && s3-uploader-windows.exe "공수 1강" "base-inbrain-resource/lectures/"
```

## 요구사항

- Go 1.21 이상 (빌드시)
- AWS 환경변수 설정 필요

## 다운로드 및 빌드

### 다운로드
- [Mac Intel](https://github.com/unboxerscorp/utility/releases/download/executable/s3-uploader-mac-intel)
- [Mac Apple Silicon](https://github.com/unboxerscorp/utility/releases/download/executable/s3-uploader-mac-apple)
- [Windows](https://github.com/unboxerscorp/utility/releases/download/executable/s3-uploader-windows.exe)

### 빌드
```bash
# 빌드 폴더 생성
mkdir -p build

# Mac Intel
CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o build/s3-uploader-mac-intel main.go

# Mac Apple Silicon
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -o build/s3-uploader-mac-apple main.go

# Windows
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o build/s3-uploader-windows.exe main.go
```

## 주의사항

- 로컬 폴더 경로에 공백이 있으면 따옴표로 감쌀 것
- S3 경로는 bucket-name/prefix/ 형식으로 입력
- 같은 키로 파일이 이미 존재하면 덮어씀
- 네트워크 상태에 따라 업로드 시간이 오래 걸릴 수 있음