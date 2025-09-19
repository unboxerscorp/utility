package main

import (
	"context"
	"crypto/md5" //nolint:gosec
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

const (
	// CloudFront 설정
	cloudfrontBaseURL = "https://media.basemath.co.kr"

	// 고정값
	lecturesCategoryID = 526
	sessionSequence    = 0
	studentID          = 21
)

type Parser struct {
	db                *sql.DB
	s3Client          *s3.Client
	ctx               context.Context
	bucketName        string
	region            string
	forceReplaceVideo bool
}

type SessionInfo struct {
	Name     string
	ID       int64
	Sequence int
}

type ModuleInfo struct {
	Name     string
	Type     string
	ID       int64
	Sequence int
}

type SectionInfo struct {
	Name     string
	ID       int64
	Sequence int
}

func main() {
	// 명령줄 인자 파싱
	var sessionName string
	var s3Prefix string
	var dbHost string
	var dbPort int
	var dbUser string
	var dbPassword string
	var dbName string
	var dbSSLMode string
	var s3Bucket string
	var s3Region string
	var forceReplaceVideo bool

	flag.StringVar(&sessionName, "session", "", "세션 이름 (예: '공통수학2 Day1')")
	flag.StringVar(&s3Prefix, "s3-prefix", "", "S3 폴더명 (예: '공통수학2 Day1')")
	flag.StringVar(&dbHost, "db-host", "localhost", "데이터베이스 호스트")
	flag.IntVar(&dbPort, "db-port", 5432, "데이터베이스 포트")
	flag.StringVar(&dbUser, "db-user", "postgres", "데이터베이스 사용자")
	flag.StringVar(&dbPassword, "db-password", "password", "데이터베이스 비밀번호")
	flag.StringVar(&dbName, "db-name", "postgres", "데이터베이스 이름")
	flag.StringVar(&dbSSLMode, "db-ssl", "disable", "SSL 모드 (disable, require, verify-ca, verify-full)")
	flag.StringVar(&s3Bucket, "s3-bucket", "base-inbrain-resource", "S3 버킷 이름")
	flag.StringVar(&s3Region, "s3-region", "ap-northeast-2", "S3 리전")
	flag.BoolVar(&forceReplaceVideo, "force-replace-video", false, "기존 비디오를 강제로 대체")
	flag.Parse()

	// 세션명이 비어있으면 s3Prefix를 그대로 사용
	if sessionName == "" && s3Prefix != "" {
		sessionName = s3Prefix
	}

	if s3Prefix == "" || studentID == 0 || dbUser == "" || dbPassword == "" || dbName == "" || s3Bucket == "" {
		fmt.Println("사용법: parse_s3_content [옵션들]")
		fmt.Println("필수 옵션:")
		fmt.Println("  -s3-prefix='S3 폴더명' (예: '공통수학2 Day1')")
		fmt.Println("  -db-user='사용자명'")
		fmt.Println("  -db-password='비밀번호'")
		fmt.Println("선택 옵션:")
		fmt.Println("  -session='세션명' (비어있으면 s3-prefix에서 추출)")
		fmt.Println("  -db-host='호스트' (기본값: localhost)")
		fmt.Println("  -db-port=포트 (기본값: 5432)")
		fmt.Println("  -db-name='데이터베이스명' (기본값: postgres)")
		fmt.Println("  -db-ssl='SSL모드' (기본값: disable)")
		fmt.Println("  -s3-bucket='버킷명' (기본값: base-inbrain-resource)")
		fmt.Println("  -s3-region='리전' (기본값: ap-northeast-2)")
		fmt.Println("  -force-replace-video (기존 비디오 강제 대체)")
		os.Exit(1)
	}

	// Parser 초기화
	parser, err := NewParser(dbHost, dbPort, dbUser, dbPassword, dbName, dbSSLMode, s3Bucket, s3Region, forceReplaceVideo)
	if err != nil {
		log.Fatal("Parser 초기화 실패:", err)
	}
	defer parser.Close()

	// 사전 테스트
	if err := parser.RunPreTests(sessionName, s3Prefix); err != nil {
		parser.Close()
		log.Fatal("사전 테스트 실패:", err)
	}

	// 메인 처리
	if err := parser.ProcessSession(sessionName, s3Prefix, studentID, sessionSequence); err != nil {
		log.Fatal("세션 처리 실패:", err)
	}

	log.Println("✅ S3 콘텐츠 파싱 완료!")
}

func NewParser(dbHost string, dbPort int, dbUser, dbPassword, dbName, dbSSLMode, bucketName, region string, forceReplaceVideo bool) (*Parser, error) {
	// 데이터베이스 연결
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		dbHost, dbPort, dbUser, dbPassword, dbName, dbSSLMode)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("DB 연결 실패 -> %w", err)
	}

	// S3 클라이언트 초기화
	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, fmt.Errorf("AWS 설정 실패 -> %w", err)
	}

	return &Parser{
		db:                db,
		s3Client:          s3.NewFromConfig(awsCfg),
		ctx:               context.Background(),
		bucketName:        bucketName,
		region:            region,
		forceReplaceVideo: forceReplaceVideo,
	}, nil
}

func (p *Parser) Close() {
	if p.db != nil {
		_ = p.db.Close()
	}
}

func (p *Parser) RunPreTests(sessionName, s3Prefix string) error {
	fmt.Println("==============================================")
	fmt.Println("       S3 콘텐츠 파싱 스크립트 사전 테스트")
	fmt.Println("==============================================")
	fmt.Println()

	// 1. 도구 확인
	fmt.Println("=== 도구 설치 확인 ===")
	if err := checkCommand("ffmpeg", "-version"); err != nil {
		return fmt.Errorf("ffmpeg 설치되지 않음")
	}
	fmt.Println("✓ ffmpeg 설치됨")

	if err := checkCommand("ffprobe", "-version"); err != nil {
		return fmt.Errorf("ffprobe 설치되지 않음")
	}
	fmt.Println("✓ ffprobe 설치됨")
	fmt.Println()

	// 2. 데이터베이스 연결 확인
	fmt.Println("=== 데이터베이스 연결 확인 ===")
	if err := p.db.Ping(); err != nil {
		return fmt.Errorf("PostgreSQL 연결 실패 -> %w", err)
	}
	fmt.Printf("✓ PostgreSQL 연결 성공\n")
	fmt.Println()

	// 3. S3 연결 확인
	fmt.Println("=== AWS S3 연결 확인 ===")
	fmt.Printf("  - Bucket: %s\n", p.bucketName)
	fmt.Printf("  - Region: %s\n", p.region)

	_, err := p.s3Client.ListObjectsV2(p.ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(p.bucketName),
		Prefix:  aws.String("lectures/"),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return fmt.Errorf("S3 버킷 접근 실패 -> %w", err)
	}
	fmt.Println("✓ S3 버킷 접근 성공")
	fmt.Println()

	// 4. S3 구조 확인
	fmt.Println("=== S3 구조 확인 ===")
	fmt.Printf("세션: %s\n", sessionName)
	fmt.Printf("S3 Prefix: %s\n\n", s3Prefix)

	modules, err := p.GetModules(s3Prefix)
	if err != nil || len(modules) == 0 {
		return fmt.Errorf("모듈을 찾을 수 없습니다")
	}

	fmt.Println("발견된 모듈:")
	for _, module := range modules {
		fmt.Printf("  - %s\n", module)
	}
	fmt.Println()

	// 5. CloudFront 테스트
	fmt.Println("=== CloudFront 접근 테스트 ===")
	files, err := p.GetFilesInSection(s3Prefix, modules[0], "")
	if err != nil || len(files) == 0 {
		// 첫 번째 섹션 찾기
		sections, _ := p.GetSections(s3Prefix, modules[0])
		if len(sections) > 0 {
			files, _ = p.GetFilesInSection(s3Prefix, modules[0], sections[0])
		}
	}

	if len(files) > 0 {
		testURL := fmt.Sprintf("%s/%s", cloudfrontBaseURL, urlPathEncode(files[0]))
		fmt.Printf("테스트 URL: %s\n", testURL)

		duration, err := getVideoDuration(testURL)
		if err != nil {
			return fmt.Errorf("영상 길이 추출 실패 -> %w", err)
		}
		fmt.Printf("✓ 영상 길이 추출 성공: %d초\n", duration)
	}
	fmt.Println()

	fmt.Println("✅ 모든 사전 테스트를 통과했습니다!")
	fmt.Println()

	// 사용자 확인
	fmt.Print("실제 데이터베이스에 데이터를 생성하시겠습니까? [y/N]: ")
	var response string
	_, _ = fmt.Scanln(&response)
	if response != "y" && response != "Y" {
		return fmt.Errorf("작업이 취소되었습니다")
	}

	return nil
}

func (p *Parser) ProcessSession(sessionName, s3Prefix string, studentID, sessionSequence int) error {
	log.Printf("S3 콘텐츠 파싱 시작: %s (student_id: %d)", sessionName, studentID)

	// 1. 세션 생성
	sessionID, err := p.createSession(sessionName, studentID, sessionSequence)
	if err != nil {
		return fmt.Errorf("세션 생성 실패 -> %w", err)
	}
	log.Printf("세션 생성 완료: ID %d", sessionID)

	// 2. 모듈 처리
	modules, err := p.GetModules(s3Prefix)
	if err != nil {
		return fmt.Errorf("모듈 목록 조회 실패 -> %w", err)
	}

	for i, moduleName := range modules {
		moduleType := p.getModuleType(moduleName)
		moduleSeq := extractSequenceWithIndex(moduleName, i)
		log.Printf("모듈 처리 시작: %s (type: %s, seq: %d)", moduleName, moduleType, moduleSeq)
		moduleID, err := p.createModule(moduleName, sessionID, moduleSeq, moduleType)
		if err != nil {
			return fmt.Errorf("모듈 생성 실패 -> %w", err)
		}
		log.Printf("모듈 생성 완료: ID %d", moduleID)

		// 3. 섹션 처리
		sections, err := p.GetSections(s3Prefix, moduleName)
		if err != nil {
			return fmt.Errorf("섹션 목록 조회 실패 -> %w", err)
		}

		for j, sectionName := range sections {
			sectionID, err := p.createSectionWithIndex(sectionName, moduleID, j)
			if err != nil {
				return fmt.Errorf("섹션 생성 실패 -> %w", err)
			}
			log.Printf("섹션 생성 완료: ID %d", sectionID)

			// 4. 콘텐츠 처리
			log.Printf("콘텐츠 처리 시작: section_id %d", sectionID)
			if err := p.processSectionContents(s3Prefix, moduleName, sectionName, sectionID, studentID, moduleType); err != nil {
				return fmt.Errorf("콘텐츠 처리 실패 -> %w", err)
			}
			log.Printf("콘텐츠 처리 완료: section_id %d", sectionID)
		}
	}

	return nil
}

func (p *Parser) GetModules(s3Prefix string) ([]string, error) {
	prefix := fmt.Sprintf("lectures/%s/", s3Prefix)

	result, err := p.s3Client.ListObjectsV2(p.ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(p.bucketName),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		return nil, err
	}

	var modules []string
	for _, prefix := range result.CommonPrefixes {
		modulePath := *prefix.Prefix
		// lectures/s3Prefix/모듈명/ 형태에서 모듈명 추출
		parts := strings.Split(strings.TrimSuffix(modulePath, "/"), "/")
		if len(parts) >= 3 {
			moduleName := parts[2]
			// .으로 시작하는 폴더 제외
			if !strings.HasPrefix(moduleName, ".") {
				modules = append(modules, moduleName)
			}
		}
	}

	sort.Strings(modules)
	return modules, nil
}

func (p *Parser) GetSections(s3Prefix, moduleName string) ([]string, error) {
	prefix := fmt.Sprintf("lectures/%s/%s/", s3Prefix, moduleName)

	result, err := p.s3Client.ListObjectsV2(p.ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(p.bucketName),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		return nil, err
	}

	var sections []string
	for _, prefix := range result.CommonPrefixes {
		sectionPath := *prefix.Prefix
		// lectures/s3Prefix/모듈명/섹션명/ 형태에서 섹션명 추출
		parts := strings.Split(strings.TrimSuffix(sectionPath, "/"), "/")
		if len(parts) >= 4 {
			sectionName := parts[3]
			// .으로 시작하는 폴더 제외
			if !strings.HasPrefix(sectionName, ".") {
				sections = append(sections, sectionName)
			}
		}
	}

	sort.Strings(sections)
	return sections, nil
}

func (p *Parser) GetFilesInSection(s3Prefix, moduleName, sectionName string) ([]string, error) {
	prefix := fmt.Sprintf("lectures/%s/%s/%s/", s3Prefix, moduleName, sectionName)

	result, err := p.s3Client.ListObjectsV2(p.ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(p.bucketName),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, err
	}

	var files []string
	for _, obj := range result.Contents {
		key := *obj.Key
		filename := path.Base(key)

		// .으로 시작하는 파일과 썸네일 제외
		if !strings.HasPrefix(filename, ".") &&
			!strings.Contains(filename, "_thumbnail") &&
			(strings.HasSuffix(filename, ".mov") || strings.HasSuffix(filename, ".mp4")) {

			files = append(files, key)
		}
	}

	sort.Strings(files)
	return files, nil
}

// 데이터베이스 생성 함수들
func (p *Parser) createSession(name string, studentID, sequence int) (int64, error) {
	// 같은 타이틀의 세션이 이미 있는지 확인 (삭제되지 않은 것만)
	var existingID int64
	checkQuery := `SELECT id FROM learning_sessions WHERE student_id = $1 AND title = $2 AND deleted_at IS NULL`
	err := p.db.QueryRow(checkQuery, studentID, name).Scan(&existingID)

	// 이미 존재하는 경우 사용자에게 확인
	if err == nil {
		fmt.Printf("⚠️  동일한 타이틀의 세션이 이미 존재합니다 (ID: %d, Title: %s)\n", existingID, name)
		fmt.Print("기존 세션을 사용하시겠습니까? [y/N]: ")
		var response string
		_, _ = fmt.Scanln(&response)
		if response == "y" || response == "Y" {
			log.Printf("기존 세션 사용: ID %d (title: %s)", existingID, name)
			return existingID, nil
		} else {
			return 0, fmt.Errorf("작업이 취소되었습니다")
		}
	}

	// 새로운 세션 생성
	var id int64
	query := `
		INSERT INTO learning_sessions (student_id, status, sequence, title, date)
		VALUES ($1, 'registered', $2, $3, $4)
		RETURNING id`

	err = p.db.QueryRow(query, studentID, sequence, name, time.Now()).Scan(&id)
	if err != nil {
		return 0, err
	}

	log.Printf("새 세션 생성: ID %d (title: %s)", id, name)
	return id, err
}

func (p *Parser) createModule(name string, sessionID int64, sequence int, moduleType string) (int64, error) {
	// 모듈명에서 sequence 번호와 타입 제거 (예: "0_개념_점과 좌표" -> "점과 좌표")
	baseName := name

	// 먼저 앞의 숫자_ 부분 제거
	re := regexp.MustCompile(`^\d+_`)
	baseName = re.ReplaceAllString(baseName, "")

	// 그다음 타입 제거
	if strings.Contains(baseName, "개념_") {
		baseName = strings.Replace(baseName, "개념_", "", 1)
	} else if strings.Contains(baseName, "유형_") {
		baseName = strings.Replace(baseName, "유형_", "", 1)
	} else if strings.Contains(baseName, "시험_") {
		baseName = strings.Replace(baseName, "시험_", "", 1)
	}

	// 같은 title + sequence 조합의 모듈이 이미 있는지 확인 (삭제되지 않은 것만)
	var existingID int64
	checkQuery := `SELECT id FROM learning_modules WHERE session_id = $1 AND title = $2 AND sequence = $3 AND deleted_at IS NULL`
	err := p.db.QueryRow(checkQuery, sessionID, baseName, sequence).Scan(&existingID)

	// 이미 존재하는 경우 해당 ID 반환
	if err == nil {
		log.Printf("기존 모듈 사용: ID %d (title: %s, sequence: %d)", existingID, baseName, sequence)
		return existingID, nil
	}

	// 새로운 모듈 생성
	var id int64
	query := `
		INSERT INTO learning_modules (title, type, sequence, session_id)
		VALUES ($1, $2, $3, $4)
		RETURNING id`

	err = p.db.QueryRow(query, baseName, moduleType, sequence, sessionID).Scan(&id)
	if err != nil {
		return 0, err
	}

	log.Printf("새 모듈 생성: ID %d (title: %s, sequence: %d)", id, baseName, sequence)
	return id, err
}

func (p *Parser) createSectionWithIndex(name string, moduleID int64, index int) (int64, error) {
	// 섹션 sequence와 이름 파싱 (인덱스 fallback 사용)
	sequence := extractSequenceWithIndex(name, index)
	title := extractSectionTitle(name)

	// 같은 title + sequence 조합의 섹션이 이미 있는지 확인 (삭제되지 않은 것만)
	var existingID int64
	checkQuery := `SELECT id FROM learning_sections WHERE module_id = $1 AND title = $2 AND sequence = $3 AND deleted_at IS NULL`
	err := p.db.QueryRow(checkQuery, moduleID, title, sequence).Scan(&existingID)

	// 이미 존재하는 경우 해당 ID 반환
	if err == nil {
		log.Printf("기존 섹션 사용: ID %d (title: %s, sequence: %d)", existingID, title, sequence)
		return existingID, nil
	}

	// 새로운 섹션 생성
	var id int64
	query := `
		INSERT INTO learning_sections (title, sequence, module_id)
		VALUES ($1, $2, $3)
		RETURNING id`

	err = p.db.QueryRow(query, title, sequence, moduleID).Scan(&id)
	if err != nil {
		return 0, err
	}

	log.Printf("새 섹션 생성: ID %d (title: %s, sequence: %d)", id, title, sequence)
	return id, err
}

// video 생성 함수 - parse_excel과 동일한 로직
func (p *Parser) createVideoFromURL(title, videoURL, s3Path string) (int64, error) {
	// URL에서 MD5 해시 계산
	md5Hash, err := calculateURLMD5(videoURL)
	if err != nil {
		return 0, fmt.Errorf("MD5 계산 실패 -> %w", err)
	}

	// MD5 해시로 이미 존재하는 비디오 확인
	var existingID int64
	var existingUUID string
	checkQuery := `SELECT id, uuid FROM videos WHERE md5_hash = $1 AND deleted_at IS NULL`
	err = p.db.QueryRow(checkQuery, md5Hash).Scan(&existingID, &existingUUID)

	// 이미 존재하는 경우 처리
	if err == nil {
		log.Printf("동일한 비디오 이미 존재 (MD5: %s): ID %d, UUID %s", md5Hash, existingID, existingUUID)
		return existingID, nil
	}

	// 새로운 UUID 생성
	videoUUID := uuid.New().String()

	// 영상 길이 추출
	duration, _ := getVideoDuration(videoURL)

	// 썸네일 생성 및 업로드
	thumbnailS3Path := strings.TrimSuffix(s3Path, path.Ext(s3Path)) + "_thumbnail.png"
	err = p.createAndUploadThumbnail(videoURL, thumbnailS3Path)
	if err != nil {
		log.Printf("썸네일 생성 실패: %v", err)
	}

	thumbnailURL := fmt.Sprintf("%s/%s", cloudfrontBaseURL, urlPathEncode(thumbnailS3Path))

	// videos 테이블에 삽입
	var id int64
	query := `
		INSERT INTO videos (uuid, title, source_url, thumbnail_url, max_progress, md5_hash)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id`

	err = p.db.QueryRow(query, videoUUID, title, videoURL, thumbnailURL, duration, md5Hash).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("비디오 DB 삽입 실패 -> %w", err)
	}

	log.Printf("비디오 생성 완료: ID %d, UUID %s", id, videoUUID)
	return id, nil
}

func (p *Parser) createLectureWithVideoID(title string, videoID int64) (int64, error) {
	// 해당 video_id로 이미 존재하는 lecture가 있는지 확인
	var existingID int64
	checkQuery := `SELECT id FROM lectures WHERE lecture_video_id = $1`
	err := p.db.QueryRow(checkQuery, videoID).Scan(&existingID)

	// 이미 존재하는 경우 해당 ID 반환
	if err == nil {
		log.Printf("기존 강의 사용: ID %d (video_id: %d)", existingID, videoID)
		return existingID, nil
	}

	var id int64
	query := `
		INSERT INTO lectures (title, category_id, lecture_video_id)
		VALUES ($1, $2, $3)
		RETURNING id`

	err = p.db.QueryRow(query, title, lecturesCategoryID, videoID).Scan(&id)
	return id, err
}

func (p *Parser) updateExerciseSolutionWithVideoID(exerciseID int, videoID int64) error {
	// force 옵션이 없을 때만 기존 비디오 체크
	if !p.forceReplaceVideo {
		// 먼저 해당 exercise의 solution_video_id가 이미 설정되어 있는지 확인
		var existingVideoID sql.NullInt64
		checkQuery := `SELECT solution_video_id FROM exercises WHERE id = $1`
		err := p.db.QueryRow(checkQuery, exerciseID).Scan(&existingVideoID)

		// 레코드가 없는 경우
		if errors.Is(err, sql.ErrNoRows) {
			log.Printf("exercise_id %d를 찾을 수 없습니다", exerciseID)
			return fmt.Errorf("exercise not found: %d", exerciseID)
		}

		// 이미 비디오가 설정되어 있는 경우
		if existingVideoID.Valid && existingVideoID.Int64 > 0 {
			log.Printf("해설 영상 이미 존재: exercise_id %d (video_id: %d)", exerciseID, existingVideoID.Int64)
			return nil
		}
	}

	log.Printf("해설 영상 처리: exercise_id %d", exerciseID)

	// exercises 테이블 업데이트
	query := `UPDATE exercises SET solution_video_id = $1 WHERE id = $2`
	_, err := p.db.Exec(query, videoID, exerciseID)

	return err
}

func (p *Parser) processSectionContents(s3Prefix, moduleName, sectionName string, sectionID int64, studentID int, moduleType string) error {
	log.Printf("S3 파일 목록 조회 시작: %s/%s/%s", s3Prefix, moduleName, sectionName)
	files, err := p.GetFilesInSection(s3Prefix, moduleName, sectionName)
	if err != nil {
		return err
	}
	log.Printf("S3 파일 %d개 발견", len(files))

	// 기존 DB 콘텐츠 확인
	var existingCount int
	checkQuery := `SELECT COUNT(*) FROM learning_contents WHERE section_id = $1 AND user_id = $2 AND deleted_at IS NULL`
	err = p.db.QueryRow(checkQuery, sectionID, studentID).Scan(&existingCount)
	if err != nil {
		log.Printf("DB 콘텐츠 수 확인 실패: %v", err)
		existingCount = 0
	}

	log.Printf("섹션 콘텐츠 비교: section_id=%d, user_id=%d, S3파일=%d개, DB콘텐츠=%d개",
		sectionID, studentID, len(files), existingCount)

	// force 옵션이 아니고, S3 파일 수와 DB 콘텐츠 수가 같으면 스킵
	if !p.forceReplaceVideo && len(files) == existingCount && existingCount > 0 {
		log.Printf("S3 파일과 DB 콘텐츠 개수가 일치 (%d개), 처리 스킵", existingCount)
		return nil
	}

	if p.forceReplaceVideo {
		log.Printf("force-replace-video 옵션으로 기존 콘텐츠의 비디오만 재생성")
	} else if len(files) != existingCount {
		log.Printf("S3 파일(%d개)과 DB 콘텐츠(%d개) 개수 불일치, 누락된 콘텐츠 추가 진행", len(files), existingCount)
	}

	// 파일들을 contentSequence 기준으로 정렬
	sort.Slice(files, func(i, j int) bool {
		filenameI := path.Base(files[i])
		filenameJ := path.Base(files[j])
		seqI := extractSequence(filenameI)
		seqJ := extractSequence(filenameJ)
		return seqI < seqJ
	})

	exerciseCounter := 1
	lectureCounter := 0

	// 강의 파일 개수 확인
	lectureCount := 0
	for _, file := range files {
		filename := path.Base(file)
		if !isSolutionFile(filename) {
			lectureCount++
		}
	}

	// 파일 처리
	for i, s3Path := range files {
		filename := path.Base(s3Path)
		videoURL := fmt.Sprintf("%s/%s", cloudfrontBaseURL, urlPathEncode(s3Path))

		log.Printf("파일 처리 %d/%d: %s", i+1, len(files), filename)

		// 파일명에서 sequence 추출
		contentSequence := extractSequence(filename)

		if isSolutionFile(filename) {
			// 해설 영상 처리
			exerciseGroupID := extractExerciseGroupID(filename)
			exerciseID := extractExerciseID(filename)
			title := fmt.Sprintf("해설 영상 - %s", extractTitle(filename))
			var exampleTitle string
			if moduleType == "exam" {
				exampleTitle = extractSectionTitle(sectionName)
			} else {
				exampleTitle = generateExerciseTitle("example", exerciseCounter)
			}

			// 기존 콘텐츠 확인
			var existingContentID int64
			checkQuery := `SELECT id FROM learning_contents WHERE section_id = $1 AND sequence = $2 AND content_type = 'exercise' AND user_id = $3 AND deleted_at IS NULL`
			err := p.db.QueryRow(checkQuery, sectionID, contentSequence, studentID).Scan(&existingContentID)

			if err == nil {
				// 기존 콘텐츠가 있음
				if p.forceReplaceVideo {
					// force-replace-video 옵션: 기존 콘텐츠의 해설 비디오 교체
					log.Printf("기존 연습 콘텐츠의 해설 비디오 교체: content_id %d, exercise_id %d", existingContentID, exerciseID)

					// 새 비디오 생성
					var videoID int64
					videoID, err = p.createVideoFromURL(title, videoURL, s3Path)
					if err != nil {
						log.Printf("해설 비디오 생성 실패: %v", err)
						continue
					}

					// exercise의 solution_video_id 업데이트
					err = p.updateExerciseSolutionWithVideoID(exerciseID, videoID)
					if err != nil {
						log.Printf("해설 영상 업데이트 실패: %v", err)
						continue
					}

					log.Printf("해설 비디오 교체 완료: exercise_id %d, new_video_id %d", exerciseID, videoID)
				} else {
					// 일반 모드에서는 기존 콘텐츠가 있으면 스킵
					log.Printf("기존 연습 콘텐츠 존재 (sequence: %d), 스킵", contentSequence)
				}
				exerciseCounter++
				continue
			}

			// 새로운 콘텐츠 생성 (기존 콘텐츠가 없을 때)
			// video 생성
			videoID, err := p.createVideoFromURL(title, videoURL, s3Path)
			if err != nil {
				log.Printf("해설 비디오 생성 실패: %v", err)
				continue
			}

			// exercise 업데이트
			err = p.updateExerciseSolutionWithVideoID(exerciseID, videoID)
			if err != nil {
				log.Printf("해설 영상 업데이트 실패: %v", err)
				continue
			}

			_ = p.createExerciseContent(exerciseID, exerciseGroupID, sectionID, studentID, contentSequence, "example", exampleTitle)
			exerciseCounter++
		} else {
			// 강의 영상 처리
			title := extractTitle(filename)
			lectureTitle := generateLectureTitle(moduleType, lectureCount, lectureCounter)

			// 기존 콘텐츠 확인
			var existingContentID int64
			var existingLectureID int64
			checkQuery := `SELECT id, lecture_id FROM learning_contents WHERE section_id = $1 AND sequence = $2 AND content_type = 'lecture' AND user_id = $3 AND deleted_at IS NULL`
			err := p.db.QueryRow(checkQuery, sectionID, contentSequence, studentID).Scan(&existingContentID, &existingLectureID)

			if err == nil {
				// 기존 콘텐츠가 있음
				if p.forceReplaceVideo {
					// force-replace-video 옵션: 기존 콘텐츠의 비디오 교체
					log.Printf("기존 강의 콘텐츠의 비디오 교체: content_id %d, lecture_id %d", existingContentID, existingLectureID)

					// 새 비디오 생성
					var videoID int64
					videoID, err = p.createVideoFromURL(title, videoURL, s3Path)
					if err != nil {
						log.Printf("강의 비디오 생성 실패: %v", err)
						continue
					}

					// lecture의 video_id 업데이트
					updateQuery := `UPDATE lectures SET lecture_video_id = $1 WHERE id = $2`
					_, err = p.db.Exec(updateQuery, videoID, existingLectureID)
					if err != nil {
						log.Printf("강의 비디오 업데이트 실패: %v", err)
						continue
					}

					log.Printf("강의 비디오 교체 완료: lecture_id %d, new_video_id %d", existingLectureID, videoID)
				} else {
					// 일반 모드에서는 기존 콘텐츠가 있으면 스킵
					log.Printf("기존 강의 콘텐츠 존재 (sequence: %d), 스킵", contentSequence)
				}
				lectureCounter++
				continue
			}

			// 새로운 콘텐츠 생성 (기존 콘텐츠가 없을 때)
			// video 생성
			videoID, err := p.createVideoFromURL(title, videoURL, s3Path)
			if err != nil {
				log.Printf("강의 비디오 생성 실패: %v", err)
				continue
			}

			// lecture 생성
			lectureID, err := p.createLectureWithVideoID(title, videoID)
			if err != nil {
				log.Printf("강의 생성 실패: %v", err)
				continue
			}

			_ = p.createLectureContent(lectureID, sectionID, studentID, contentSequence, lectureTitle)
			lectureCounter++
		}
	}

	return nil
}

func (p *Parser) createLectureContent(lectureID, sectionID int64, studentID, sequence int, title string) error {
	// 새로운 강의 콘텐츠 생성 (중복 체크는 호출하는 곳에서 이미 함)
	query := `
		INSERT INTO learning_contents (title, content_type, lecture_id, exercise_id, required_exercise_group_id, sequence, section_id, user_id)
		VALUES ($1, 'lecture', $2, NULL, NULL, $3, $4, $5)`

	_, err := p.db.Exec(query, title, lectureID, sequence, sectionID, studentID)
	if err == nil {
		log.Printf("새 강의 콘텐츠 생성: title %s (sequence: %d)", title, sequence)
	}
	return err
}

func (p *Parser) createExerciseContent(exerciseID, exerciseGroupID int, sectionID int64, studentID, sequence int, exerciseType, title string) error {
	// 새로운 연습 콘텐츠 생성 (중복 체크는 호출하는 곳에서 이미 함)
	query := `
		INSERT INTO learning_contents (title, content_type, lecture_id, exercise_id, required_exercise_group_id, exercise_type, sequence, section_id, user_id)
		VALUES ($1, 'exercise', NULL, $2, $3, $4, $5, $6, $7)`

	_, err := p.db.Exec(query, title, exerciseID, exerciseGroupID, exerciseType, sequence, sectionID, studentID)
	if err == nil {
		log.Printf("새 연습 콘텐츠 생성: title %s (sequence: %d)", title, sequence)
	}
	return err
}

func (p *Parser) createAndUploadThumbnail(videoURL, s3Path string) error {
	// 임시 파일명 생성
	tempFile := fmt.Sprintf("/tmp/thumbnail_%d.png", time.Now().UnixNano())
	defer func() {
		_ = os.Remove(tempFile)
	}()

	// 경로 검증 및 ffmpeg 실행을 위한 안전한 경로
	cleanPath, err := ValidateTempPath(tempFile)
	if err != nil {
		return err
	}

	// ffmpeg로 썸네일 생성 (bash에서 성공했던 방식과 동일)
	cmd := exec.Command("ffmpeg", "-i", videoURL, "-vframes", "1", "-f", "image2", cleanPath, "-y")

	// 에러 출력 캡처
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("썸네일 생성 실패: %w, 출력: %s", err, string(output))
	}

	// S3에 업로드
	fileHandle, err := SafeOpenFile(cleanPath)
	if err != nil {
		return fmt.Errorf("썸네일 파일 열기 실패 -> %w", err)
	}
	defer func() {
		_ = fileHandle.Close()
	}()

	_, err = p.s3Client.PutObject(p.ctx, &s3.PutObjectInput{
		Bucket: aws.String(p.bucketName),
		Key:    aws.String(s3Path),
		Body:   fileHandle,
	})

	return err
}

// 유틸리티 함수들
func (p *Parser) getModuleType(moduleName string) string {
	if strings.Contains(moduleName, "개념") {
		return "concept"
	} else if strings.Contains(moduleName, "유형") {
		return "pattern"
	} else if strings.Contains(moduleName, "시험") {
		return "exam"
	}
	return "unknown"
}

// URL에서 MD5 해시 계산
func calculateURLMD5(url string) (string, error) {
	resp, err := http.Get(url) //nolint:gosec
	if err != nil {
		return "", err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	hash := md5.New() //nolint:gosec
	if _, err := io.Copy(hash, resp.Body); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

// URL 경로 인코딩 함수 - 한글은 유지하고 띄어쓰기와 주요 특수문자만 인코딩
func urlPathEncode(urlPath string) string {
	// 띄어쓰기와 주요 특수문자만 인코딩
	result := strings.ReplaceAll(urlPath, " ", "%20")
	result = strings.ReplaceAll(result, "+", "%2B")
	result = strings.ReplaceAll(result, "=", "%3D")
	result = strings.ReplaceAll(result, "&", "%26")
	result = strings.ReplaceAll(result, "#", "%23")
	result = strings.ReplaceAll(result, "?", "%3F")
	return result
}

func checkCommand(cmd string, args ...string) error {
	command := exec.Command(cmd, args...)
	return command.Run()
}

func getVideoDuration(videoURL string) (int, error) {
	cmd := exec.Command("ffprobe", "-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0", videoURL)
	output, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	durationStr := strings.TrimSpace(string(output))
	duration, err := strconv.ParseFloat(durationStr, 64)
	if err != nil {
		return 0, err
	}

	return int(duration), nil
}

func extractSequence(name string) int {
	re := regexp.MustCompile(`^(\d+)_`)
	matches := re.FindStringSubmatch(name)
	if len(matches) > 1 {
		seq, _ := strconv.Atoi(matches[1])
		return seq
	}
	return 0
}

func extractSequenceWithIndex(name string, index int) int {
	// 먼저 이름에서 숫자 추출 시도
	seq := extractSequence(name)
	if seq > 0 {
		return seq
	}
	// 숫자가 없으면 인덱스 사용
	return index
}

func extractTitle(filename string) string {
	// 0_제목.mov -> 제목
	re := regexp.MustCompile(`^\d+_(.+)\.(mov|mp4)$`)
	matches := re.FindStringSubmatch(filename)
	if len(matches) > 1 {
		return matches[1]
	}
	return filename
}

func extractSectionTitle(name string) string {
	// 0_섹션명 -> 섹션명
	re := regexp.MustCompile(`^\d+_(.+)$`)
	matches := re.FindStringSubmatch(name)
	if len(matches) > 1 {
		return matches[1]
	}
	return name
}

func isSolutionFile(filename string) bool {
	return strings.Contains(filename, "해설")
}

func extractExerciseID(filename string) int {
	// 파일명_1234.mov -> 1234
	re := regexp.MustCompile(`_(\d+)\.(mov|mp4)$`)
	matches := re.FindStringSubmatch(filename)
	if len(matches) > 1 {
		id, _ := strconv.Atoi(matches[1])
		return id
	}
	return 0
}

func extractExerciseGroupID(filename string) int {
	// 해설_1201_2399.mov -> 1201
	if strings.Contains(filename, "해설") {
		re := regexp.MustCompile(`해설_(\d+)_\d+\.(mov|mp4)$`)
		matches := re.FindStringSubmatch(filename)
		if len(matches) > 1 {
			id, _ := strconv.Atoi(matches[1])
			return id
		}
	}
	return 0
}

func generateLectureTitle(moduleType string, lectureCount, lectureIndex int) string {
	baseTitle := "강의"
	switch moduleType {
	case "concept":
		baseTitle = "개념강의"
	case "pattern":
		baseTitle = "유형강의"
	}

	if lectureCount > 1 {
		return fmt.Sprintf("%s%d", baseTitle, lectureIndex+1)
	}
	return baseTitle
}

func generateExerciseTitle(exerciseType string, exerciseNumber int) string {
	switch exerciseType {
	case "example":
		return fmt.Sprintf("예제%d", exerciseNumber)
	default:
		return fmt.Sprintf("문제%d", exerciseNumber)
	}
}
