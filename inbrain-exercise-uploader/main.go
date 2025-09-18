package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

func processExercises(ctx context.Context, database *sql.DB, jsonProblems []map[string]any, basename string, isG bool) error {
	for _, v := range jsonProblems {
		// 각 문제를 개별 트랜잭션으로 처리
		tx, err := database.BeginTx(ctx, nil)
		if err != nil {
			fmt.Printf("Warning: failed to begin transaction: %v\n", err)
			continue
		}
		err = processExercise(ctx, tx, v, basename, isG)
		if err != nil {
			tx.Rollback()
			fmt.Printf("Warning: failed to process exercise: %v\n", err)
			// 개별 문제 처리 실패는 경고만 하고 다음 문제 계속 처리
		} else {
			tx.Commit()
		}
	}
	return nil
}

func processExercise(ctx context.Context, tx *sql.Tx, v map[string]any, basename string, isG bool) error {

	// 타입 안전성 개선
	typeStr, ok := v["type"].(string)
	if !ok {
		return nil // skip this exercise
	}
	if typeStr != "SINGLE_CHOICE" && typeStr != "SHORT_ANSWER" {
		return nil // skip this exercise
	}

	conceptIDFloat, ok := v["conceptId"].(float64)
	if !ok {
		return errors.New("invalid conceptId")
	}
	conceptID := int(conceptIDFloat)

	// SQL Injection 방지
	query := `SELECT id FROM categories WHERE metadata->>'mathflatConceptId' = $1 AND deleted_at IS NULL`
	rows, err := tx.QueryContext(ctx, query, strconv.Itoa(conceptID))
	if err != nil {
		return fmt.Errorf("failed to query categories: %w", err)
	}
	defer rows.Close()

	var categories []struct{ ID int64 }
	for rows.Next() {
		var cat struct{ ID int64 }
		err := rows.Scan(&cat.ID)
		if err != nil {
			return fmt.Errorf("failed to scan category: %w", err)
		}
		categories = append(categories, cat)
	}
	err = rows.Err()

	if err != nil {
		return fmt.Errorf("failed to find category: %w", err)
	}
	if len(categories) == 0 {
		return fmt.Errorf("no category found for conceptId: %d", conceptID)
	}

	// Exercise Group 처리
	groupCodeFloat, hasGroupCode := v["groupCode"].(float64)
	var exerciseGroup struct {
		ID               int64
		RepresentativeID *int64
	}

	if hasGroupCode {
		groupCode := int(groupCodeFloat)
		query := `SELECT id, representative_id FROM exercise_groups
				  WHERE metadata->>'mathflatConceptId' = $1 AND metadata->>'mathflatGroupCode' = $2 AND deleted_at IS NULL`
		err = tx.QueryRowContext(ctx, query, strconv.Itoa(conceptID), strconv.Itoa(groupCode)).Scan(
			&exerciseGroup.ID, &exerciseGroup.RepresentativeID)

		if err != nil {
			if err == sql.ErrNoRows {
				// 메타데이터 타입 일관성 - 모두 int로 통일
				metadataData := map[string]any{
					"mathflatConceptId": conceptID,
					"mathflatGroupCode": groupCode,
				}
				metadataBytes, _ := json.Marshal(metadataData)
				insertQuery := `INSERT INTO exercise_groups (metadata, category_id, created_at, updated_at)
						   VALUES ($1, $2, NOW(), NOW())
						   RETURNING id, representative_id`
				err = tx.QueryRowContext(ctx, insertQuery, metadataBytes, categories[0].ID).Scan(
					&exerciseGroup.ID, &exerciseGroup.RepresentativeID)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
	} else {
		// 메타데이터 타입 일관성 - int로 통일
		metadataData := map[string]any{
			"mathflatConceptId": conceptID,
		}
		metadataBytes, _ := json.Marshal(metadataData)
		insertQuery := `INSERT INTO exercise_groups (metadata, category_id, created_at, updated_at)
				   VALUES ($1, $2, NOW(), NOW())
				   RETURNING id, representative_id`
		err = tx.QueryRowContext(ctx, insertQuery, metadataBytes, categories[0].ID).Scan(
			&exerciseGroup.ID, &exerciseGroup.RepresentativeID)
		if err != nil {
			return err
		}
	}

	// 타입 안전성 검증
	problemID, ok := v["id"].(float64)
	if !ok {
		return errors.New("invalid problem id")
	}
	problemIDInt := int(problemID)

	// 기존 Exercise 확인
	var existingExercise struct {
		ID         int64
		References []byte
	}
	query = `SELECT id, references FROM exercises
			 WHERE metadata->>'mathflatProblemId' = $1 AND deleted_at IS NULL`
	err = tx.QueryRowContext(ctx, query, strconv.Itoa(problemIDInt)).Scan(
		&existingExercise.ID, &existingExercise.References)

	// tagTop 처리 - references 필드로 변환
	var references []string
	if tagTopStr, ok := v["tagTop"].(string); ok && tagTopStr != "" {
		// \n으로 분리하여 references 배열 생성
		references = strings.Split(tagTopStr, "\n")
		// 각 reference 앞뒤 공백 제거
		for i := range references {
			references[i] = strings.TrimSpace(references[i])
		}
	}

	if isG {
		sp := strings.Split(basename, "_")
		gRefs := []string{"[기출]"}
		for i, sp := range sp {
			spstr := sp
			if i == 1 {
				spstr += "년"
			} else if i == 2 {
				spstr += "월"
			}
			gRefs = append(gRefs, spstr)
		}
		fullname := strings.Join(gRefs, " ")

		if defaultOrder, ok := v["defaultOrder"].(float64); ok {
			references = append(references, fmt.Sprintf("%s, 1p, %d", fullname, int(defaultOrder)))
		}
	}

	// 이미 존재하면 references만 업데이트
	if err == nil {
		fmt.Printf("Exercise with mathflatProblemId %d already exists", problemIDInt)

		// 기존 references 가져오기
		var existingRefs []string
		if len(existingExercise.References) > 0 {
			_ = json.Unmarshal(existingExercise.References, &existingRefs)
		}

		// 중복 제거하며 병합
		refMap := make(map[string]bool)
		for _, ref := range existingRefs {
			if ref != "" {
				refMap[ref] = true
			}
		}
		for _, ref := range references {
			if ref != "" {
				refMap[ref] = true
			}
		}

		// map을 다시 slice로 변환
		var mergedRefs []string
		for ref := range refMap {
			mergedRefs = append(mergedRefs, ref)
		}

		// references가 변경된 경우만 업데이트
		if len(mergedRefs) > len(existingRefs) {
			referencesData, _ := json.Marshal(mergedRefs)

			updateQuery := `UPDATE exercises SET references = $1, updated_at = NOW() WHERE id = $2`
			_, err = tx.ExecContext(ctx, updateQuery, referencesData, existingExercise.ID)

			if err != nil {
				return fmt.Errorf("failed to update exercise references: %w", err)
			}
			fmt.Printf(", updated references (added %d new)\n", len(mergedRefs)-len(existingRefs))
		} else {
			fmt.Printf(", skipping (no new references)\n")
		}
		return nil // 이미 존재하는 문제 처리 완료
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to check existing exercise: %w", err)
	}

	// Exercise 데이터 준비
	exerciseUUID := uuid.New()

	metadataData := map[string]any{
		"mathflatProblemId": problemIDInt,
	}
	metadataBytes, _ := json.Marshal(metadataData)

	problemImageURL, _ := v["problemImageUrl"].(string)
	questionImagesData, _ := json.Marshal([]string{problemImageURL})

	solutionImageURL, _ := v["solutionImageUrl"].(string)
	solutionImagesData, _ := json.Marshal([]string{solutionImageURL})

	conceptName, _ := v["conceptName"].(string)
	level, _ := v["level"].(float64)
	rate, _ := v["rate"].(float64)
	answerImageURL, _ := v["answerImageUrl"].(string)
	isTrendy, _ := v["trendy"].(bool)

	// references 준비
	var referencesData []byte
	if len(references) > 0 {
		referencesData, _ = json.Marshal(references)
	} else {
		referencesData, _ = json.Marshal([]string{})
	}

	var levelPtr, ratePtr *int64
	var answerImagePtr *string
	var isTrendyPtr *bool
	var categoryIDPtr *int64

	if level != 0 {
		l := int64(level)
		levelPtr = &l
	}
	if rate != 0 {
		r := int64(rate)
		ratePtr = &r
	}
	if answerImageURL != "" {
		answerImagePtr = &answerImageURL
	}
	isTrendyPtr = &isTrendy
	categoryIDPtr = &categories[0].ID

	var objectiveAnswer *int64
	var subjectiveAnswer *string

	switch typeStr {
	case "SINGLE_CHOICE":
		answerStr, ok := v["answer"].(string)
		if !ok {
			return errors.New("invalid answer format")
		}

		answer, err := strconv.Atoi(answerStr)
		if err != nil {
			return err
		}
		a := int64(answer)
		objectiveAnswer = &a
	case "SHORT_ANSWER":
		answerStr, _ := v["answer"].(string)
		subjectiveAnswer = &answerStr
	default:
		return errors.New("unknown type")
	}

	// Exercise INSERT
	insertQuery := `INSERT INTO exercises (
		uuid, title, level, rate, metadata, question_images, answer_image,
		solution_images, is_trendy, category_id, objective_answer,
		subjective_answer, exercise_group_id, references, answer_type, created_at, updated_at
	) VALUES (
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, NOW(), NOW()
	) RETURNING id`

	var exerciseID int64
	err = tx.QueryRowContext(ctx, insertQuery,
		exerciseUUID.String(), conceptName, levelPtr, ratePtr, metadataBytes,
		questionImagesData, answerImagePtr, solutionImagesData, isTrendyPtr,
		categoryIDPtr, objectiveAnswer, subjectiveAnswer, exerciseGroup.ID,
		referencesData, typeStr,
	).Scan(&exerciseID)
	if err != nil {
		return err
	}

	// ExerciseGroup에 대표 문제가 없으면 현재 문제를 대표 문제로 설정
	if exerciseGroup.RepresentativeID == nil {
		updateQuery := `UPDATE exercise_groups SET representative_id = $1, updated_at = NOW() WHERE id = $2`
		_, err = tx.ExecContext(ctx, updateQuery, exerciseID, exerciseGroup.ID)
		if err != nil {
			return err
		}
		exerciseGroup.RepresentativeID = &exerciseID
	}

	return nil
}

// 카테고리 구조 생성 함수 (기존 setUnit의 로직)
// sequence 파일 관리 함수들
func getSequenceFilePath(category string) string {
	return filepath.Join("cmd", "seed", "sequences", category+".txt")
}

func loadSequenceMap(category string) (map[string]int, error) {
	filePath := getSequenceFilePath(category)
	sequenceMap := make(map[string]int)

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// 파일이 없으면 빈 맵 반환
			return sequenceMap, nil
		}
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	sequence := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			sequenceMap[line] = sequence
			sequence++
		}
	}

	return sequenceMap, scanner.Err()
}

func saveSequenceMap(category string, sequenceMap map[string]int) error {
	filePath := getSequenceFilePath(category)

	// map을 sequence 순서대로 정렬
	items := make([]string, len(sequenceMap))
	for name, seq := range sequenceMap {
		items[seq] = name
	}

	// 파일에 쓰기
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, item := range items {
		if item != "" {
			if _, err := writer.WriteString(item + "\n"); err != nil {
				return err
			}
		}
	}
	return writer.Flush()
}

func getOrAssignSequence(category string, name string) (int64, error) {
	sequenceMap, err := loadSequenceMap(category)
	if err != nil {
		return 0, err
	}

	// 이미 있으면 해당 sequence 반환
	if seq, exists := sequenceMap[name]; exists {
		return int64(seq), nil
	}

	// 없으면 새 sequence 할당
	newSeq := len(sequenceMap)
	sequenceMap[name] = newSeq

	// 파일에 저장
	if err := saveSequenceMap(category, sequenceMap); err != nil {
		return 0, err
	}

	return int64(newSeq), nil
}

func createCategoryStructure(ctx context.Context, tx *sql.Tx, conceptInfo map[string]any) error {
	// revisionName에서 교육과정 추출
	var c1 string
	var s1 int64
	if revisionName, ok := conceptInfo["revisionName"].(string); ok {
		c1 = revisionName
		var err error
		s1, err = getOrAssignSequence("curriculum", c1)
		if err != nil {
			return fmt.Errorf("failed to get curriculum sequence: %w", err)
		}
	} else {
		return errors.New("revisionName not found")
	}

	// 학교 레벨 설정
	c2 := "고등학교"
	var s2 int64
	if schoolName, ok := conceptInfo["schoolName"].(string); ok && schoolName != "" {
		c2 = schoolName
	}
	s2, err := getOrAssignSequence("school", c2)
	if err != nil {
		return fmt.Errorf("failed to get school sequence: %w", err)
	}

	c3 := "수학"
	s3 := int64(0)

	// 카테고리 생성 (빈 metadata 대신 빈 map 전달)
	curriculumCategory, err := getOrCreateCategory(ctx, tx, nil, c1, s1, map[string]any{})
	if err != nil {
		return err
	}

	schoolCategory, err := getOrCreateCategory(ctx, tx, &curriculumCategory.ID, c2, s2, map[string]any{})
	if err != nil {
		return err
	}

	subjectCategory, err := getOrCreateCategory(ctx, tx, &schoolCategory.ID, c3, s3, map[string]any{})
	if err != nil {
		return err
	}

	// subjectName 처리 및 세부과목 카테고리 생성
	var detailCategory *struct{ ID int64 }
	if c2 == "고등학교" {
		// 고등학교는 subjectName 사용
		if subjectName, ok := conceptInfo["subjectName"].(string); ok && subjectName != "" {
			c4 := subjectName

			s4, err := getOrAssignSequence("high_subject", c4)
			if err != nil {
				return err
			}

			subjectId, _ := conceptInfo["subjectId"].(float64)

			detailCategory, err = getOrCreateCategory(ctx, tx, &subjectCategory.ID, c4, s4, map[string]any{
				"mathflatSubjectID": int(subjectId),
			})
			if err != nil {
				return err
			}

		} else {
			return errors.New("subjectName not found or empty for high school")
		}
	} else if c2 == "중학교" {
		// 중학교는 gradeName + semesterName 조합
		gradeName, _ := conceptInfo["gradeName"].(string)
		semesterName, _ := conceptInfo["semesterName"].(string)

		if gradeName != "" && semesterName != "" {
			c4 := fmt.Sprintf("%s %s", gradeName, semesterName) // "3 학년 1 학기"

			s4, err := getOrAssignSequence("middle_subject", c4)
			if err != nil {
				return fmt.Errorf("failed to get middle school subject sequence: %w", err)
			}

			detailCategory, err = getOrCreateCategory(ctx, tx, &subjectCategory.ID, c4, s4, map[string]any{})
			if err != nil {
				return err
			}

		} else {
			return fmt.Errorf("gradeName or semesterName not found for middle school: grade=%s, semester=%s", gradeName, semesterName)
		}
	}

	// detailCategory가 없으면 오류 처리
	if detailCategory == nil {
		return errors.New("detail category is nil")
	}

	// 대단원, 중단원, 소단원, 개념 생성 - 이제 세부과목(detailCategory) 아래에 생성
	// conceptInfo에서 대단원, 중단원, 소단원, 개념 정보 추출하여 생성
	if bigChapterId, ok := conceptInfo["bigChapterId"].(float64); ok {
		if bigChapterName, ok := conceptInfo["bigChapterName"].(string); ok {

			bigChapter, err := getOrCreateCategory(ctx, tx, &detailCategory.ID, bigChapterName, 0, map[string]any{
				"mathflatBigChapterId": int(bigChapterId),
			})
			if err != nil {
				return err
			}

			if middleChapterId, ok := conceptInfo["middleChapterId"].(float64); ok {
				if middleChapterName, ok := conceptInfo["middleChapterName"].(string); ok {
					middleChapter, err := getOrCreateCategory(ctx, tx, &bigChapter.ID, middleChapterName, 0, map[string]any{
						"mathflatMiddleChapterId": int(middleChapterId),
					})
					if err != nil {
						return err
					}

					if littleChapterId, ok := conceptInfo["littleChapterId"].(float64); ok {
						if littleChapterName, ok := conceptInfo["littleChapterName"].(string); ok {
							littleChapter, err := getOrCreateCategory(ctx, tx, &middleChapter.ID, littleChapterName, 0, map[string]any{
								"mathflatLittleChapterId": int(littleChapterId),
							})
							if err != nil {
								return err
							}

							if conceptId, ok := conceptInfo["conceptId"].(float64); ok {
								if conceptName, ok := conceptInfo["conceptName"].(string); ok {
									_, err = getOrCreateCategory(ctx, tx, &littleChapter.ID, conceptName, 0, map[string]any{
										"mathflatConceptId": int(conceptId),
									})
									if err != nil {
										return err
									}
								}
							}
						}
					}
				}
			}
		}
	}

	return nil
}

// processFolder는 폴더 내 모든 JSON 파일을 처리합니다
func processFolder(database *sql.DB, folderPath string, isG bool) {
	// 폴더 내 모든 JSON 파일 찾기
	pattern := filepath.Join(folderPath, "*.json")
	files, err := filepath.Glob(pattern)
	if err != nil {
		fmt.Printf("Error finding JSON files: %v\n", err)
		return
	}

	if len(files) == 0 {
		fmt.Printf("No JSON files found in %s\n", folderPath)
		return
	}

	fmt.Printf("Found %d JSON files in %s\n", len(files), folderPath)

	// 각 파일 처리
	for i, filePath := range files {
		fmt.Printf("\n[%d/%d] Processing: %s\n", i+1, len(files), filepath.Base(filePath))

		err := processFile(database, filePath, isG)
		if err != nil {
			fmt.Printf("Error processing %s: %v\n", filepath.Base(filePath), err)
			// 에러가 있어도 다음 파일 계속 처리
			continue
		}

		fmt.Printf("Successfully processed: %s\n", filepath.Base(filePath))
	}

	fmt.Printf("\nCompleted processing %d files\n", len(files))
}

// processFile은 단일 JSON 파일을 처리합니다
func processFile(database *sql.DB, filename string, isG bool) error {
	// JSON 파일 읽기
	jsonFile, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer jsonFile.Close()

	data, err := io.ReadAll(jsonFile)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	var jsonProblems []map[string]any
	err = json.Unmarshal(data, &jsonProblems)
	if err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	if len(jsonProblems) == 0 {
		return fmt.Errorf("empty json file")
	}

	ctx := context.Background()

	// 1. 모든 문제의 concept 정보로 카테고리 구조 생성 (개별 트랜잭션)
	conceptMap := make(map[float64]bool)
	for _, problem := range jsonProblems {
		if concept, ok := problem["concept"].(map[string]any); ok {
			if conceptId, ok := concept["conceptId"].(float64); ok {
				if !conceptMap[conceptId] {
					conceptMap[conceptId] = true
					// 개별 트랜잭션으로 카테고리 구조 생성
					tx, err := database.BeginTx(ctx, nil)
					if err != nil {
						fmt.Printf("Warning: failed to begin transaction for category: %v\n", err)
						continue
					}
					err = createCategoryStructure(ctx, tx, concept)
					if err != nil {
						tx.Rollback()
					} else {
						tx.Commit()
					}
					if err != nil {
						fmt.Printf("Warning: failed to create category structure for conceptId %v: %v\n", conceptId, err)
						// 카테고리 생성 실패는 경고만 하고 계속 진행
					}
				}
			}
		}
	}

	basename := filepath.Base(filename)
	basename = strings.ReplaceAll(basename, filepath.Ext(basename), "")

	// 2. Exercise 처리 (개별 트랜잭션)
	return processExercises(ctx, database, jsonProblems, basename, isG)
}

func main() {
	// 플래그 정의
	var (
		dataType   = flag.String("type", "", "문제집 타입: m(문제집), g(기출)")
		folderPath = flag.String("folder", "", "JSON 파일이 있는 폴더 경로")
		dbHost     = flag.String("host", "localhost", "데이터베이스 호스트")
		dbPort     = flag.String("port", "5432", "데이터베이스 포트")
		dbName     = flag.String("db", "postgres", "데이터베이스 이름")
		sslMode    = flag.String("sslmode", "disable", "SSL 모드")
	)

	dbUser := "app_user" // 항상 고정

	flag.Parse()

	// 필수 플래그 검사
	if *dataType == "" || *folderPath == "" || *dbName == "" {
		fmt.Println("Usage:")
		flag.PrintDefaults()
		fmt.Println("\nExample:")
		fmt.Println("  go run main.go -type=g -folder=data/_전체/수능모의고사 -host=localhost -port=5432 -db=mydb")
		fmt.Println("\nNote: DB password is automatically retrieved from AWS Secrets Manager")
		os.Exit(1)
	}

	if *dataType != "m" && *dataType != "g" {
		fmt.Println("Error: type must be 'm' (문제집) or 'g' (기출)")
		os.Exit(1)
	}

	// 폴더가 존재하는지 확인
	fileInfo, err := os.Stat(*folderPath)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
	if !fileInfo.IsDir() {
		fmt.Printf("Error: %s is not a directory\n", *folderPath)
		os.Exit(1)
	}

	// AWS Secrets Manager에서 DB 패스워드 가져오기
	fmt.Println("Retrieving DB password from AWS Secrets Manager...")
	dbPassword, err := getDBPasswordFromSecretsManager()
	if err != nil {
		fmt.Printf("Error retrieving DB password: %v\n", err)
		os.Exit(1)
	}

	// PostgreSQL 데이터베이스 연결 문자열 생성
	dbURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		dbUser, dbPassword, *dbHost, *dbPort, *dbName, *sslMode)

	fmt.Printf("Connecting to database: postgres://%s:***@%s:%s/%s?sslmode=%s\n",
		dbUser, *dbHost, *dbPort, *dbName, *sslMode)

	database, err := sql.Open("postgres", dbURL)
	if err != nil {
		fmt.Printf("Error connecting to database: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		_ = database.Close()
	}()

	isG := *dataType == "g"

	// 폴더 내 모든 JSON 파일 처리
	processFolder(database, *folderPath, isG)
}

func getOrCreateCategory(ctx context.Context, tx *sql.Tx, parentID *int64, title string, sequence int64, metadata map[string]any) (*struct{ ID int64 }, error) {
	var category struct{ ID int64 }
	var err error

	if parentID == nil {
		query := `SELECT id FROM categories WHERE parent_id IS NULL AND title = $1 AND deleted_at IS NULL LIMIT 1`
		err = tx.QueryRowContext(ctx, query, title).Scan(&category.ID)
	} else {
		query := `SELECT id FROM categories WHERE parent_id = $1 AND title = $2 AND deleted_at IS NULL LIMIT 1`
		err = tx.QueryRowContext(ctx, query, *parentID, title).Scan(&category.ID)
	}

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// metadata가 nil이면 빈 object로 설정
			if metadata == nil {
				metadata = map[string]any{}
			}
			metadataBytes, _ := json.Marshal(metadata)

			insertQuery := `INSERT INTO categories (title, parent_id, sequence, metadata, created_at, updated_at)
						   VALUES ($1, $2, $3, $4, NOW(), NOW())
						   RETURNING id`
			err = tx.QueryRowContext(ctx, insertQuery, title, parentID, sequence, metadataBytes).Scan(&category.ID)
			if err != nil {
				return nil, fmt.Errorf("failed to create category: %w", err)
			}
			return &category, nil
		}
		return nil, fmt.Errorf("failed to query category: %w", err)
	}

	return &category, nil
}

// AWS Secrets Manager에서 DB 패스워드 가져오기
func getDBPasswordFromSecretsManager() (string, error) {
	// AWS 세션 생성 (서울 리전)
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("ap-northeast-2"),
	})
	if err != nil {
		return "", fmt.Errorf("failed to create AWS session: %w", err)
	}

	// Secrets Manager 클라이언트 생성
	svc := secretsmanager.New(sess)

	// 시크릿 값 가져오기
	secretName := "base-inbrain/production/DB_PASSWORD"
	result, err := svc.GetSecretValue(&secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretName),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get secret value: %w", err)
	}

	// JSON 파싱
	var secretData map[string]string
	err = json.Unmarshal([]byte(*result.SecretString), &secretData)
	if err != nil {
		return "", fmt.Errorf("failed to parse secret JSON: %w", err)
	}

	password, exists := secretData["password"]
	if !exists {
		return "", fmt.Errorf("password field not found in secret")
	}

	return password, nil
}
