package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/qrm"
	"github.com/google/uuid"

	"github.com/unboxerscorp/base-server/.gen/model"
	"github.com/unboxerscorp/base-server/.gen/table"
	"github.com/unboxerscorp/base-server/db"
	"github.com/unboxerscorp/base-server/internal/types"
	"github.com/unboxerscorp/base-server/internal/utils/file"
	"github.com/unboxerscorp/base-server/internal/utils/pointer"
)

func processExercises(ctx context.Context, database *sql.DB, jsonProblems []map[string]any, basename string, isG bool) error {
	for _, v := range jsonProblems {
		// 각 문제를 개별 트랜잭션으로 처리
		err := db.ExecWithTx(ctx, database, func(ctx context.Context, tx *sql.Tx) error {
			return processExercise(ctx, database, v, basename, isG)
		})
		if err != nil {
			fmt.Printf("Warning: failed to process exercise: %v\n", err)
			// 개별 문제 처리 실패는 경고만 하고 다음 문제 계속 처리
		}
	}
	return nil
}

func processExercise(ctx context.Context, database *sql.DB, v map[string]any, basename string, isG bool) error {
	executor := db.GetExecutor(ctx, database)

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

	var categories []model.Categories
	// SQL Injection 방지 - CAST 사용
	err := postgres.SELECT(
		table.Categories.AllColumns,
	).FROM(
		table.Categories,
	).WHERE(
		postgres.RawBool("metadata->'mathflatConceptId' = conceptID", postgres.RawArgs{"conceptID": conceptID}),
	).Query(executor, &categories)

	if err != nil {
		return fmt.Errorf("failed to find category: %w", err)
	}
	if len(categories) == 0 {
		return fmt.Errorf("no category found for conceptId: %d", conceptID)
	}

	// Exercise Group 처리
	groupCodeFloat, hasGroupCode := v["groupCode"].(float64)
	var exerciseGroup model.ExerciseGroups

	if hasGroupCode {
		groupCode := int(groupCodeFloat)
		err = postgres.SELECT(
			table.ExerciseGroups.AllColumns,
		).FROM(
			table.ExerciseGroups,
		).WHERE(
			postgres.RawBool("metadata->'mathflatConceptId' = conceptID", postgres.RawArgs{"conceptID": conceptID}).AND(
				postgres.RawBool("metadata->'mathflatGroupCode' = groupCode", postgres.RawArgs{"groupCode": groupCode}),
			),
		).Query(executor, &exerciseGroup)

		if err != nil {
			if errors.Is(err, qrm.ErrNoRows) {
				// 메타데이터 타입 일관성 - 모두 int로 통일
				metadataData := map[string]any{
					"mathflatConceptId": conceptID,
					"mathflatGroupCode": groupCode,
				}
				metadataBytes, _ := json.Marshal(metadataData)
				metadata := types.JSONB(metadataBytes)
				err = table.ExerciseGroups.INSERT(
					table.ExerciseGroups.Metadata,
					table.ExerciseGroups.CategoryID,
				).VALUES(
					metadata,
					categories[0].ID,
				).RETURNING(table.ExerciseGroups.AllColumns).Query(executor, &exerciseGroup)
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
		metadata := types.JSONB(metadataBytes)
		err = table.ExerciseGroups.INSERT(
			table.ExerciseGroups.Metadata,
			table.ExerciseGroups.CategoryID,
		).VALUES(
			metadata,
			categories[0].ID,
		).RETURNING(table.ExerciseGroups.AllColumns).Query(executor, &exerciseGroup)
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
	var existingExercise model.Exercises
	err = postgres.SELECT(
		table.Exercises.AllColumns,
	).FROM(
		table.Exercises,
	).WHERE(
		postgres.RawBool("metadata->'mathflatProblemId' = problemID", postgres.RawArgs{"problemID": problemIDInt}).AND(table.Exercises.DeletedAt.IS_NULL()),
	).Query(executor, &existingExercise)

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
			referencesJSON := types.JSONB(referencesData)

			_, err = table.Exercises.UPDATE(
				table.Exercises.References,
			).SET(
				&referencesJSON,
			).WHERE(
				table.Exercises.ID.EQ(postgres.Int64(existingExercise.ID)),
			).Exec(executor)

			if err != nil {
				return fmt.Errorf("failed to update exercise references: %w", err)
			}
			fmt.Printf(", updated references (added %d new)\n", len(mergedRefs)-len(existingRefs))
		} else {
			fmt.Printf(", skipping (no new references)\n")
		}
		return nil // 이미 존재하는 문제 처리 완료
	}
	if !errors.Is(err, qrm.ErrNoRows) {
		return fmt.Errorf("failed to check existing exercise: %w", err)
	}

	// Exercise 데이터 준비
	uuid := uuid.New()

	metadataData := map[string]any{
		"mathflatProblemId": problemIDInt,
	}
	metadataBytes, _ := json.Marshal(metadataData)
	metadata := types.JSONB(metadataBytes)

	problemImageURL, _ := v["problemImageUrl"].(string)
	questionImagesData, _ := json.Marshal([]string{problemImageURL})
	questionImages := types.JSONB(questionImagesData)

	solutionImageURL, _ := v["solutionImageUrl"].(string)
	solutionImagesData, _ := json.Marshal([]string{solutionImageURL})
	solutionImages := types.JSONB(solutionImagesData)

	conceptName, _ := v["conceptName"].(string)
	level, _ := v["level"].(float64)
	rate, _ := v["rate"].(float64)
	answerImageURL, _ := v["answerImageUrl"].(string)
	isTrendy, _ := v["trendy"].(bool)

	// references 준비
	var referencesJSON types.JSONB
	if len(references) > 0 {
		referencesData, _ := json.Marshal(references)
		referencesJSON = types.JSONB(referencesData)
	} else {
		referencesJSON = types.JSONB("[]")
	}

	exercise := &model.Exercises{
		UUID:            uuid.String(),
		Title:           conceptName,
		Level:           pointer.To(int64(level)),
		Rate:            pointer.To(int64(rate)),
		Metadata:        metadata,
		QuestionImages:  questionImages,
		AnswerImage:     pointer.To(answerImageURL),
		SolutionImages:  solutionImages,
		IsTrendy:        pointer.To(isTrendy),
		CategoryID:      pointer.To(categories[0].ID),
		ExerciseGroupID: exerciseGroup.ID,
		References:      referencesJSON,
	}

	switch typeStr {
	case "SINGLE_CHOICE":
		answerStr, ok := v["answer"].(string)
		if !ok {
			return errors.New("invalid answer format")
		}

		var answer int
		answer, err = strconv.Atoi(answerStr)
		if err != nil {
			return err
		}
		exercise.ObjectiveAnswer = pointer.To(int64(answer))
	case "SHORT_ANSWER":
		answerStr, _ := v["answer"].(string)
		exercise.SubjectiveAnswer = pointer.To(answerStr)
	default:
		return errors.New("unknown type")
	}

	err = table.Exercises.INSERT(
		table.Exercises.UUID,
		table.Exercises.Title,
		table.Exercises.Level,
		table.Exercises.Rate,
		table.Exercises.Metadata,
		table.Exercises.QuestionImages,
		table.Exercises.AnswerImage,
		table.Exercises.SolutionImages,
		table.Exercises.IsTrendy,
		table.Exercises.CategoryID,
		table.Exercises.ObjectiveAnswer,
		table.Exercises.SubjectiveAnswer,
		table.Exercises.ExerciseGroupID,
		table.Exercises.References,
	).
		MODEL(exercise).
		RETURNING(table.Exercises.AllColumns).
		Query(executor, exercise)
	if err != nil {
		return err
	}

	// ExerciseGroup에 대표 문제가 없으면 현재 문제를 대표 문제로 설정
	if exerciseGroup.RepresentativeID == nil {
		_, err = table.ExerciseGroups.UPDATE(
			table.ExerciseGroups.RepresentativeID,
		).SET(
			exercise.ID,
		).WHERE(
			table.ExerciseGroups.ID.EQ(postgres.Int64(exerciseGroup.ID)),
		).Exec(executor)
		if err != nil {
			return err
		}
		exerciseGroup.RepresentativeID = &exercise.ID
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

func createCategoryStructure(ctx context.Context, executor qrm.DB, conceptInfo map[string]any) error {
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
	curriculumCategory, err := getOrCreateCategory(executor, nil, c1, s1, map[string]any{})
	if err != nil {
		return err
	}

	schoolCategory, err := getOrCreateCategory(executor, &curriculumCategory.ID, c2, s2, map[string]any{})
	if err != nil {
		return err
	}

	subjectCategory, err := getOrCreateCategory(executor, &schoolCategory.ID, c3, s3, map[string]any{})
	if err != nil {
		return err
	}

	// subjectName 처리 및 세부과목 카테고리 생성
	var detailCategory *model.Categories
	if c2 == "고등학교" {
		// 고등학교는 subjectName 사용
		if subjectName, ok := conceptInfo["subjectName"].(string); ok && subjectName != "" {
			c4 := subjectName

			s4, err := getOrAssignSequence("high_subject", c4)
			if err != nil {
				return err
			}

			subjectId, _ := conceptInfo["subjectId"].(float64)

			detailCategory, err = getOrCreateCategory(executor, &subjectCategory.ID, c4, s4, map[string]any{
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

			detailCategory, err = getOrCreateCategory(executor, &subjectCategory.ID, c4, s4, map[string]any{})
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

			bigChapter, err := getOrCreateCategory(executor, &detailCategory.ID, bigChapterName, 0, map[string]any{
				"mathflatBigChapterId": int(bigChapterId),
			})
			if err != nil {
				return err
			}

			if middleChapterId, ok := conceptInfo["middleChapterId"].(float64); ok {
				if middleChapterName, ok := conceptInfo["middleChapterName"].(string); ok {
					middleChapter, err := getOrCreateCategory(executor, &bigChapter.ID, middleChapterName, 0, map[string]any{
						"mathflatMiddleChapterId": int(middleChapterId),
					})
					if err != nil {
						return err
					}

					if littleChapterId, ok := conceptInfo["littleChapterId"].(float64); ok {
						if littleChapterName, ok := conceptInfo["littleChapterName"].(string); ok {
							littleChapter, err := getOrCreateCategory(executor, &middleChapter.ID, littleChapterName, 0, map[string]any{
								"mathflatLittleChapterId": int(littleChapterId),
							})
							if err != nil {
								return err
							}

							if conceptId, ok := conceptInfo["conceptId"].(float64); ok {
								if conceptName, ok := conceptInfo["conceptName"].(string); ok {
									_, err = getOrCreateCategory(executor, &littleChapter.ID, conceptName, 0, map[string]any{
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
	data, err := file.SafeReadFile(filename)
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
					err := db.ExecWithTx(ctx, database, func(ctx context.Context, tx *sql.Tx) error {
						executor := db.GetExecutor(ctx, database)
						return createCategoryStructure(ctx, executor, concept)
					})
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
	// 폴더 경로를 argument로 받기
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go <type - 문제집(m), 기출(g)> <folder_path>")
		fmt.Println("Example: go run main.go g data/_전체/수능모의고사")
		os.Exit(1)
	}

	dataType := os.Args[1]
	folderPath := os.Args[2]

	// 폴더가 존재하는지 확인
	fileInfo, err := os.Stat(folderPath)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
	if !fileInfo.IsDir() {
		fmt.Printf("Error: %s is not a directory\n", folderPath)
		os.Exit(1)
	}

	database := db.GetSQLDB()
	defer func() {
		_ = database.Close()
	}()

	isG := dataType == "g"

	// 폴더 내 모든 JSON 파일 처리
	processFolder(database, folderPath, isG)
}

func getOrCreateCategory(executor qrm.DB, parentID *int64, title string, sequence int64, metadata map[string]any) (*model.Categories, error) {
	var category model.Categories
	var err error
	if parentID == nil {
		err = postgres.SELECT(
			table.Categories.AllColumns,
		).FROM(
			table.Categories,
		).WHERE(
			table.Categories.ParentID.IS_NULL().AND(
				table.Categories.Title.EQ(postgres.String(title))).AND(
				table.Categories.DeletedAt.IS_NULL(),
			),
		).LIMIT(1).Query(executor, &category)
	} else {
		err = postgres.SELECT(
			table.Categories.AllColumns,
		).FROM(
			table.Categories,
		).WHERE(
			table.Categories.ParentID.EQ(postgres.Int64(*parentID)).AND(
				table.Categories.Title.EQ(postgres.String(title))),
		).LIMIT(1).Query(executor, &category)
	}
	if err != nil {
		if errors.Is(err, qrm.ErrNoRows) {
			// metadata가 nil이면 빈 object로 설정
			if metadata == nil {
				metadata = map[string]any{}
			}
			metadataBytes, _ := json.Marshal(metadata)
			metadataJSON := types.JSONB(metadataBytes)

			err = table.Categories.INSERT(
				table.Categories.Title,
				table.Categories.ParentID,
				table.Categories.Sequence,
				table.Categories.Metadata,
			).MODEL(model.Categories{
				Title:    title,
				ParentID: parentID,
				Sequence: sequence,
				Metadata: metadataJSON,
			}).RETURNING(table.Categories.AllColumns).Query(executor, &category)
			if err != nil {
				return nil, fmt.Errorf("failed to create category: %w", err)
			}
			return &category, nil
		}
		return nil, fmt.Errorf("failed to query category: %w", err)
	}

	return &category, nil
}
