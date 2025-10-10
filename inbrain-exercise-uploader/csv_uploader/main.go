package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	_ "github.com/lib/pq"
)

type CrossingResult struct {
	NewGroupID       int           `json:"NewGroupID"`
	BaseGroupID      int           `json:"BaseGroupID"`
	ProblemIDs       []int         `json:"ProblemIDs"`
	CrossingGroups   []CrossingGroup `json:"CrossingGroups"`
	Representative   int           `json:"Representative"`
	SelectionReason  string        `json:"SelectionReason"`
}

type CrossingGroup struct {
	ID           int   `json:"ID"`
	Intersection []int `json:"Intersection"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run csv_uploader.go <csv_results.json> [-host=localhost] [-port=5433] [-db=postgres]")
		os.Exit(1)
	}

	resultsFile := os.Args[1]
	
	// 기본값 설정
	dbHost := "localhost"
	dbPort := "5433"
	dbName := "postgres"
	
	// 플래그 파싱
	for _, arg := range os.Args[2:] {
		if strings.HasPrefix(arg, "-host=") {
			dbHost = strings.TrimPrefix(arg, "-host=")
		} else if strings.HasPrefix(arg, "-port=") {
			dbPort = strings.TrimPrefix(arg, "-port=")
		} else if strings.HasPrefix(arg, "-db=") {
			dbName = strings.TrimPrefix(arg, "-db=")
		}
	}

	fmt.Printf("Connecting to database: host=%s port=%s dbname=%s\n", dbHost, dbPort, dbName)

	// DB 연결
	database, err := connectDB(dbHost, dbPort, dbName)
	if err != nil {
		fmt.Printf("Error connecting to database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	// 결과 로드
	fmt.Println("Loading results from JSON...")
	results, err := loadResults(resultsFile)
	if err != nil {
		fmt.Printf("Error loading results: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Loaded %d results\n", len(results))

	// DB에 업로드
	fmt.Println("Uploading to database...")
	err = uploadResults(database, results)
	if err != nil {
		fmt.Printf("Error uploading results: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Upload completed successfully!")
}

func connectDB(host, port, dbName string) (*sql.DB, error) {
	dbUser := "app_user"
	
	// 로컬 DB인 경우 고정 패스워드 사용
	var dbPassword string
	if host == "localhost" {
		dbPassword = "localpass123"
	} else {
		// AWS Secrets Manager에서 패스워드 가져오기
		ctx := context.Background()
		var err error
		dbPassword, err = getDBPasswordFromSecretsManager(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get DB password: %w", err)
		}
	}

	dbDSN := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
		host, port, dbName, dbUser, dbPassword)

	database, err := sql.Open("postgres", dbDSN)
	if err != nil {
		return nil, err
	}

	// 연결 테스트
	err = database.Ping()
	if err != nil {
		return nil, err
	}

	return database, nil
}

func getDBPasswordFromSecretsManager(ctx context.Context) (string, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("ap-northeast-2"))
	if err != nil {
		return "", fmt.Errorf("failed to load AWS config: %w", err)
	}

	svc := secretsmanager.NewFromConfig(cfg)
	secretName := "base-inbrain/production/DB_PASSWORD"
	result, err := svc.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretName),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get secret value: %w", err)
	}

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

func loadResults(filename string) ([]CrossingResult, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var results []CrossingResult
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&results)
	if err != nil {
		return nil, err
	}

	return results, nil
}

func uploadResults(database *sql.DB, results []CrossingResult) error {
	ctx := context.Background()
	
	// 배치 처리를 위한 트랜잭션
	const batchSize = 1000
	
	for i := 0; i < len(results); i += batchSize {
		end := i + batchSize
		if end > len(results) {
			end = len(results)
		}
		
		batch := results[i:end]
		err := processBatch(ctx, database, batch)
		if err != nil {
			return fmt.Errorf("failed to process batch %d-%d: %w", i, end-1, err)
		}
		
		fmt.Printf("Processed batch %d-%d (%d/%d)\n", i, end-1, end, len(results))
	}
	
	return nil
}

func processBatch(ctx context.Context, database *sql.DB, batch []CrossingResult) error {
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, result := range batch {
		err = processResult(ctx, tx, result)
		if err != nil {
			return fmt.Errorf("failed to process result %d: %w", result.NewGroupID, err)
		}
	}

	return tx.Commit()
}

func processResult(ctx context.Context, tx *sql.Tx, result CrossingResult) error {
	if len(result.ProblemIDs) == 0 {
		return nil
	}

	// 존재하는 문제의 카테고리 ID 가져오기 (존재하지 않는 문제들은 건너뛰기)
	var categoryID int64
	var err error
	for _, problemID := range result.ProblemIDs {
		categoryID, err = getCategoryIDFromProblem(ctx, tx, problemID)
		if err != nil {
			return err
		}
		if categoryID != 0 {
			break // 존재하는 문제를 찾으면 중단
		}
	}
	
	// 모든 문제가 존재하지 않으면 스킵
	if categoryID == 0 {
		fmt.Printf("Warning: Skipping group %d - no valid problems found\n", result.NewGroupID)
		return nil
	}

	// 새 exercise_group 생성
	newGroupID, err := createExerciseGroup(ctx, tx, categoryID)
	if err != nil {
		return err
	}

	// 기존 교차 그룹들을 deleted로 마킹
	for _, crossingGroup := range result.CrossingGroups {
		err = markGroupAsDeleted(ctx, tx, int64(crossingGroup.ID))
		if err != nil {
			return err
		}
	}

	// 존재하는 문제들만 새 그룹에 매핑
	err = updateExercisesGroup(ctx, tx, result.ProblemIDs, newGroupID)
	if err != nil {
		return err
	}

	// 올바른 대표 문제 선정 및 설정
	representative, err := selectBestRepresentative(ctx, tx, result.ProblemIDs, result.CrossingGroups)
	if err != nil {
		return err
	}
	
	if representative != 0 {
		err = setRepresentativeExercise(ctx, tx, representative, newGroupID)
		if err != nil {
			return err
		}
	}

	return nil
}

func getCategoryIDFromProblem(ctx context.Context, tx *sql.Tx, problemID int) (int64, error) {
	query := `SELECT category_id FROM exercises WHERE metadata->>'mathflatProblemId' = $1 AND deleted_at IS NULL LIMIT 1`
	var categoryID int64
	err := tx.QueryRowContext(ctx, query, strconv.Itoa(problemID)).Scan(&categoryID)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil // 문제가 존재하지 않으면 0 반환
		}
		return 0, fmt.Errorf("failed to get category ID from problem %d: %w", problemID, err)
	}
	return categoryID, nil
}

func createExerciseGroup(ctx context.Context, tx *sql.Tx, categoryID int64) (int64, error) {
	query := `INSERT INTO exercise_groups (category_id, metadata, created_at, updated_at)
			  VALUES ($1, '{}', NOW(), NOW()) RETURNING id`
	var groupID int64
	err := tx.QueryRowContext(ctx, query, categoryID).Scan(&groupID)
	if err != nil {
		return 0, fmt.Errorf("failed to create exercise group: %w", err)
	}
	return groupID, nil
}

func markGroupAsDeleted(ctx context.Context, tx *sql.Tx, groupID int64) error {
	query := `UPDATE exercise_groups SET deleted_at = NOW(), updated_at = NOW() WHERE id = $1`
	_, err := tx.ExecContext(ctx, query, groupID)
	if err != nil {
		return fmt.Errorf("failed to mark group %d as deleted: %w", groupID, err)
	}
	return nil
}

func updateExercisesGroup(ctx context.Context, tx *sql.Tx, problemIDs []int, newGroupID int64) error {
	for _, problemID := range problemIDs {
		query := `UPDATE exercises SET exercise_group_id = $1, updated_at = NOW()
				  WHERE metadata->>'mathflatProblemId' = $2 AND deleted_at IS NULL`
		result, err := tx.ExecContext(ctx, query, newGroupID, strconv.Itoa(problemID))
		if err != nil {
			return fmt.Errorf("failed to update exercise %d group: %w", problemID, err)
		}
		// 존재하지 않는 문제는 조용히 무시 (rowsAffected가 0이어도 에러 없음)
		_ = result
	}
	return nil
}

func setRepresentativeExercise(ctx context.Context, tx *sql.Tx, problemID int, groupID int64) error {
	// 먼저 해당 그룹의 모든 is_representative를 false로 설정
	query := `UPDATE exercises SET is_representative = false, updated_at = NOW()
			  WHERE exercise_group_id = $1 AND deleted_at IS NULL`
	_, err := tx.ExecContext(ctx, query, groupID)
	if err != nil {
		return fmt.Errorf("failed to clear representative flags: %w", err)
	}

	// 선택된 문제를 대표로 설정 (존재하는 경우에만)
	query = `UPDATE exercises SET is_representative = true, updated_at = NOW()
			 WHERE metadata->>'mathflatProblemId' = $1 AND exercise_group_id = $2 AND deleted_at IS NULL`
	result, err := tx.ExecContext(ctx, query, strconv.Itoa(problemID), groupID)
	if err != nil {
		return fmt.Errorf("failed to set representative exercise %d: %w", problemID, err)
	}
	// 존재하지 않는 문제는 조용히 무시
	_ = result

	return nil
}

// selectBestRepresentative는 교차 그룹을 고려하여 최적의 대표 문제를 선택합니다
func selectBestRepresentative(ctx context.Context, tx *sql.Tx, problemIDs []int, crossingGroups []CrossingGroup) (int, error) {
	if len(problemIDs) == 0 {
		return 0, nil
	}

	// 교차 그룹이 없으면 가장 높은 ID 선택
	if len(crossingGroups) == 0 {
		highest := problemIDs[0]
		for _, id := range problemIDs {
			if id > highest {
				highest = id
			}
		}
		return highest, nil
	}

	// 교차 그룹들의 기존 대표 문제들 수집
	var existingRepresentatives []RepresentativeInfo
	for _, crossing := range crossingGroups {
		query := `SELECT id, CAST(metadata->>'mathflatProblemId' AS INTEGER),
				         CASE WHEN solution_video_id IS NOT NULL THEN true ELSE false END as has_solution_video
				  FROM exercises
				  WHERE exercise_group_id = $1 AND is_representative = true AND deleted_at IS NULL`
		
		rows, err := tx.QueryContext(ctx, query, crossing.ID)
		if err != nil {
			return 0, fmt.Errorf("failed to query existing representatives: %w", err)
		}
		
		for rows.Next() {
			var rep RepresentativeInfo
			err := rows.Scan(&rep.ExerciseID, &rep.ProblemID, &rep.HasSolutionVideo)
			if err != nil {
				rows.Close()
				return 0, fmt.Errorf("failed to scan representative: %w", err)
			}
			existingRepresentatives = append(existingRepresentatives, rep)
		}
		rows.Close()
	}

	// 기존 대표 문제가 새 그룹에 포함되어 있다면 우선 선택
	for _, rep := range existingRepresentatives {
		for _, problemID := range problemIDs {
			if problemID == rep.ProblemID {
				// solution_video가 있는 기존 대표 문제를 우선
				if rep.HasSolutionVideo {
					return rep.ProblemID, nil
				}
			}
		}
	}
	
	// solution_video가 없는 기존 대표 문제라도 포함되어 있다면 선택
	for _, rep := range existingRepresentatives {
		for _, problemID := range problemIDs {
			if problemID == rep.ProblemID {
				return rep.ProblemID, nil
			}
		}
	}

	// 기존 대표 문제가 포함되지 않은 경우, 새 그룹에서 solution_video가 있는 문제 우선 선택
	for _, problemID := range problemIDs {
		query := `SELECT CASE WHEN solution_video_id IS NOT NULL THEN true ELSE false END
				  FROM exercises
				  WHERE metadata->>'mathflatProblemId' = $1 AND deleted_at IS NULL LIMIT 1`
		
		var hasVideo bool
		err := tx.QueryRowContext(ctx, query, strconv.Itoa(problemID)).Scan(&hasVideo)
		if err == nil && hasVideo {
			return problemID, nil
		}
	}

	// solution_video가 없다면 가장 높은 ID 선택
	highest := problemIDs[0]
	for _, id := range problemIDs {
		if id > highest {
			highest = id
		}
	}
	return highest, nil
}

type RepresentativeInfo struct {
	ExerciseID       int64
	ProblemID        int
	HasSolutionVideo bool
	SelectionReason  string
}