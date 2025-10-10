package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type ExerciseGroup struct {
	ID                    int
	ProblemIDs            []int
	ProblemVideos         []bool
	Representative        int
	HasRepresentative     bool
	RepresentativeHasVideo bool
}

type RepresentativeInfo struct {
	ProblemID        int
	HasSolutionVideo bool
	SelectionReason  string
}

type CrossingResult struct {
	NewGroupID      int
	BaseGroupID     int
	ProblemIDs      []int
	CrossingGroups  []CrossingGroup
	Representative  int
	SelectionReason string
}

type CrossingGroup struct {
	ID           int
	Intersection []int
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run csv_processor.go <exercise_groups.csv> <pair_groups.json>")
		os.Exit(1)
	}

	csvFile := os.Args[1]
	jsonFile := os.Args[2]

	fmt.Println("Loading exercise groups from CSV...")
	groups, err := loadExerciseGroups(csvFile)
	if err != nil {
		fmt.Printf("Error loading CSV: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Loaded %d exercise groups\n", len(groups))

	fmt.Println("Building problem-to-groups index...")
	problemIndex := buildProblemIndex(groups)
	fmt.Printf("Indexed %d problems\n", len(problemIndex))

	fmt.Println("Loading new groups from JSON...")
	newGroups, err := loadNewGroups(jsonFile)
	if err != nil {
		fmt.Printf("Error loading JSON: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Loaded %d new groups\n", len(newGroups))

	fmt.Println("Processing groups...")
	results := processGroups(newGroups, problemIndex, groups)

	fmt.Println("Writing results...")
	err = writeResults(results, "csv_results.json")
	if err != nil {
		fmt.Printf("Error writing results: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Completed! Processed %d groups with %d crossings\n",
		len(newGroups), countCrossings(results))
}

func loadExerciseGroups(filename string) (map[int]ExerciseGroup, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	groups := make(map[int]ExerciseGroup)

	// Skip header
	_, err = reader.Read()
	if err != nil {
		return nil, err
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		groupID, err := strconv.Atoi(record[0])
		if err != nil {
			continue
		}

		var problemIDs []int
		if record[1] != "" {
			problemStrs := strings.Split(record[1], ",")
			for _, problemStr := range problemStrs {
				problemID, err := strconv.Atoi(strings.TrimSpace(problemStr))
				if err == nil {
					problemIDs = append(problemIDs, problemID)
				}
			}
		}

		var problemVideos []bool
		if len(record) > 2 && record[2] != "" {
			videoStrs := strings.Split(record[2], ",")
			for _, videoStr := range videoStrs {
				problemVideos = append(problemVideos, strings.TrimSpace(videoStr) == "true")
			}
		}

		var representative int
		var hasRepresentative bool
		var representativeHasVideo bool
		
		// representative_problem_id (record[3])
		if len(record) > 3 && record[3] != "" {
			representative, _ = strconv.Atoi(record[3])
		}
		
		// has_representative (record[4])
		if len(record) > 4 {
			hasRepresentative = record[4] == "true"
		}
		
		// representative_has_video (record[5])
		if len(record) > 5 {
			representativeHasVideo = record[5] == "true"
		}

		groups[groupID] = ExerciseGroup{
			ID:                    groupID,
			ProblemIDs:            problemIDs,
			ProblemVideos:         problemVideos,
			Representative:        representative,
			HasRepresentative:     hasRepresentative,
			RepresentativeHasVideo: representativeHasVideo,
		}
	}

	return groups, nil
}

func buildProblemIndex(groups map[int]ExerciseGroup) map[int][]int {
	index := make(map[int][]int)

	for _, group := range groups {
		for _, problemID := range group.ProblemIDs {
			index[problemID] = append(index[problemID], group.ID)
		}
	}

	return index
}

func loadNewGroups(filename string) ([][]int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var newGroups [][]int
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&newGroups)
	if err != nil {
		return nil, err
	}

	return newGroups, nil
}

func processGroups(newGroups [][]int, problemIndex map[int][]int, existingGroups map[int]ExerciseGroup) []CrossingResult {
	results := make([]CrossingResult, 0, len(newGroups))
	nextGroupID := getMaxGroupID(existingGroups) + 1

	// 병렬 처리를 위한 채널과 워커 풀
	const numWorkers = 8
	jobs := make(chan int, len(newGroups))
	resultsChan := make(chan CrossingResult, len(newGroups))

	var wg sync.WaitGroup

	// 워커 시작
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(jobs, resultsChan, &wg, newGroups, problemIndex, existingGroups, &nextGroupID)
	}

	// 작업 전송
	for i := range newGroups {
		jobs <- i
	}
	close(jobs)

	// 워커 완료 대기
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// 결과 수집
	for result := range resultsChan {
		results = append(results, result)
	}

	// 결과 정렬
	sort.Slice(results, func(i, j int) bool {
		return results[i].NewGroupID < results[j].NewGroupID
	})

	return results
}

func worker(jobs <-chan int, results chan<- CrossingResult, wg *sync.WaitGroup,
	newGroups [][]int, problemIndex map[int][]int, existingGroups map[int]ExerciseGroup, nextGroupID *int) {
	defer wg.Done()

	for i := range jobs {
		if i%1000 == 0 {
			fmt.Printf("Processing group %d/%d...\n", i+1, len(newGroups))
		}

		newGroup := newGroups[i]
		if len(newGroup) == 0 {
			continue
		}

		result := processGroup(newGroup, problemIndex, existingGroups, nextGroupID)
		results <- result
	}
}

func processGroup(newGroup []int, problemIndex map[int][]int, existingGroups map[int]ExerciseGroup, nextGroupID *int) CrossingResult {
	// 관련된 기존 그룹들 찾기
	relatedGroupIDs := make(map[int]bool)
	for _, problemID := range newGroup {
		if groupIDs, exists := problemIndex[problemID]; exists {
			for _, groupID := range groupIDs {
				relatedGroupIDs[groupID] = true
			}
		}
	}

	// 교차 그룹 찾기
	crossingGroups := []CrossingGroup{}
	var baseGroupID int

	for groupID := range relatedGroupIDs {
		if group, exists := existingGroups[groupID]; exists {
			intersection := findIntersection(newGroup, group.ProblemIDs)
			if len(intersection) > 0 {
				crossingGroups = append(crossingGroups, CrossingGroup{
					ID:           groupID,
					Intersection: intersection,
				})
				if groupID > baseGroupID {
					baseGroupID = groupID
				}
			}
		}
	}

	// 새 그룹 ID 할당
	newGroupID := *nextGroupID
	*nextGroupID++

	// 대표 문제 선정 로직
	representative, selectionReason := selectBestRepresentative(newGroup, crossingGroups, existingGroups)

	return CrossingResult{
		NewGroupID:      newGroupID,
		BaseGroupID:     baseGroupID,
		ProblemIDs:      newGroup,
		CrossingGroups:  crossingGroups,
		Representative:  representative,
		SelectionReason: selectionReason,
	}
}

func findIntersection(slice1, slice2 []int) []int {
	elementMap := make(map[int]bool)
	for _, v := range slice1 {
		elementMap[v] = true
	}

	var intersection []int
	seen := make(map[int]bool)
	for _, v := range slice2 {
		if elementMap[v] && !seen[v] {
			intersection = append(intersection, v)
			seen[v] = true
		}
	}

	return intersection
}

func getMaxGroupID(groups map[int]ExerciseGroup) int {
	maxID := 0
	for id := range groups {
		if id > maxID {
			maxID = id
		}
	}
	return maxID
}

func writeResults(results []CrossingResult, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(results)
}

func countCrossings(results []CrossingResult) int {
	count := 0
	for _, result := range results {
		if len(result.CrossingGroups) > 0 {
			count++
		}
	}
	return count
}

// selectBestRepresentative는 교차 그룹을 고려하여 최적의 대표 문제를 선택합니다
func selectBestRepresentative(newGroup []int, crossingGroups []CrossingGroup, existingGroups map[int]ExerciseGroup) (int, string) {
	if len(newGroup) == 0 {
		return 0, "빈 그룹"
	}

	// 기존 교차 그룹들에서 대표 문제들 수집
	var existingRepresentatives []RepresentativeInfo
	for _, crossing := range crossingGroups {
		if group, exists := existingGroups[crossing.ID]; exists {
			if group.HasRepresentative && group.Representative > 0 {
				existingRepresentatives = append(existingRepresentatives, RepresentativeInfo{
					ProblemID:        group.Representative,
					HasSolutionVideo: group.RepresentativeHasVideo,
					SelectionReason:  "기존 대표 문제",
				})
			}
		}
	}

	// 기존 대표 문제가 새 그룹에 포함되어 있다면 우선 선택
	// solution_video가 있는 기존 대표 문제를 먼저 찾아보기
	var candidateRepresentatives []RepresentativeInfo
	for _, rep := range existingRepresentatives {
		for _, problemID := range newGroup {
			if problemID == rep.ProblemID {
				candidateRepresentatives = append(candidateRepresentatives, rep)
			}
		}
	}
	
	if len(candidateRepresentatives) > 0 {
		// solution_video가 있는 것 우선 선택
		for _, rep := range candidateRepresentatives {
			if rep.HasSolutionVideo {
				return rep.ProblemID, "기존 대표 문제가 새 그룹에 포함됨 (비디오 있음)"
			}
		}
		// 비디오가 없어도 기존 대표 문제는 우선
		return candidateRepresentatives[0].ProblemID, "기존 대표 문제가 새 그룹에 포함됨"
	}

	// 기존 대표 문제가 없거나 새 그룹에 포함되지 않은 경우
	// solution_video_id가 있는 문제 우선 선택, 그 다음 가장 높은 ID 선택
	return selectBestFromNewGroup(newGroup, crossingGroups, existingRepresentatives)
}

// selectBestFromNewGroup은 새 그룹에서 최적의 대표 문제를 선택합니다
func selectBestFromNewGroup(newGroup []int, crossingGroups []CrossingGroup, existingRepresentatives []RepresentativeInfo) (int, string) {
	// 새 그룹의 문제들에 대한 비디오 정보는 JSON 파일에 없으므로
	// 가장 높은 ID를 선택하고, 실제 비디오 우선순위는 DB 업로드 시 처리
	highest := newGroup[0]
	for _, problemID := range newGroup {
		if problemID > highest {
			highest = problemID
		}
	}

	var reason string
	if len(crossingGroups) == 0 {
		reason = "교차 없음 - 가장 높은 ID 선택"
	} else if len(existingRepresentatives) == 0 {
		reason = "기존 대표 문제가 없어서 가장 높은 ID 선택"
	} else {
		reason = "기존 대표 문제가 새 그룹에 포함되지 않아서 가장 높은 ID 선택"
	}

	return highest, reason
}
