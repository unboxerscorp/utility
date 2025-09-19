package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

func SafeOpenFile(filename string) (*os.File, error) {
	// 상대 경로 공격 방지
	if strings.Contains(filename, "..") {
		return nil, errors.New("invalid file path: relative path not allowed")
	}

	// 절대 경로로 정리
	cleanPath := filepath.Clean(filename)

	return os.Open(cleanPath)
}

// ValidateTempPath 임시 파일 경로 검증 - /tmp 디렉토리만 허용
func ValidateTempPath(filename string) (string, error) {
	// 상대 경로 공격 방지
	if strings.Contains(filename, "..") {
		return "", errors.New("invalid file path: relative path not allowed")
	}

	// 절대 경로로 정리
	cleanPath := filepath.Clean(filename)

	// /tmp 디렉토리만 허용
	if !strings.HasPrefix(cleanPath, "/tmp/") {
		return "", errors.New("invalid temp file path: only /tmp directory allowed")
	}

	return cleanPath, nil
}
