package migrator

import (
	"fmt"
	"os"
	"path/filepath"
)

func listFilesFilter(root, pattern string) ([]string, error) {
	var matches []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if matched, err := filepath.Match(pattern, filepath.Base(path)); err != nil {
			return err
		} else if matched {
			matches = append(matches, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return matches, nil
}

func sortArray(arr []int) []int {
	for i := 0; i <= len(arr)-1; i++ {
		for j := 0; j < len(arr)-1-i; j++ {
			if arr[j] > arr[j+1] {
				arr[j], arr[j+1] = arr[j+1], arr[j]
			}
		}
	}
	return arr
}

func checkFolders(sqlFolder string) bool {
	sqlFolderInfo, err := os.Stat(sqlFolder)
	if os.IsNotExist(err) {
		fmt.Println("SQL folder provided does not exist")
		return false
	}

	if !sqlFolderInfo.IsDir() {
		fmt.Println("SQL folder provided is not a directory")
		return false
	}

	schemaFileInfo, err := os.Stat(sqlFolder + schemaFileName)
	if os.IsNotExist(err) {
		fmt.Println("Schema file (schema.sql) inside SQL folder provided does not exist")
		return false
	}

	if schemaFileInfo.IsDir() {
		fmt.Println("Schema directory inside SQL folder is not supported")
		return false
	}

	migrationsFolderInfo, err := os.Stat(sqlFolder + migrationsFolderName)
	if os.IsNotExist(err) {
		fmt.Println("Migrations (migrations) directory inside SQL folder does not exist")
		return false
	}

	if !migrationsFolderInfo.IsDir() {
		fmt.Println("Migrations (migrations) directory inside SQL folder is not a directory")
		return false
	}

	return true
}
