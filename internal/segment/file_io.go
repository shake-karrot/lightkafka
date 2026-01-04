package segment

import (
	"fmt"
	"os"
	"path/filepath"
)

func RemoveFiles(dir string, baseOffset int64) error {
	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	indexPath := filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset))

	if err := os.Remove(logPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove log file: %w", err)
	}

	if err := os.Remove(indexPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove index file: %w", err)
	}

	return nil
}
