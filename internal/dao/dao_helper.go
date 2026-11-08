package dao

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/haierkeys/fast-note-sync-service/pkg/fileurl"
)

// getContentPath gets the content storage path
// getContentPath 获取内容存储路径
func (d *Dao) GetNoteFolderPath(uid int64, noteID int64) string {
	return filepath.Join("storage", "vault", fmt.Sprintf("u_%d", uid), "note", fmt.Sprintf("n_%d", noteID))
}

// getSettingFolderPath gets the setting storage path
// getSettingFolderPath 获取配置存储路径
func (d *Dao) GetSettingFolderPath(uid int64, settingID int64) string {
	return filepath.Join("storage", "vault", fmt.Sprintf("u_%d", uid), "setting", fmt.Sprintf("s_%d", settingID))
}

// GetFileFolderPath gets the file folder path
// GetFileFolderPath 获取文件目录路径
func (d *Dao) GetFileFolderPath(uid int64, fileID int64) string {
	return filepath.Join("storage", "vault", fmt.Sprintf("u_%d", uid), "file", fmt.Sprintf("f_%d", fileID))
}

// GetNoteHistoryFolderPath gets the note history storage path
// GetNoteHistoryFolderPath 获取笔记历史存储路径
func (d *Dao) GetNoteHistoryFolderPath(uid int64, historyID int64) string {
	return filepath.Join("storage", "vault", fmt.Sprintf("u_%d", uid), "history", fmt.Sprintf("h_%d", historyID))
}

// saveContentToFile saves content to a file
// saveContentToFile 保存内容到文件
func (d *Dao) SaveContentToFile(folderPath string, fileName string, content string) error {
	if err := os.MkdirAll(folderPath, 0755); err != nil {
		return err
	}
	return AtomicWriteFile(folderPath, fileName, []byte(content))
}

// AtomicWriteFile writes data to dir/name via a same-directory temp file: write + fsync + rename
// + fsync(dir). Readers see either the complete old file or the complete new one; a crash
// mid-write leaves the old content intact (lucky-v2).
// AtomicWriteFile 同目录临时文件写入 + fsync + rename + fsync 目录；中断时旧内容完整。
func AtomicWriteFile(dir, name string, data []byte) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	fail := func(e error) error {
		f.Close()
		os.Remove(tmp)
		return e
	}
	if _, err := f.Write(data); err != nil {
		return fail(err)
	}
	if err := f.Chmod(0644); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		os.Remove(tmp)
		return err
	}
	if df, err := os.Open(dir); err == nil {
		_ = df.Sync()
		df.Close()
	}
	return nil
}

// loadContentFromFile loads content from a file
// loadContentFromFile 从文件加载内容
// Return values: content, whether it exists, error
// 返回值: 内容, 是否存在, 错误
func (d *Dao) LoadContentFromFile(folderPath string, fileName string) (string, bool, error) {
	filePath := filepath.Join(folderPath, fileName)
	content, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", false, err
	}
	return string(content), true, nil
}

// removeContentFolder removes the content folder
// removeContentFolder 删除内容文件夹
func (d *Dao) RemoveContentFolder(folderPath string) error {
	if fileurl.IsExist(folderPath) {
		return os.RemoveAll(folderPath)
	}
	return nil
}
