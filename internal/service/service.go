// Package service implements the business logic layer
// Package service 实现业务逻辑层
// This file retains package-level channel and message type definitions
// 本文件保留包级别的通道和消息类型定义
package service

import "sync/atomic"

// NoteMigrateChannel migration task channel
// NoteMigrateChannel 迁移任务通道
var NoteMigrateChannel = make(chan NoteMigrateMsg, 1000)

// NoteMigrateMsg note migration message
// NoteMigrateMsg 笔记迁移消息
type NoteMigrateMsg struct {
	OldNoteID int64 // Old note ID // 旧笔记 ID
	NewNoteID int64 // New note ID // 新笔记 ID
	UID       int64 // User ID // 用户 ID
}

// NoteHistoryMsg note history record delayed processing message
// NoteHistoryMsg 笔记历史记录延时处理消息
type NoteHistoryMsg struct {
	NoteID int64 // Note ID // 笔记 ID
	UID    int64 // User ID // 用户 ID
}

// NoteHistoryChannel delayed task channel, background task will listen to this channel
// NoteHistoryChannel 延时任务通道，后台 task 会监听此通道
var NoteHistoryChannel = make(chan NoteHistoryMsg, 1000)

// NoteHistoryDelayPush pushes note to the delayed processing queue
// NoteHistoryDelayPush 将笔记推送至延时处理队列
func NoteHistoryDelayPush(noteID int64, uid int64) {
	// lucky-v2: never block a writer (it may hold the note lock) on a full history queue;
	// history is best-effort, so drop and count instead.
	select {
	case NoteHistoryChannel <- NoteHistoryMsg{NoteID: noteID, UID: uid}:
	default:
		HistoryDropped.Add(1)
	}
}

// HistoryDropped counts history pushes dropped because the queue was full (health counter).
var HistoryDropped atomic.Int64
