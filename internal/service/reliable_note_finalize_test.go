package service

import (
	"context"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	domainmocks "github.com/haierkeys/fast-note-sync-service/internal/domain/mocks"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestReliableRenameFinalizationPreservesSharesAndIsIdempotent(t *testing.T) {
	repo := new(domainmocks.MockNoteRepository)
	shares := new(domainmocks.MockUserShareRepository)
	history := new(domainmocks.MockNoteHistoryRepository)
	old := &domain.Note{ID: 1, VaultID: 1, Path: "old.md", Content: "manuscript", UpdatedTimestamp: 10, Action: domain.NoteActionCreate}
	target := &domain.Note{ID: 2, VaultID: 1, Path: "new.md", Content: "manuscript", Action: domain.NoteActionCreate}
	repo.On("GetByID", mock.Anything, int64(1), int64(1)).Return(old, nil)
	repo.On("GetByID", mock.Anything, int64(2), int64(1)).Return(target, nil)
	repo.On("UpdateDelete", mock.Anything, mock.AnythingOfType("*domain.Note"), int64(1)).Run(func(a mock.Arguments) { a.Get(1).(*domain.Note).UpdatedTimestamp = 11 }).Return(nil).Once()
	repo.On("UpdateSnapshot", mock.Anything, "", "", int64(0), int64(2), int64(1)).Return(nil)
	shares.On("MigrateResID", mock.Anything, int64(1), int64(1), int64(2)).Return(nil)
	history.On("Migrate", mock.Anything, int64(1), int64(2), int64(1)).Return(nil)
	repo.On("CountSizeSum", mock.Anything, int64(1), int64(1)).Return(&domain.CountSizeResult{}, nil).Maybe()
	svc := NewNoteService(nil, repo, nil, nil, shares, history, nil, nil, nil, nil, nil, nil).(*noteService)
	oldDTO := &dto.NoteDTO{ID: 1, Path: "old.md", Content: "manuscript", UpdatedTimestamp: 10}
	newDTO := &dto.NoteDTO{ID: 2, Path: "new.md", Content: "manuscript"}
	require.NoError(t, svc.FinalizeReliableRename(context.Background(), 1, oldDTO, newDTO))
	require.NoError(t, svc.FinalizeReliableRename(context.Background(), 1, oldDTO, newDTO))
	require.Equal(t, int64(11), oldDTO.UpdatedTimestamp)
	require.Equal(t, domain.NoteActionDelete, old.Action)
	require.Equal(t, int64(1), old.Rename)
	repo.AssertNumberOfCalls(t, "UpdateDelete", 1)
	shares.AssertNotCalled(t, "UpdateStatusByRes", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	shares.AssertNumberOfCalls(t, "MigrateResID", 2)
	history.AssertNumberOfCalls(t, "Migrate", 2)
}

func TestReliableRenameFinalizationRejectsChangedSource(t *testing.T) {
	repo := new(domainmocks.MockNoteRepository)
	repo.On("GetByID", mock.Anything, int64(1), int64(1)).Return(&domain.Note{ID: 1, Path: "old.md", Content: "new user edit", UpdatedTimestamp: 12, Action: domain.NoteActionCreate}, nil)
	repo.On("GetByID", mock.Anything, int64(2), int64(1)).Return(&domain.Note{ID: 2, Path: "new.md", Content: "before", Action: domain.NoteActionCreate}, nil)
	svc := NewNoteService(nil, repo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil).(*noteService)
	err := svc.FinalizeReliableRename(context.Background(), 1, &dto.NoteDTO{ID: 1, Path: "old.md", Content: "before", UpdatedTimestamp: 10}, &dto.NoteDTO{ID: 2, Path: "new.md", Content: "before"})
	require.ErrorContains(t, err, "source changed")
	repo.AssertNotCalled(t, "UpdateDelete", mock.Anything, mock.Anything, mock.Anything)
}
