package adminsession

import (
	"context"
	"errors"
	"sync"
	"time"
)

type MemoryStore struct {
	mu       sync.Mutex
	sessions map[string]Session
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{sessions: make(map[string]Session)}
}

func (s *MemoryStore) Create(ctx context.Context, session *Session) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if session == nil || !session.Valid() {
		return ErrInvalidSession
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.sessions[session.SessionIDHash]; exists {
		return errors.New("admin session already exists")
	}
	s.sessions[session.SessionIDHash] = *session
	return nil
}

func (s *MemoryStore) GetActive(ctx context.Context, hash string, now time.Time) (*Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	session, exists := s.sessions[hash]
	if !exists || !session.ExpiresAt.After(now) {
		return nil, nil
	}
	return &session, nil
}

func (s *MemoryStore) ExtendActive(ctx context.Context, hash string, now, expiresAt time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	session, exists := s.sessions[hash]
	if !exists || !session.ExpiresAt.After(now) {
		return false, nil
	}
	if !expiresAt.After(now) {
		return false, ErrInvalidSession
	}
	if expiresAt.After(session.ExpiresAt) {
		session.ExpiresAt = expiresAt
		s.sessions[hash] = session
	}
	return true, nil
}

func (s *MemoryStore) Delete(ctx context.Context, hash string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, hash)
	return nil
}

func (s *MemoryStore) DeleteExpired(ctx context.Context, now time.Time, limit int) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var deleted int64
	for hash, session := range s.sessions {
		if deleted >= int64(limit) {
			break
		}
		if !session.ExpiresAt.After(now) {
			delete(s.sessions, hash)
			deleted++
		}
	}
	return deleted, nil
}
