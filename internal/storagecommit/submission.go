package storagecommit

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/strahe/synapse-go/storage"
)

const submissionEnvelopeVersion = 1

type submissionEnvelope struct {
	Version    int                      `json:"version"`
	Submission storage.CommitSubmission `json:"submission"`
}

func EncodeSubmission(submission storage.CommitSubmission) (string, error) {
	payload, err := json.Marshal(submissionEnvelope{
		Version:    submissionEnvelopeVersion,
		Submission: submission,
	})
	if err != nil {
		return "", fmt.Errorf("encoding storage commit submission: %w", err)
	}
	return string(payload), nil
}

func DecodeSubmission(value string) (storage.CommitSubmission, error) {
	var envelope submissionEnvelope
	decoder := json.NewDecoder(bytes.NewBufferString(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return storage.CommitSubmission{}, fmt.Errorf("decoding storage commit submission: %w", err)
	}
	if err := ensureSubmissionJSONEOF(decoder); err != nil {
		return storage.CommitSubmission{}, err
	}
	if envelope.Version != submissionEnvelopeVersion {
		return storage.CommitSubmission{}, fmt.Errorf("unsupported storage commit submission version %d", envelope.Version)
	}
	return envelope.Submission, nil
}

func ensureSubmissionJSONEOF(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("decoding trailing storage commit submission data: %w", err)
	}
	return errors.New("storage commit submission contains trailing data")
}
