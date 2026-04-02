package handlers

import (
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/mongo"
)

func TestIsDuplicateKeyError_True(t *testing.T) {
	bwe := mongo.BulkWriteException{
		WriteErrors: []mongo.BulkWriteError{
			{WriteError: mongo.WriteError{Code: 11000, Message: "duplicate key"}},
		},
	}
	if !mongo.IsDuplicateKeyError(bwe) {
		t.Fatal("expected IsDuplicateKeyError to return true for code 11000")
	}
}

func TestIsDuplicateKeyError_False(t *testing.T) {
	err := errors.New("network timeout")
	if mongo.IsDuplicateKeyError(err) {
		t.Fatal("expected IsDuplicateKeyError to return false for generic error")
	}
}
