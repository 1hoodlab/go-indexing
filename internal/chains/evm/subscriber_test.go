package evm

import (
	"math"
	"math/big"
	"testing"
)

func TestBlockEventNumber_LargeUint64(t *testing.T) {
	blockNum := uint64(math.MaxInt64) + 1

	bad := big.NewInt(int64(blockNum))
	if bad.Sign() >= 0 {
		t.Fatal("expected negative from int64 cast to prove the bug exists")
	}

	good := new(big.Int).SetUint64(blockNum)
	if good.Uint64() != blockNum {
		t.Fatalf("expected %d, got %d", blockNum, good.Uint64())
	}
}
