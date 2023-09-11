package db

import (
	"database/sql"
	"github.com/citizenwallet/indexer/pkg/indexer"
	"github.com/stretchr/testify/require"
	"math/big"
	"testing"
	"time"

	_ "github.com/proullon/ramsql/driver"
)

func TestDBInit(t *testing.T) {
	fOpen := func(host string) (*sql.DB, error) {
		return sql.Open("ramsql", "TestDBInit")
	}
	fExists := func(db *sql.DB, tableName string) (bool, error) {
		// for testing purposes we can always assume tables aren't there ...?
		return false, nil
	}
	testDB, err := newDBInternal(big.NewInt(0), "host", "rhost", false, fOpen, fExists)
	require.NoError(t, err)
	require.NotNil(t, testDB)

	err = testDB.EventDB.AddEvent("someContract", indexer.EventStateQueued, 0, 1, indexer.ERC20, "foo", "bar")
	require.NoError(t, err)

	checkEvents, err := testDB.EventDB.GetEvents()
	require.NoError(t, err)
	require.Equal(t, 1, len(checkEvents))
	require.Equal(t, int64(0), checkEvents[0].StartBlock)

	tdb, err := testDB.AddTransferDB("someContract")
	require.NoError(t, err)
	require.NotNil(t, tdb)

	// TODO: awkward that the table is not created automatically ...?
	err = tdb.CreateTransferTable()
	require.NoError(t, err)

	err = tdb.CreateTransferTableIndexes()
	require.NoError(t, err)

	testTrans := indexer.Transfer{
		Hash:      "someHash",
		TxHash:    "someTxHash",
		TokenID:   0,
		CreatedAt: time.Now(),
		FromTo:    "fromTo",
		From:      "from",
		To:        "To",
		Nonce:     0,
		Value:     big.NewInt(1_000_000_000),
		Data:      nil,
		Status:    indexer.TransferStatusPending,
	}
	err = tdb.AddTransfer(&testTrans)
	require.NoError(t, err)

	exists, err := tdb.TransferExists("someTxHash")
	require.NoError(t, err)
	require.True(t, exists)

	testTrans.TxHash = "someNewTxHash"
	err = tdb.AddTransfers([]*indexer.Transfer{&testTrans})
	require.NoError(t, err)

	exists, err = tdb.TransferExists("someNewTxHash")
	require.NoError(t, err)
	require.True(t, exists)
}
