package db

import (
	"fmt"
	"log"
	"math/big"
	"strings"
	"sync"

	"database/sql"

	_ "github.com/lib/pq"
)

type DB struct {
	chainID *big.Int
	mu      sync.Mutex
	db      *sql.DB
	rdb     *sql.DB

	EventDB    *EventDB
	TransferDB map[string]*TransferDB

	fExists func(db *sql.DB, tableName string) (bool, error)

	withConflict bool
}

func newDBInternal(chainID *big.Int, host, rhost string, withConflict bool, fOpen func(string) (*sql.DB, error), fExists func(db *sql.DB, tableName string) (bool, error)) (*DB, error) {
	db, err := fOpen(host)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	rdb, err := fOpen(rhost)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	err = rdb.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	evname := chainID.String()

	eventDB, err := NewEventDB(db, rdb, evname, withConflict)
	if err != nil {
		return nil, err
	}

	d := &DB{
		chainID:      chainID,
		db:           db,
		rdb:          rdb,
		EventDB:      eventDB,
		fExists:      fExists,
		withConflict: withConflict,
	}

	// check if db exists before opening, since we use rwc mode
	exists, err := d.EventTableExists(evname)
	if err != nil {
		return nil, err
	}

	if !exists {
		// create table
		err = eventDB.CreateEventsTable(evname)
		if err != nil {
			return nil, err
		}

		// create indexes
		err = eventDB.CreateEventsTableIndexes(evname)
		if err != nil {
			return nil, err
		}
	}

	txdb := map[string]*TransferDB{}

	evs, err := eventDB.GetEvents()
	if err != nil {
		return nil, err
	}

	for _, ev := range evs {
		name := d.TransferName(ev.Contract)
		log.Default().Println("creating transfer db for: ", name)

		txdb[name], err = NewTransferDB(db, rdb, name, withConflict)
		if err != nil {
			return nil, err
		}

		// check if db exists before opening, since we use rwc mode
		exists, err := d.TransferTableExists(name)
		if err != nil {
			return nil, err
		}

		if !exists {
			// create table
			err = txdb[name].CreateTransferTable()
			if err != nil {
				return nil, err
			}

			// create indexes
			err = txdb[name].CreateTransferTableIndexes()
			if err != nil {
				return nil, err
			}
		}
	}

	d.TransferDB = txdb

	return d, nil
}

// NewDB instantiates a new DB
func NewDB(chainID *big.Int, username, password, name, hostIn, rhostIn string) (*DB, error) {

	fExists := func(db *sql.DB, tableName string) (bool, error) {
		var exists bool
		err := db.QueryRow(fmt.Sprintf(`
		   SELECT EXISTS (
		       SELECT 1
		       FROM information_schema.tables
		       WHERE table_schema = 'public'
		       AND table_name = '%s'
		   );
		   `, tableName)).Scan(&exists)
		if err != nil {
			return false, err
		}
		return exists, nil
	}

	fOpen := func(host string) (*sql.DB, error) {
		connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=5432 sslmode=disable", username, password, name, host)
		return sql.Open("postgres", connStr)
	}

	return newDBInternal(chainID, hostIn, rhostIn, true, fOpen, fExists)
}

// EventTableExists checks if a table exists in the database
func (db *DB) EventTableExists(suffix string) (bool, error) {
	return db.fExists(db.db, fmt.Sprintf("t_events_%s", suffix))
}

// TransferTableExists checks if a table exists in the database
func (db *DB) TransferTableExists(suffix string) (bool, error) {
	return db.fExists(db.db, fmt.Sprintf("t_transfers_%s", suffix))
}

// TransferName returns the name of the transfer db for the given contract
func (d *DB) TransferName(contract string) string {
	return fmt.Sprintf("%v_%s", d.chainID, strings.ToLower(contract))
}

// GetTransferDB returns true if the transfer db for the given contract exists, returns the db if it exists
func (d *DB) GetTransferDB(contract string) (*TransferDB, bool) {
	name := d.TransferName(contract)
	d.mu.Lock()
	defer d.mu.Unlock()
	txdb, ok := d.TransferDB[name]
	if !ok {
		return nil, false
	}
	return txdb, true
}

// AddTransferDB adds a new transfer db for the given contract
func (d *DB) AddTransferDB(contract string) (*TransferDB, error) {
	name := d.TransferName(contract)
	d.mu.Lock()
	defer d.mu.Unlock()
	if txdb, ok := d.TransferDB[name]; ok {
		return txdb, nil
	}
	txdb, err := NewTransferDB(d.db, d.rdb, name, d.withConflict)
	if err != nil {
		return nil, err
	}
	d.TransferDB[name] = txdb
	return txdb, nil
}

// Close closes the db and all its transfer dbs
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i, txdb := range d.TransferDB {
		err := txdb.Close()
		if err != nil {
			return err
		}

		delete(d.TransferDB, i)
	}
	return d.EventDB.Close()
}
