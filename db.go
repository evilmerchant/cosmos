package cosmos

import (
	"github.com/a8m/documentdb"
)

type CosmosDb[E any] struct {
	Db
}

func New[E any](url, key, database, collection string) CosmosDb[E] {
	return newDb[E](&DbConfig{
		Url:        url,
		MasterKey:  *documentdb.NewKey(key),
		Database:   database,
		Collection: collection,
	})
}

func newDb[E any](config *DbConfig) (db CosmosDb[E]) {
	db.Client = documentdb.New(config.Url, documentdb.NewConfig(&config.MasterKey))
	if err := db.findDatabase(config.Database); err != nil {
		panic(err)
	}
	if err := db.findCollection(config.Collection); err != nil {
		panic(err)
	}
	return
}
