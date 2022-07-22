package cosmos

import (
	"fmt"
	"log"

	"github.com/a8m/documentdb"
	"github.com/google/uuid"
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
	if err := db.findDatabase("order"); err != nil {
		panic(err)
	}
	if err := db.findCollection("orders"); err != nil {
		panic(err)
	}
	return
}

func (u *CosmosDb[E]) Query(query string) []E {
	var docs []E
	_, err := u.Client.QueryDocuments(u.Db.Coll.Self, documentdb.NewQuery(query), &docs, documentdb.CrossPartition())
	if err != nil {
		log.Fatalln(err)
	}
	return docs
}

func (u *CosmosDb[E]) Get(id uuid.UUID) *E {
	query := fmt.Sprintf("SELECT * FROM c WHERE c.id='%s'", id.String())
	var orders []E
	_, err := u.Client.QueryDocuments(u.Db.Coll.Self, documentdb.NewQuery(query), &orders, documentdb.CrossPartition())
	if err != nil {
		log.Fatalln(err)
	}
	if len(orders) == 0 {
		return nil
	}
	if len(orders) > 1 {
		panic("more than 1 doc")
	}
	return &orders[0]
}

func (u *CosmosDb[E]) Upsert(product *E, partitionKey string) (*documentdb.Response, error) {
	return u.Client.UpsertDocument(u.Coll.Self, product, documentdb.PartitionKey(partitionKey))
}

func (u *CosmosDb[E]) Delete(productId uuid.UUID) (*documentdb.Response, error) {
	var doc []documentdb.Document
	query := fmt.Sprintf("SELECT * FROM c WHERE c.id='%s'", productId.String())
	_, err := u.Client.QueryDocuments(u.Coll.Self, documentdb.NewQuery(query), &doc, documentdb.CrossPartition())

	if err != nil {
		return nil, err
	}

	if len(doc) > 1 {
		panic("more than 1 doc")
	}

	return u.Client.DeleteDocument(doc[0].Self, documentdb.PartitionKey(productId.String()))
}
