package cosmos

import (
	"log"

	"github.com/a8m/documentdb"
	"github.com/google/uuid"
)

func (u *CosmosDb[E]) Query(query string, params ...Param) []E {
	queryParams := []documentdb.Parameter{}
	for i := range params {
		queryParams = append(queryParams, *params[i].toDbParam())
	}
	var docs []E
	_, err := u.Client.QueryDocuments(u.Db.Coll.Self, documentdb.NewQuery(query, queryParams...), &docs, documentdb.CrossPartition())
	if err != nil {
		log.Fatalln(err)
	}
	return docs
}

func (u *CosmosDb[E]) Get(id uuid.UUID) *E {
	query := documentdb.NewQuery("select * from c where c.id='@id'", documentdb.Parameter{
		Name:  "@id",
		Value: id.String(),
	})
	var orders []E
	_, err := u.Client.QueryDocuments(u.Db.Coll.Self, query, &orders, documentdb.CrossPartition())
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

func (u *CosmosDb[E]) Upsert(doc *E, partitionKey string) (*documentdb.Response, error) {
	return u.Client.UpsertDocument(u.Coll.Self, doc, documentdb.PartitionKey(partitionKey))
}

func (u *CosmosDb[E]) Delete(id uuid.UUID) (*documentdb.Response, error) {
	var doc []documentdb.Document
	query := documentdb.NewQuery("select * from c where c.id='@id'", documentdb.Parameter{
		Name:  "@id",
		Value: id.String(),
	})
	_, err := u.Client.QueryDocuments(u.Coll.Self, query, &doc, documentdb.CrossPartition())

	if err != nil {
		return nil, err
	}

	if len(doc) > 1 {
		panic("more than 1 doc")
	}

	return u.Client.DeleteDocument(doc[0].Self, documentdb.PartitionKey(id.String()))
}

func (u *CosmosDb[E]) Empty() {
	query := documentdb.NewQuery("SELECT * FROM ROOT r")
	doc := &[]documentdb.Document{}
	_, err := u.Client.QueryDocuments(u.Coll.Self, query, doc, documentdb.CrossPartition())
	if err != nil {
		log.Fatalln(err)
	}
	for _, v := range *doc {
		log.Println(v.Self)
		_, err := u.Client.DeleteDocument(v.Self, documentdb.CrossPartition())
		if err != nil {
			log.Fatalln(err)
		}
	}

}
