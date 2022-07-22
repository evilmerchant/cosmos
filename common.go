package cosmos

import (
	"fmt"

	"github.com/a8m/documentdb"
)

type Db struct {
	Db     *documentdb.Database
	Coll   *documentdb.Collection
	Client *documentdb.DocumentDB
}

type DbConfig struct {
	Url        string
	MasterKey  documentdb.Key
	Database   string
	Collection string
}

func (u *Db) findCollection(name string) (err error) {
	query := fmt.Sprintf("SELECT * FROM ROOT r WHERE r.id='%s'", name)
	if colls, err := u.Client.QueryCollections(u.Db.Self, documentdb.NewQuery(query)); err != nil {
		return err
	} else if len(colls) == 0 {
		return fmt.Errorf("collection %s does not exists", name)
	} else {
		u.Coll = &colls[0]
	}
	return
}

func (u *Db) findDatabase(name string) (err error) {
	query := fmt.Sprintf("SELECT * FROM ROOT r WHERE r.id='%s'", name)
	if dbs, err := u.Client.QueryDatabases(documentdb.NewQuery(query)); err != nil {
		return err
	} else if len(dbs) == 0 {
		return fmt.Errorf("database %s does not exists", name)
	} else {
		u.Db = &dbs[0]
	}
	return
}
