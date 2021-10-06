package ovsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

const (
	CombinedIndexDelimiter = ","
)

var emptyIndex = make(map[string]map[string]*cachedRow)

type rawRow map[string]interface{}
type cachedRow struct {
	row     rawRow
	counter int
}

func (r rawRow) GetUUID() (string, error) {
	uuidInt, ok := r[libovsdb.ColUUID]
	if !ok {
		return "", fmt.Errorf("row %v doesn't contain %s column", r, libovsdb.ColUUID)
	}
	ovsUuid, ok := uuidInt.([]string)
	if !ok {
		return "", fmt.Errorf("wrong uuid type %T %v", uuidInt, uuidInt)
	}
	return ovsUuid[1], nil
}

type cache map[string]*databaseCache

type databaseCache struct {
	mu      sync.Mutex
	dbCache map[string]tableCache
	// cancel function to close the etcdTrx watcher
	cancel context.CancelFunc
	log    logr.Logger
}

type tableCache struct {
	uuidIndex map[string]*cachedRow
	indexes   map[string]map[string]*cachedRow
}

func newTableCache(tSchema *libovsdb.TableSchema) *tableCache {
	ind := tSchema.Indexes
	var indexes map[string]map[string]*cachedRow
	if ind != nil && len(ind) > 0 {
		indexes = make(map[string]map[string]*cachedRow)
		for _, i := range ind {
			var currentIndex string
			l := len(i)
			switch l {
			case 0:
				continue
			case 1:
				// single column index
				currentIndex = i[0]
			default:
				currentIndex = i[0]
				for j := 1; j < l; j++ {
					currentIndex = fmt.Sprintf("%s%s%s", currentIndex, CombinedIndexDelimiter, i[j])
				}
			}
			indexes[currentIndex] = make(map[string]*cachedRow)
		}
	} else {
		indexes = emptyIndex
	}
	return &tableCache{uuidIndex: make(map[string]*cachedRow), indexes: indexes}
}

func (tc *tableCache) addRow(newRow rawRow) error {
	uuid, err := newRow.GetUUID()
	if err != nil {
		return err
	}
	cRow := cachedRow{row: newRow}
	tc.uuidIndex[uuid] = &cRow
	for indx := range tc.indexes {
		indxs := strings.Split(indx, CombinedIndexDelimiter)
		value := ""
		for _, i := range indxs {
			value += fmt.Sprintf("%v", cRow.row[i])
		}
		tc.indexes[indx][value] = &cRow
	}
	return nil
}

func (tc *tableCache) delRow(uuid string) {
	cRow, ok := tc.uuidIndex[uuid]
	if !ok {
		return
	}
	for indx := range tc.indexes {
		indxs := strings.Split(indx, CombinedIndexDelimiter)
		value := ""
		for _, i := range indxs {
			value += fmt.Sprintf("%v", cRow.row[i])
		}
		indMap := tc.indexes[indx]
		oRow, ok := indMap[value]
		if ok && oRow == cRow {
			delete(indMap, value)
		}
	}
}

func (tc *tableCache) updateRow(uRow rawRow) error {
	// TODO GC update references
	uuid, err := uRow.GetUUID()
	if err != nil {
		return err
	}
	cRow, ok := tc.uuidIndex[uuid]
	if !ok {
		return tc.addRow(uRow)
	}
	orgRow := cRow.row
	cRow.row = uRow
	tc.uuidIndex[uuid] = cRow

	for indx := range tc.indexes {
		indxs := strings.Split(indx, CombinedIndexDelimiter)
		newValue := ""
		oldValue := ""
		for _, i := range indxs {
			newValue += fmt.Sprintf("%v", uRow[i])
			oldValue += fmt.Sprintf("%v", orgRow[i])
		}
		if newValue != oldValue {
			indMap := tc.indexes[indx]
			delete(indMap, oldValue)
			indMap[newValue] = cRow
		}
	}
	return nil
}

func (tc *tableCache) getRowByCondition(cons *Conditions) (*cachedRow, bool) {

	return nil, false
}

func (tc *tableCache) size() int {
	return len(tc.uuidIndex)
}

func (c *cache) addDatabaseCache(schema *libovsdb.DatabaseSchema, etcdClient *clientv3.Client, log logr.Logger) error {
	dbName := schema.Name
	if _, ok := (*c)[dbName]; ok {
		return errors.New("Duplicate DatabaseCache: " + dbName)
	}
	dbCache := databaseCache{dbCache: map[string]tableCache{}, log: log}
	for tName, tSchema  := range schema.Tables {
		dbCache.dbCache[tName] = *newTableCache(&tSchema)
	}
	(*c)[dbName] = &dbCache
	if dbName == InternalServer {
		return nil
	}
	ctxt, cancel := context.WithCancel(context.Background())
	dbCache.cancel = cancel
	key := common.NewDBPrefixKey(dbName)
	resp, err := etcdClient.Get(ctxt, key.String(), clientv3.WithPrefix())
	if err != nil {
		log.Error(err, "get KeyData")
		return err
	}
	wch := etcdClient.Watch(clientv3.WithRequireLeader(ctxt), key.String(),
		clientv3.WithPrefix(),
		clientv3.WithCreatedNotify(),
		clientv3.WithPrevKV())

	err = dbCache.putEtcdKV(resp.Kvs)
	if err != nil {
		log.Error(err, "putEtcdKV")
		return err
	}
	go func() {
		// TODO propagate to monitors
		for wresp := range wch {
			if wresp.Canceled {
				log.V(1).Info("DB cache monitor was canceled", "dbName", dbName)
				// TODO: reconnect ?
				return
			}
			dbCache.updateCache(wresp.Events)
		}
	}()
	return nil
}

func (dc *databaseCache) updateCache(events []*clientv3.Event) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	for _, event := range events {
		key, err := common.ParseKey(string(event.Kv.Key))
		if err != nil {
			dc.log.Error(err, "got a wrong formatted key from etcd", "key", string(event.Kv.Key))
			continue
		}
		if key.IsCommentKey() {
			continue
		}
		tb := dc.getTableCache(key.TableName)
		if event.Type == mvccpb.DELETE {
			tb.delRow(key.UUID)
		} else {
			rr := rawRow{}
			err = json.Unmarshal(event.Kv.Value, &rr)
			if err != nil {
				dc.log.Error(err, "cannot unmarshal etcd value to [string]interface{}", "key", string(event.Kv.Key))
				continue
			}
			err = (*tb).updateRow(rr)
			if err != nil {
				dc.log.Error(err, "updateRow returned err", "key", string(event.Kv.Key))
				continue
			}
		}
	}
}

func (dc *databaseCache) putEtcdKV(kvs []*mvccpb.KeyValue) error {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	for _, kv := range kvs {
		if kv == nil {
			continue
		}
		key, err := common.ParseKey(string(kv.Key))
		if err != nil {
			return err
		}
		if key.IsCommentKey() {
			continue
		}
		tb := dc.getTableCache(key.TableName)
		rr := rawRow{}
		err = json.Unmarshal(kv.Value, &rr)
		if err != nil {
			return err
		}
		err = (*tb).updateRow(rr)
		if err != nil {
			return err
		}
	}
	return nil
}

// should be called under locked dc.mu
func (dc *databaseCache) getTableCache(tableName string) *tableCache {
	tb, ok := dc.dbCache[tableName]
	if !ok {
		tb = tableCache{}
		dc.dbCache[tableName] = tb
	}
	return &tb
}

func (dc *databaseCache) size() int {
	var ret int
	for _, tbCache := range dc.dbCache {
		ret += tbCache.size()
	}
	return ret
}

func (c *cache) size() int {
	var ret int
	for _, dbCache := range *c {
		ret += dbCache.size()
	}
	return ret
}

func (c *cache) getDBCache(dbname string) *databaseCache {
	db, ok := (*c)[dbname]
	if !ok {
		panic(fmt.Sprintf("Database cache for %s did not fined", dbname))
	}
	return db
}
