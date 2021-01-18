package repo

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/longyufei109/leaf-go/config"
	"github.com/longyufei109/leaf-go/entity"
	"github.com/longyufei109/leaf-go/log"
	"sync/atomic"
)

//DROP TABLE IF EXISTS `leaf_alloc`;
//
//CREATE TABLE `leaf_alloc` (
//`biz_tag` varchar(128)  NOT NULL DEFAULT '',
//`max_id` bigint(20) NOT NULL DEFAULT '1',
//`step` int(11) NOT NULL,
//`description` varchar(256)  DEFAULT NULL,
//`update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
//PRIMARY KEY (`biz_tag`)
//) ENGINE=InnoDB;

type dbImpl struct {
	db    []*sql.DB
	pos   int32
	total int32
}

func newDBRepo() (Repo, error) {
	var err error
	r := &dbImpl{}
	for _, datasource := range config.Global.DB.DataSource {
		db, e := sql.Open("mysql", datasource)
		if e == nil {
			r.db = append(r.db, db)
		} else {
			log.Print("open db failed. datasource:%s, err:%v", datasource, err)
		}
	}
	if len(r.db) == 0 {
		err = fmt.Errorf("no valid db")
		return nil, err
	}
	r.pos = 0
	r.total = int32(len(r.db))
	return r, err
}

func (r *dbImpl) getDB() *sql.DB {
	pos := atomic.AddInt32(&r.pos, 1) & r.total
	if pos == r.total {
		pos -= 1
	}
	return r.db[pos]
}

func (r *dbImpl) GetAllKeys() ([]string, error) {
	rows, err := r.getDB().Query("SELECT biz_tag FROM leaf_alloc")
	if err != nil {
		return nil, err
	}
	var keys []string
	for rows.Next() {
		key := ""
		_ = rows.Scan(&key)
		if key != "" {
			keys = append(keys, key)
		}
	}
	_ = rows.Close()
	return keys, nil
}

func (r *dbImpl) UpdateMaxIdAndGetSegment(key string) (seg entity.Segment, err error) {
	seg.Key = key
	var tx *sql.Tx
	tx, err = r.getDB().Begin()
	if err != nil {
		return
	}
	_, err = tx.Exec("UPDATE leaf_alloc SET max_id=max_id+step WHERE biz_tag=?", key)
	if err != nil {
		_ = tx.Rollback()
		return
	}

	row := tx.QueryRow("SELECT max_id,step FROM leaf_alloc WHERE biz_tag=?", key)
	if row == nil {
		err = fmt.Errorf("not found, key:%s", key)
		_ = tx.Rollback()
		return
	}
	err = row.Scan(&seg.MaxId, &seg.Step)
	_ = tx.Commit()
	return
}

func (r *dbImpl) UpdateMaxIdByStepAndGetSegment(key string, step int64) (seg entity.Segment, err error) {
	seg.Key = key
	var tx *sql.Tx
	tx, err = r.getDB().Begin()
	if err != nil {
		return
	}

	_, err = tx.Exec("UPDATE leaf_alloc SET max_id=max_id+? WHERE biz_tag=?", step, key)
	if err != nil {
		_ = tx.Rollback()
		return
	}

	row := tx.QueryRow("SELECT max_id,step FROM leaf_alloc WHERE biz_tag=?", key)
	if row == nil {
		err = fmt.Errorf("not found, key:%s", key)
		_ = tx.Rollback()
		return
	}
	err = row.Scan(&seg.MaxId, &seg.Step)
	_ = tx.Commit()
	return
}
