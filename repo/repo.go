package repo

import (
	"github.com/zzonee/leaf-go/config"
	"github.com/zzonee/leaf-go/entity"
)

type Repo interface {
	GetAllKeys() ([]string, error)
	// 原子操作
	UpdateMaxIdAndGetSegment(key string) (entity.Segment, error)
	// 原子操作
	UpdateMaxIdByStepAndGetSegment(key string, step int64) (entity.Segment, error)
}

func NewRepo() (Repo, error) {
	_type := config.Global.DB.Type
	if _type == config.DB_Type_MySQL {
		return newDBRepo()
	}
	return nil, nil
}
