package mconfig

import (
	"github.com/892294101/mparallel/src/tools"
	"github.com/magiconair/properties"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	SOURCEDB              = "source.db"
	SOURCETABLE           = "source.table"
	TARGETDB              = "target.db"
	TARGETTABLE           = "target.table"
	PARALLEL              = "dml.parallel"
	CURSOR                = "dml.cursor.number"
	BATCH                 = "dml.insert.batch"
	TABLECOLUMNFILTER     = "table.column.filter"
	TABLEFINDWHERE        = "table.data.find"
	TABLEAGGREGATE        = "table.data.aggregate"
	TABLECOLUSTERSHARDING = "table.cluster.sharding"
	TABLESHARDKEY         = "table.shard.key"
	TABLESHARDINDEX       = "table.shard.index"
)

type KeySets struct {
	Kes   string
	Value string
}

type KeyValue struct {
	Value   string
	Default string
}

type Param struct {
	Keys map[string]*KeyValue
	log  *logrus.Logger
}

func (s *Param) loadKeysLib() error {
	s.log.Infof("Load keyword library")
	s.Keys = make(map[string]*KeyValue)
	s.Keys[SOURCEDB] = &KeyValue{}
	s.Keys[SOURCETABLE] = &KeyValue{}
	s.Keys[TARGETDB] = &KeyValue{}
	s.Keys[TARGETTABLE] = &KeyValue{}
	s.Keys[PARALLEL] = &KeyValue{Default: "2"}
	s.Keys[CURSOR] = &KeyValue{Default: "1000"}
	s.Keys[BATCH] = &KeyValue{Default: "1000"}
	s.Keys[TABLECOLUMNFILTER] = &KeyValue{}
	s.Keys[TABLEFINDWHERE] = &KeyValue{}
	s.Keys[TABLEAGGREGATE] = &KeyValue{}
	/*s.Keys[TABLECOLUSTERSHARDING] = &KeyValue{Default: "N"}
	s.Keys[TABLESHARDKEY] = &KeyValue{}
	s.Keys[TABLESHARDINDEX] = &KeyValue{}*/

	return nil
}

func (s *Param) CheckHostAndTable() error {
	if (s.GetKeyVal(SOURCEDB) == s.GetKeyVal(TARGETDB)) && (s.GetKeyVal(SOURCETABLE) == s.GetKeyVal(TARGETTABLE)) {
		return errors.Errorf("in the same database, the table names cannot be the same")
	}

	_, err := s.GetColumnFilter()

	return err
}

func (s *Param) Load(log *logrus.Logger) error {
	s.log = log
	s.loadKeysLib()
	dir, err := tools.GetHomeDirectory()
	if err != nil {
		return err
	}
	configPath := filepath.Join(*dir, "conf", "mconfig.properties")
	s.log.Infof("read %v configuration file content", configPath)
	p, err := properties.LoadFile(configPath, properties.UTF8)
	if err != nil {
		return err
	}

	for _, v := range p.Keys() {
		_, ok := s.Keys[v]
		if !ok {
			return errors.Errorf("%v illegal keyword", v)
		}
	}

	for k, v := range s.Keys {
		val := p.GetString(k, v.Default)
		if len(val) > 0 {
			switch k {
			case TARGETTABLE, SOURCETABLE:
				_, _, err := tools.GetDBTable(val)
				if err != nil {
					return err
				}
			case TABLECOLUSTERSHARDING:
				if !strings.EqualFold(val, "n") && !strings.EqualFold(val, "y") {
					return errors.Errorf("%s value of a can only be y/n(Y/N)", k)
				}
			}
			v.Value = val
		} else {
			switch k {
			case TABLEFINDWHERE, TABLEAGGREGATE:
			default:
				return errors.Errorf("%s must have a value set", k)
			}

		}
	}

	return s.CheckHostAndTable()
}

func (s *Param) GetColumnFilter() (k []*KeySets, err error) {
	res := strings.Split(s.Keys[TABLECOLUMNFILTER].Value, ",")
	if len(res) > 0 {
		for _, re := range res {
			reg := regexp.MustCompile(`^(.*)+\:(.*)+$`)
			matches := reg.FindStringSubmatch(re)
			if matches != nil && len(matches) == 3 {
				k = append(k, &KeySets{Kes: matches[1], Value: matches[2]})
			} else {
				return nil, errors.Errorf("%s Illegal input, format is <column>.<identification>", re)
			}
		}
	}
	return
}

func (s *Param) CheckShardKey() error {

	return nil
}

func (s *Param) CheckShardIndex() error {

	return nil
}

func (s *Param) GetKeyVal(key string) string {
	return s.Keys[key].Value
}
func NewParams() *Param {
	return new(Param)
}
