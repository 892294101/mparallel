package bus

import (
	"context"
	mauth "github.com/892294101/mparallel/src/auth"
	mlog "github.com/892294101/mparallel/src/log"
	"github.com/892294101/mparallel/src/mconfig"
	"github.com/892294101/mparallel/src/tools"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"runtime/debug"
	"strconv"
	"sync"
	"time"
)

const (
	ExtractProcess  = "ExtractProcess"
	ReplicatProcess = "ReplicatProcess"
	QueueProcess    = "QueueProcess"
	MaxAttempts     = 3
)

type BatchData struct {
	batchCache  []interface{}
	batchNumber int
}
type BusPoint struct {
	param        *mconfig.Param
	mongo        *mauth.MongoDB
	ProcessState map[string]bool
	log          *logrus.Logger
	cache        chan bson.Raw
	batchCache   chan *BatchData
	dmlbatch     int
	dmlParallel  int
	lock         sync.Mutex
}

/*
see: https://pkg.go.dev/go.mongodb.org/mongo-driver@v1.11.2/mongo#Database.RunCommand
*/
func (b *BusPoint) enableTableSharding() error {
	st := b.param.GetKeyVal(mconfig.TARGETTABLE)
	/*db, table, err := tools.GetDBTable(st)
	if err != nil {
		return err
	}*/

	command := bson.D{
		{Key: "shardCollection", Value: st},
		{Key: "key", Value: bson.E{
			Key: "start_time", Value: "hashed"},
		},
		{Key: "unique", Value: false},
	}

	//opts := options.RunCmd().SetReadPreference(readpref.Primary())
	var result bson.M

	adminDB := b.mongo.Source.Client.Database("admin")

	err := adminDB.RunCommand(context.TODO(), command).Decode(&result)
	if err != nil {
		return err
	}

	b.log.Infof("%v", result)

	return nil
}
func (b *BusPoint) insertMany(d *BatchData) (*mongo.InsertManyResult, error) {
	st := b.param.GetKeyVal(mconfig.TARGETTABLE)
	db, table, err := tools.GetDBTable(st)
	if err != nil {
		return nil, err
	}

	coll := b.mongo.Source.Client.Database(db).Collection(table)

	res, err := coll.InsertMany(context.TODO(), d.batchCache, options.InsertMany().SetBypassDocumentValidation(false), options.InsertMany().SetOrdered(false))

	if err != nil {
		return nil, err
	}
	b.log.Infof("write complete. batch id: %v", d.batchNumber)
	return res, nil
}

func (b *BusPoint) batchData(swg *sync.WaitGroup) {
	defer func() {
		swg.Done()
		b.log.Infof("close batch cache queue. current queue number %v", len(b.batchCache))
		close(b.batchCache)
		if err := recover(); err != nil {
			b.SetExtractExitState(true)
			b.log.Errorf("Panic Message: %s", err)
			b.log.Errorf("Exception File: %s", tools.GetFunctionName(b.batchCache))
			b.log.Errorf("Print Stack Message: %s", string(debug.Stack()))
			b.log.Errorf("Abnormal exit of program")
		}
	}()
	b.log.Infof("start the process of generating batch queue data")

	var buf []interface{}
	var batchNum int
	for {
		data, ok := <-b.cache
		if ok == false {
			break
		}
		switch {
		case b.GetRepliactExitState():
			b.log.Errorf("due to abnormal replica process. this batch queue process will also exit.")
			return
		case b.GetExtractExitState():
			b.log.Errorf("due to abnormal extract process. this batch queue process will also exit.")
			return
		default:
			buf = append(buf, data)
			if len(buf) == b.dmlbatch {
				batchNum++
				b.batchCache <- &BatchData{batchCache: buf, batchNumber: batchNum}
				b.log.Infof("batch id: %v. batch data length: %v", batchNum, len(buf))
				buf = make([]interface{}, 0)
			}
		}
	}
	if len(buf) > 0 {
		batchNum++
		b.batchCache <- &BatchData{batchCache: buf, batchNumber: batchNum}
		b.log.Infof("end batch id: %v. batch data length: %v", batchNum, len(buf))
	}

	b.log.Infof("generate batch data completed")
}

func (b *BusPoint) insertData(swg *sync.WaitGroup) {
	b.log.Infof("start the batch insert data process")
	var docCount int

	var wg sync.WaitGroup
	var AttemptedNumber int

	b.log.Infof("open %v concurrent channels", b.dmlParallel)
	ch := make(chan int, b.dmlParallel)
	defer func() {
		swg.Done()
		b.log.Infof("close insertMany channel. %v channels running", len(ch))
		close(ch)
		if err := recover(); err != nil {
			b.SetExtractExitState(true)
			b.log.Errorf("Panic Message: %s", err)
			b.log.Errorf("Exception File: %s", tools.GetFunctionName(b.insertData))
			b.log.Errorf("Print Stack Message: %s", string(debug.Stack()))
			b.log.Errorf("Abnormal exit of program")
		}
	}()

	for {
		data, ok := <-b.batchCache
		if !ok {
			break
		}
		b.log.Infof("dequeue data batch id: %v. data length: %v. queue length: %v", data.batchNumber, len(data.batchCache), len(b.batchCache))

	RESTART:

		if b.GetRepliactExitState() || b.GetExtractExitState() {
			if len(ch) > 0 {
				if AttemptedNumber >= 3 {
					b.log.Warnf("attempted to wait %v times, forced exit", AttemptedNumber)
					b.log.Infof("insert data complete. with a total of %v lines of documents", docCount)
					return
				}
				b.log.Warnf("asynchronous insertMany thread exists. Attempted to wait for 3 seconds, totaling %v times", MaxAttempts)
				time.Sleep(3 * time.Second)
				AttemptedNumber++
				goto RESTART
			}
			b.log.Errorf("due to abnormal other process. this process will also exit. Waiting %v times", AttemptedNumber)
			return
		} else {

			if len(ch) < b.dmlParallel {
				//b.log.Infof("current parallel: %v. Continue loading new batches", len(ch))
				wg.Add(1)
				go func(bp *BusPoint, c chan int, d *BatchData, w *sync.WaitGroup) {
					c <- 1
					defer w.Done()
					defer func() {
						<-c
					}()
					res, err := bp.insertMany(d)
					if err != nil {
						bp.log.Errorf("insertMany error: %s. batch id: %v", err, d.batchNumber)
						bp.SetRepliactExitState(true)
					} else {
						bp.log.Infof("write batch data success (batch id: %v). batch data length: %v. running channel: %v. idle channel: %v", d.batchNumber, len(d.batchCache), len(c), b.dmlParallel-len(c))
						docCount += len(res.InsertedIDs)
					}
					time.Sleep(time.Second / 10)
				}(b, ch, data, &wg)
			} else {
				//b.log.Infof("current parallel: %v. Waiting for idle threads", len(ch))
				time.Sleep(time.Millisecond)
				goto RESTART
			}
		}

	}
	wg.Wait()
	b.log.Infof("insert data complete. with a total of %v lines of documents", docCount)
}

func (b *BusPoint) SetExtractExitState(s bool) {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.ProcessState[ExtractProcess] = s
}

func (b *BusPoint) SetRepliactExitState(s bool) {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.ProcessState[ReplicatProcess] = s
}

func (b *BusPoint) GetExtractExitState() bool {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.ProcessState[ExtractProcess]
}

func (b *BusPoint) GetRepliactExitState() bool {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.ProcessState[ReplicatProcess]
}

func (b *BusPoint) extractData(wg *sync.WaitGroup) {
	b.log.Infof("starting to extract migration data into cache")
	defer func() {
		if err := recover(); err != nil {
			b.SetExtractExitState(true)
			b.log.Errorf("Panic Message: %s", err)
			b.log.Errorf("Exception File: %s", tools.GetFunctionName(b.extractData))
			b.log.Errorf("Print Stack Message: %s", string(debug.Stack()))
			b.log.Errorf("Abnormal exit of program")
		}
		wg.Done()
		b.log.Infof("close the data retrieval queue. there are %v queue data waiting to be processed", len(b.cache))
		close(b.cache)
	}()
	var docCount int
	var docByte int

	st := b.param.GetKeyVal(mconfig.SOURCETABLE)
	db, table, err := tools.GetDBTable(st)
	if err != nil {
		b.SetExtractExitState(true)
		b.log.Errorf("%s", err)
		return
	}

	curnum, err := strconv.Atoi(b.param.GetKeyVal(mconfig.CURSOR))
	if err != nil {
		b.SetExtractExitState(true)
		b.log.Errorf("encountered an error during data extraction: %s", err)
		return
	}

	coll := b.mongo.Source.Client.Database(db).Collection(table)

	// 字段过滤
	projection := bson.D{}
	if cn, err := b.param.GetColumnFilter(); err != nil {
		b.log.Errorf("Illegal filter field configuration: %s", err)
		return
	} else {
		for _, sets := range cn {
			ic, err := strconv.Atoi(sets.Value)
			if err != nil {
				b.log.Errorf("Illegal column %s:%s identification code: %s", sets.Kes, sets.Value, err)
				return
			}
			projection = append(projection, bson.E{Key: sets.Kes, Value: ic})
		}
	}
	// 只生效find
	var opts []*options.FindOptions
	opts = append(opts, options.Find().SetShowRecordID(false))
	opts = append(opts, options.Find().SetBatchSize(int32(curnum)))
	opts = append(opts, options.Find().SetNoCursorTimeout(true))
	if len(projection) > 0 {
		opts = append(opts, options.Find().SetProjection(projection))
	}

	// 谓词和聚合条件使用
	var predicate interface{}
	var cursor *mongo.Cursor

	aggregate := b.param.GetKeyVal(mconfig.TABLEAGGREGATE)
	wheres := b.param.GetKeyVal(mconfig.TABLEFINDWHERE)

	switch {
	case len(aggregate) > 0:
		// aggregate json条件序列化bson格式。
		cv, err := tools.JsonStr2Bson(aggregate)
		if err != nil {
			b.SetExtractExitState(true)
			b.log.Errorf("encountered an error during data extraction: %s", err)
			return
		}
		predicate = cv
		b.log.Infof("find %v on using aggregate: %v", st, aggregate)

		// 聚合查询时设置默认配置
		var aopts []*options.AggregateOptions
		aopts = append(aopts, options.Aggregate().SetBatchSize(int32(curnum)))
		aopts = append(aopts, options.Aggregate().SetAllowDiskUse(true))

		cursor, err = coll.Aggregate(context.Background(), predicate, aopts...)
	case len(wheres) > 0:
		// 把find json条件序列化bson格式。
		cv, err := tools.JsonStr2Bson(wheres)
		if err != nil {
			b.SetExtractExitState(true)
			b.log.Errorf("encountered an error during data extraction: %s", err)
			return
		}
		predicate = cv
		b.log.Infof("find %v on using where: %v", st, wheres)
		cursor, err = coll.Find(context.Background(), predicate, opts...)
	default:
		predicate = bson.D{}
		b.log.Infof("find %v on full table scan", st)
		cursor, err = coll.Find(context.Background(), predicate, opts...)
	}

	if err != nil {
		b.SetExtractExitState(true)
		b.log.Errorf("encountered an error during data extraction: %s", err)
		return
	}
	defer cursor.Close(context.Background())

	if cursor == nil {
		b.SetExtractExitState(true)
		b.log.Errorf("unexpected exit due to cursor is nil for extract data the source table")
		return
	}

	for cursor != nil && cursor.Next(context.Background()) {
		switch {
		case b.GetRepliactExitState():
			b.log.Errorf("due to abnormal replica process. this extract process will also exit. with a total of %v lines of documents and %v bytes", docCount, docByte)
			return
		default:
			docCount++
			docByte += len(cursor.Current)
			b.cache <- cursor.Current
		}
	}

	b.log.Infof("data extraction completed. with a total of %v lines of documents and %v bytes", docCount, docByte)
}

func (b *BusPoint) Log(err error) {
	b.log.Errorf("%s", err)
}

func (b *BusPoint) LoadMParallel() error {
	l, err := mlog.InitDDSlog(mlog.MPARALLEL)
	if err != nil {
		return err
	}
	b.log = l
	b.log.Infof("Version %s on build %s", mauth.Version, mauth.BDate)

	b.log.Infof("Initialize parameter configuration")
	b.param = mconfig.NewParams()
	if err := b.param.Load(b.log); err != nil {
		return err
	}

	b.mongo = mauth.NewMongo()
	b.log.Infof("establish mongodb database connection for source: %v", b.param.GetKeyVal(mconfig.SOURCEDB))
	if err := b.mongo.InitMongo(mconfig.SOURCEDB, b.param.GetKeyVal(mconfig.SOURCEDB)); err != nil {
		return err
	}

	b.log.Infof("establish mongodb database connection for target: %v", b.param.GetKeyVal(mconfig.TARGETDB))
	if err := b.mongo.InitMongo(mconfig.TARGETDB, b.param.GetKeyVal(mconfig.TARGETDB)); err != nil {
		return err
	}

	b.cache = make(chan bson.Raw, 200000)
	b.batchCache = make(chan *BatchData, 100)
	b.ProcessState = map[string]bool{ExtractProcess: false, ReplicatProcess: false, QueueProcess: false}

	batch, err := strconv.Atoi(b.param.GetKeyVal(mconfig.BATCH))
	if err != nil {
		return err
	}
	b.dmlbatch = batch

	parallel, err := strconv.Atoi(b.param.GetKeyVal(mconfig.PARALLEL))
	if err != nil {
		return err
	}
	b.dmlParallel = parallel

	return nil

}

func (b *BusPoint) Close() {
	if b.mongo == nil {
		return
	}

	if b.mongo.Source != nil && b.mongo.Source.Client != nil {
		b.mongo.Source.Client.Disconnect(context.TODO())
		b.log.Infof("close bus database connect for source")
	}
	if b.mongo.Target != nil && b.mongo.Target.Client != nil {
		b.mongo.Target.Client.Disconnect(context.TODO())
		b.log.Infof("close bus database connect for target")
	}

}

func (b *BusPoint) Relocate() error {

	//b.enableTableSharding()
	var wg sync.WaitGroup
	wg.Add(1)
	func(bp *BusPoint, w *sync.WaitGroup) {
		go bp.extractData(w)
	}(b, &wg)

	wg.Add(1)
	func(bp *BusPoint, w *sync.WaitGroup) {
		go bp.batchData(&wg)
	}(b, &wg)

	wg.Add(1)
	func(bp *BusPoint, w *sync.WaitGroup) {
		go bp.insertData(&wg)
	}(b, &wg)

	wg.Wait()
	return nil
}

func NewBus() *BusPoint {
	return new(BusPoint)
}
