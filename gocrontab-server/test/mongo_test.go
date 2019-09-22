package test

/**
* @project: gocrontab
*
* @description:
*
* @author: cyy
*
* @create: 9/11/19 2:47 PM
**/

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"testing"
	"time"
)

type Users struct {
	Name     string   `bson:"name"`
	Age      int      `bson:"age"`
	Interest []string `bson:"interest"`
}

type LogMgr struct {
	client     *mongo.Client
	collection *mongo.Collection
}

var (
	G_logMgr *LogMgr
	G_config *Config
)

type Config struct {
	MongodbUri            string
	MongodbConnectTimeout int
}

func init() {
	var (
		ctx        context.Context
		opts       *options.ClientOptions
		client     *mongo.Client
		err        error
		collection *mongo.Collection
	)

	G_config = &Config{
		MongodbUri:            "mongodb://localhost:27017",
		MongodbConnectTimeout: 5000,
	}

	// 连接数据库
	ctx, _ = context.WithTimeout(context.Background(), time.Duration(G_config.MongodbConnectTimeout)*time.Millisecond) // ctx
	opts = options.Client().ApplyURI(G_config.MongodbUri)                                                              // opts
	if client, err = mongo.Connect(ctx, opts); err != nil {
		fmt.Println(err)
		return
	}

	// 连接表
	collection = client.Database("test").Collection("mytest")

	//赋值单例
	G_logMgr = &LogMgr{
		client:     client,
		collection: collection,
	}
}

// 保存数据
func (logMgr *LogMgr) SaveMongo() (err error) {
	var (
		insetRest     *mongo.InsertOneResult
		id            interface{}
		users         []interface{}
		insertManRest *mongo.InsertManyResult
	)
	// -----单个写入-----
	user := Users{"aa", 13, []string{"eat", "sleep"}}
	if insetRest, err = logMgr.collection.InsertOne(context.TODO(), &user); err != nil {
		fmt.Println(err)
		return
	}
	id = insetRest.InsertedID
	fmt.Println(id)

	// -----批量写入-----
	users = append(users, &Users{Name: "bb", Age: 15}, &Users{Name: "cc", Age: 33, Interest: []string{"study", "games"}},
		&Users{Name: "dd", Age: 66})
	if insertManRest, err = logMgr.collection.InsertMany(context.TODO(), users); err != nil {
		fmt.Println(err)
		return
	}
	for _, v := range insertManRest.InsertedIDs {
		fmt.Println(v)
	}
	return
}

// 查询数据
func (logMgr *LogMgr) SelectMongo() (err error) {
	var (
		cur  *mongo.Cursor
		user *Users
		ctx  context.Context
	)
	ctx = context.TODO()

	// -----单个查询-----
	if err = logMgr.collection.FindOne(ctx, bson.M{"name": "cc"}).Decode(&user); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("名字是cc的： %+v\n", user)

	// -----多个查询-----
	// 查找age>=25的 只显示3个 从大到小排序
	if cur, err = logMgr.collection.Find(ctx, bson.M{"age": bson.M{"$gte": 25}}, options.Find().SetLimit(3), options.Find().SetSort(bson.M{"age": -1}));
		err != nil {
		fmt.Println(err)
		return
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		// 第一种解析方法 解析后是结构体
		user = &Users{}
		if err = cur.Decode(user); err != nil {
			fmt.Println(err)
		}
		fmt.Printf("满足条件的结构体: %+v\n", user)

		// 第二种解析方法 解析后是map
		var result bson.M
		if err = cur.Decode(&result); err != nil {
			fmt.Println(err)
		}
		fmt.Printf("满足条件的map: %+v\n", result)
	}
	return
}

// 更新数据
func (logMgr *LogMgr) UpdateMongo() (err error) {
	var (
		ctx       context.Context
		updateRet *mongo.UpdateResult
	)

	// 更新name是ff的用户 存在才更新
	// $set 更新字段
	if updateRet, err = logMgr.collection.UpdateOne(ctx, bson.M{"name": "dd"}, bson.M{"$set": bson.M{"age": 78}}); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("更新的个数: ", updateRet.ModifiedCount)
	return
}

// 删除数据
func (logMgr *LogMgr) DeleteMongo() (err error) {
	var (
		ctx    context.Context
		delRes *mongo.DeleteResult
	)

	// -----单个删除-----
	if delRes, err = logMgr.collection.DeleteOne(ctx, bson.M{"name": "dd"}); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("删除的个数: ", delRes.DeletedCount)

	// -----多个删除-----
	if delRes, err = logMgr.collection.DeleteMany(ctx, bson.M{"age": bson.M{"$gte": 10}}); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("删除的个数: ", delRes.DeletedCount)
	return
}

func Close() {

}

func TestSaveMongo(t *testing.T) {
	G_logMgr.SaveMongo()
}

func TestSelectMongo(t *testing.T) {
	G_logMgr.SelectMongo()
}

func TestUpdateMongo(t *testing.T) {
	G_logMgr.UpdateMongo()
}

func TestDeleteMongo(t *testing.T) {
	G_logMgr.DeleteMongo()
}
