package es

import (
	"context"
	"fmt"
	"github.com/olivere/elastic"
	"time"
)

func BulkReq(client *elastic.Client, ctx context.Context) {
	// 批量操作
	tweet1 := Tweet{
		User:     "luffy",
		Message:  "hello es",
		Retweets: 1,
		Image:    "non",
		Created:  time.Now(),
		Tags:     nil,
		Location: "",
		Suggest:  nil,
	}
	tweet2 := `{"user" : "luffy", "message" : "It's a bulk test"}`
	indexReq1 := elastic.NewBulkIndexRequest().Index("tweet").Type("tweeter").Id("1").Doc(tweet1)
	indexReq2 := elastic.NewBulkIndexRequest().OpType("create").Index("tweet").Type("tweeter").Id("2").Doc(tweet2)
	updateReq := elastic.NewBulkUpdateRequest().Index("tweet").Type("tweeter").Id("1").Doc(struct {
		Retweets int `json:"retweets"`
	}{42})
	deleteReq := elastic.NewBulkDeleteRequest().Index("tweet").Type("tweeter").Id("2")

	bulkRequest := client.Bulk()
	bulkRequest.Add(indexReq1)
	bulkRequest.Add(indexReq2)
	bulkRequest.Add(updateReq)
	bulkRequest.Add(deleteReq)

	if bulkRequest.NumberOfActions() != 4 {
		fmt.Println("err=========")
		return
	}
	res, err := bulkRequest.Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	if bulkRequest.NumberOfActions() != 0 {
		fmt.Println("err-------------")
		return
	}

	fmt.Println("Index::::::::",res.Indexed(), len(res.Indexed()),res.Indexed()[0])
	fmt.Println("created::::::",res.Created(), len(res.Created()), res.Created()[0])
	fmt.Println("update::::::",res.Updated(), len(res.Updated()), res.Updated()[0])
	fmt.Println("delete::::::",res.Deleted(), len(res.Deleted()), res.Deleted()[0])

	//根据id查找
	idIndex := res.ById("1")
	fmt.Println("idIndex:::::",idIndex,len(idIndex))
	//根据操作查找
	deleteAction := res.ByAction("delete")
	fmt.Println("deleteAction:::::", deleteAction, len(deleteAction))

	fmt.Println("failed:::::::",res.Failed())
}

func BulkProcess1(client *elastic.Client, ctx context.Context) {
	//批量处理

	// Setup a bulk processor
	p, err := client.BulkProcessor().Name("BackGroundWorker-1").Workers(2).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}

	tweet := Tweet{
		User:     "luffy",
		Message:  "当上海贼王",
		Retweets: 11,
		Created:  time.Now(),
	}
	// create a index request
	r := elastic.NewBulkIndexRequest().Index("tweet").Type("tweeter").Id("11").Doc(tweet)
	p.Add(r)
	//Stop bulk processor
	p.Stop()
}

func BulkProcess2(client *elastic.Client, ctx context.Context) {
	//批量处理

	//添加请求控制
	p, err := client.BulkProcessor().
		Name("BackGroundWorker-2").
		Workers(2).
		BulkActions(1000).	//请求量大于1000
		BulkSize(2<<20).		//请求大小大于2M
		FlushInterval(30*time.Second).	//每30秒
		Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}

	tweet := Tweet{
		User:     "luffy",
		Message:  "当上海贼王",
		Retweets: 11,
		Created:  time.Now(),
	}
	// create a index request
	r := elastic.NewBulkIndexRequest().Index("tweet").Type("tweeter").Id("11").Doc(tweet)
	p.Add(r)

	// 等待所有的请求被提交，然后再执行。这个是 同步
	err = p.Flush()
	if err != nil {
		fmt.Println(err)
		return
	}

	//Stop bulk processor
	p.Stop()
}
