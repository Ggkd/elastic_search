package es

import (
	"context"
	"fmt"
	"github.com/olivere/elastic"
	"log"
	"os"
	"time"
)

var mappings = `{
	"settings":{
		"number_of_shards":1,
		"number_of_replicas":0
	},
	"mappings":{
		"tweet":{
			"properties":{
				"tags":{
					"type":"text"
				},
				"location":{
					"type":"geo_point"
				}
			}
		}
	}
}`

type Tweet struct {
	User     string                `json:"user"`
	Message  string                `json:"message"`
	Retweets int                   `json:"retweets"`
	Image    string                `json:"image,omitempty"`
	Created  time.Time             `json:"created,omitempty"`
	Tags     []string              `json:"tags,omitempty"`
	Location string                `json:"location,omitempty"`
	Suggest  *elastic.SuggestField `json:"suggest_field,omitempty"`
}


func Client() {
	client, err := elastic.NewClient(
		elastic.SetURL("http://192.168.153.6:9200"),		//指定要连接的URL（默认值是http://127.0.0.1:9200）
		elastic.SetSniff(false),						//允许您指定弹性是否应该定期检查集群（默认为true）
		elastic.SetHealthcheckInterval(10*time.Second),			//指定间隔之间的两个健康检查（默认是60秒）
		elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)),		//将日志记录器设置为用于错误消息（默认为NIL）
		elastic.SetGzip(true),							//启用或禁用请求端的压缩。默认情况下禁用
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	ctx := context.Background()

	//CreateIndex(client, ctx)

	//IsExist(client, ctx)

	//DeleteIndex(client, ctx)

	//InsertDoc(client, ctx)

	//GetDoc(client, ctx)

	//SearchEach(client, ctx)

	//SearchHits(client, ctx)

	//DeleteDoc(client, ctx)
	//SearchHits(client, ctx)

	//UpdateDoc(client, ctx)

	//BulkReq(client, ctx)

	//BulkProcess1(client, ctx)
	//BulkProcess2(client, ctx)

	//SearchWithBoolQuery(client, ctx)

	//FinderMain(client)

	//Term(client, ctx)

	//BoolQuery(client, ctx)

	//IdsQuery(client, ctx)

	//MatchQuery(client, ctx)

	//MatchQueryWithOptions(client, ctx)

	//MatchPhraseQuery(client, ctx)

	//MatchPhrasePrefixQuery(client, ctx)

	//DisMaxQuery(client, ctx)

	//MultiMatchQuery(client, ctx)

	//MultiMatchQueryXFields(client, ctx)

	//PrefixQuery(client, ctx)

	//QueryStringQuery(client, ctx)

	//RangeQuery(client, ctx)

	//RawStringQuery(client, ctx)

	//TermQuery(client, ctx)

	//TermsQuery(client, ctx)

	SearchSortingBySorters(client, ctx)

	client.Stop()
}