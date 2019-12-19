package es

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic"
	"reflect"
)

func InsertDoc(client *elastic.Client, ctx context.Context)  {
	//插入一条document
	//tweet1 := Tweet{
	//	User:     "luffy",
	//	Message:  "hello es",
	//	Retweets: 1,
	//	Image:    "non",
	//	Created:  time.Now(),
	//	Tags:     nil,
	//	Location: "",
	//	Suggest:  nil,
	//}
	tweet2 := `{"user" : "zorro", "message" : "That's zorro"}`
	//put1, err := client.Index().Index("tweet").Type("tweeter").Id("1").BodyJson(tweet1).Do(ctx)
	put2, err := client.Index().Index("tweet").Type("tweeter").Id("2").BodyString(tweet2).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	//fmt.Printf("Indexed tweet [id: %s] to [index: %s], [type: %s]\n", put1.Id, put1.Index, put1.Type)
	fmt.Printf("Indexed tweet [id: %s] to [index: %s], [type: %s]\n", put2.Id, put2.Index, put2.Type)
}

func GetDoc(client *elastic.Client, ctx context.Context)  {
	//获取数据
	res, err := client.Get().Index("tweet").Type("tweeter").Id("1").Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%s\n", res.Source)
}

func SearchEach(client *elastic.Client, ctx context.Context) {
	// 通过SearchResult
	termQuery := elastic.NewTermQuery("user", "luffy")
	searchResult, err := client.Search().Index("tweet").Query(termQuery).From(0).Size(10).Pretty(true).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("took time:",searchResult.TookInMillis)
	var tweet Tweet
	for _, item := range searchResult.Each(reflect.TypeOf(tweet)){
		t := item.(Tweet)
		fmt.Printf("Tweet by %s: %s\n", t.User, t.Message)
	}
	fmt.Printf("Found a total of %d tweets\n", searchResult.TotalHits())
}

func SearchHits(client *elastic.Client, ctx context.Context) {
	// 通过SearchResult.Hits
	termQuery := elastic.NewTermQuery("user", "luffy")
	searchResult, err := client.Search().Index("tweet").Query(termQuery).From(0).Size(10).Pretty(true).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	if searchResult.Hits.TotalHits > 0 {
		fmt.Printf("Found a total of %d tweets\n", searchResult.Hits.TotalHits)
		for _, hit := range searchResult.Hits.Hits {
			// hit.Index contains the name of the index
			fmt.Println("index:",hit.Index)
			// Deserialize hit.Source into a Tweet (could also be just a map[string]interface{}).
			var tweet Tweet
			err := json.Unmarshal(*hit.Source, &tweet)
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Printf("\tTweet by %s: %s\n", tweet.User, tweet.Message)

		}
	} else {
		fmt.Print("Found no tweets\n")
	}
}

func DeleteDoc(client *elastic.Client, ctx context.Context) {
	// 删除document
	res, err := client.Delete().Index("tweet").Type("tweeter").Id("2").Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("delete : ", res.Id)
}

func UpdateDoc(client *elastic.Client, ctx context.Context) {
	// 更新document
	res, err := client.Update().Index("tweet").Type("tweeter").Id("1").Doc(map[string]interface{}{"user": "zorro","message":"修改后的message"}).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("update : ", res.Result)
}