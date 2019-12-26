package es

import (
	"context"
	"fmt"
	"github.com/olivere/elastic"
)

func InsertDocument(client *elastic.Client, ctx context.Context)  {
	person1 := `{
      "first_name" :  "John",
      "last_name" :   "Smith",
      "age" :         25,
      "about" :       "I love to go rock climbing",
      "interests":  [ "sports", "music" ]
  }`

	person2 := `{
               "first_name":  "Douglas",
               "last_name":   "Fir",
               "age":         35,
               "about":       "I like to build cabinets",
               "interests": [ "forestry" ]
            }`

	person3 := `{
               "first_name":  "Jane",
               "last_name":   "Smith",
               "age":         32,
               "about":       "I like to collect rock albums",
               "interests": [ "music" ]
            }`
	put1, err := client.Index().Index("user").Type("dept").Id("1").BodyString(person1).Do(ctx)
	put2, err := client.Index().Index("user").Type("dept").Id("2").BodyString(person2).Do(ctx)
	put3, err := client.Index().Index("user").Type("dept").Id("3").BodyString(person3).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("Indexed user [id: %s] to [index: %s], [type: %s]\n", put1.Id, put1.Index, put1.Type)
	fmt.Printf("Indexed user [id: %s] to [index: %s], [type: %s]\n", put2.Id, put2.Index, put2.Type)
	fmt.Printf("Indexed user [id: %s] to [index: %s], [type: %s]\n", put3.Id, put3.Index, put3.Type)
}

func Search1(client *elastic.Client, ctx context.Context)  {
	//搜索姓氏为Smith的人
	q := elastic.NewBoolQuery()
	q.Must(elastic.NewMatchQuery("last_name", "Smith"))
	res, err := client.Search().Query(q).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source))
	}
}

func Search2(client *elastic.Client, ctx context.Context)  {
	//搜索姓氏为Smith且年龄大于30的人
	q := elastic.NewBoolQuery()
	q.Must(elastic.NewMatchQuery("last_name", "Smith"))
	q.Filter(elastic.NewRangeQuery("age").Gte(30))
	res, err := client.Search().Query(q).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source))
	}
}

func Search3(client *elastic.Client, ctx context.Context)  {
	///搜索about包含rock climbing
	q := elastic.NewBoolQuery()
	q.Must(elastic.NewMatchQuery("about", "rock climbing"))
	res, err := client.Search().Query(q).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source),*hit.Score)
	}
}

func Search4(client *elastic.Client, ctx context.Context)  {
	//搜索about同时包含rock climbing
	q := elastic.NewBoolQuery()
	q.Must(elastic.NewMatchPhraseQuery("about", "rock climbing"))
	res, err := client.Search().Query(q).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source),*hit.Score)
	}
}

func Search5(client *elastic.Client, ctx context.Context)  {
	//高亮搜索
	h := elastic.NewHighlight()
	h.Fields(elastic.NewHighlighterField("about"))
	h.PreTags("<em>").PostTags("</em>")
	q := elastic.NewBoolQuery()
	q.Must(elastic.NewMatchPhraseQuery("about", "rock climbing"))
	res, err := client.Search().Query(q).Highlight(h).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source),*hit.Score,hit.Highlight)
	}
}