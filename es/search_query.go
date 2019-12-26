package es
import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic"
)

//curl -H "Content-Type: application/json" -XGET 'http://192.168.153.6:9200/_search?pretty' -d '

func BoolQuery(client *elastic.Client, ctx context.Context)  {
	q := elastic.NewBoolQuery()
	q = q.Must(elastic.NewTermQuery("genre", "Crime"))
	q = q.MustNot(elastic.NewRangeQuery("year").Gt(2000))
	q = q.Filter(elastic.NewTermQuery("director", "Francis Ford Coppola"))
	q = q.Should(elastic.NewTermQuery("year",1972),elastic.NewTermQuery("genre","crime"))	//加分项
	//sql := " ...  where genre=crime and year <= 2000 and director = Francis Ford Coppola"
	src, err := q.Source()
	data, _ := json.Marshal(src)
	fmt.Println(string(data))
	res, err := client.Search().Index("films").Query(q).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source))
	}
}

func IdsQuery(client *elastic.Client, ctx context.Context)  {
	q := elastic.NewIdsQuery("dept").Ids("1", "2")
	//sql := " select * from dept  where id in (1, 2)"
	src, err := q.Source()
	data, _ := json.Marshal(src)
	fmt.Println(string(data))
	res, err := client.Search().Index("user").Query(q).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source))
	}
}

func MatchQuery(client *elastic.Client, ctx context.Context)  {
	//分词
	q := elastic.NewMatchQuery("about", "i love")
	src, err := q.Source()
	data, _ := json.Marshal(src)
	fmt.Println(string(data))
	res, err := client.Search().Index("user").Query(q).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source))
	}
}

func MatchQueryWithOptions(client *elastic.Client, ctx context.Context)  {
	//按指定分析器和操作分词或不分词
	q := elastic.NewMatchQuery("about", "i love").Analyzer("whitespace").Operator("or")
	src, err := q.Source()
	data, _ := json.Marshal(src)
	fmt.Println(string(data))
	res, err := client.Search().Index("user").Query(q).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source))
	}
}


func MatchPhraseQuery(client *elastic.Client, ctx context.Context)  {
	//不分词
	q := elastic.NewMatchPhraseQuery("about", "i love")
	src, err := q.Source()
	data, _ := json.Marshal(src)
	fmt.Println(string(data))
	res, err := client.Search().Index("user").Query(q).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source))
	}
}


func MatchPhrasePrefixQuery(client *elastic.Client, ctx context.Context)  {
	//前缀不分词
	q := elastic.NewMatchPhrasePrefixQuery("about", "i love")
	src, err := q.Source()
	data, _ := json.Marshal(src)
	fmt.Println(string(data))
	res, err := client.Search().Index("user").Query(q).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source))
	}
}

func DisMaxQuery(client *elastic.Client, ctx context.Context)  {
	q := elastic.NewDisMaxQuery()		// or
	q = q.Query(elastic.NewTermQuery("year", 1993), elastic.NewTermQuery("year",1994))
	src, err := q.Source()
	data, _ := json.Marshal(src)
	fmt.Println(string(data))
	res, err := client.Search().Index("films").Query(q).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source))
	}
}


func MultiMatchQuery(client *elastic.Client, ctx context.Context)  {
	//多字段分词查询
	q := elastic.NewMultiMatchQuery("love music", "about", "interests")
	src, err := q.Source()
	data, _ := json.Marshal(src)
	fmt.Println(string(data))
	res, err := client.Search().Index("user").Query(q).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source))
	}
}

func MultiMatchQueryXFields(client *elastic.Client, ctx context.Context)  {
	//多字段 分词 根据type 查询
	//q := elastic.NewMultiMatchQuery("love music", "about", "interests").Type("best_fields")
	//q := elastic.NewMultiMatchQuery("Smith love music", "about", "interests").Type("most_fields")
	//q := elastic.NewMultiMatchQuery("Smith love music", "about", "interests").Type("cross_fields")
	//q := elastic.NewMultiMatchQuery("Smith love music", "about", "interests").Type("phrase")
	q := elastic.NewMultiMatchQuery("Smith love music", "about", "interests").Type("phrase_prefix")
	src, err := q.Source()
	data, _ := json.Marshal(src)
	fmt.Println(string(data))
	res, err := client.Search().Index("user").Query(q).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source))
	}
}


func PrefixQuery(client *elastic.Client, ctx context.Context)  {
	//找到对应字段 按照前缀查询
	q := elastic.NewPrefixQuery("last_name", "sm")
	src, err := q.Source()
	data, _ := json.Marshal(src)
	fmt.Println(string(data))
	res, err := client.Search().Index("user").Query(q).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source))
	}
}


func QueryStringQuery(client *elastic.Client, ctx context.Context)  {
	//全文 分词 搜索
	q := elastic.NewQueryStringQuery("the love")
	src, err := q.Source()
	data, _ := json.Marshal(src)
	fmt.Println(string(data))
	res, err := client.Search().Query(q).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source))
	}
}

func RangeQuery(client *elastic.Client, ctx context.Context)  {
	//按照范围搜索
	q := elastic.NewRangeQuery("year").Gte(1994).Lte(2000)
	src, err := q.Source()
	data, _ := json.Marshal(src)
	fmt.Println(string(data))
	res, err := client.Search().Query(q).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source))
	}
}

func RawStringQuery(client *elastic.Client, ctx context.Context)  {
	//使用原生语句查询
	q := elastic.NewRawStringQuery(`{"range":{"year":{"from":1994,"include_lower":true,"include_upper":true,"to":2000}}}`)
	src, err := q.Source()
	data, _ := json.Marshal(src)
	fmt.Println(string(data))
	res, err := client.Search().Query(q).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source))
	}
}


func TermQuery(client *elastic.Client, ctx context.Context)  {
	//精确查询
	q := elastic.NewTermQuery("about", "love")
	//q := elastic.NewTermQuery("about", "i love")   //按照一个整体去查询
	src, err := q.Source()
	data, _ := json.Marshal(src)
	fmt.Println(string(data))
	res, err := client.Search().Query(q).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source))
	}
}

func TermsQuery(client *elastic.Client, ctx context.Context)  {
	//多个值精确查询
	q := elastic.NewTermsQuery("about", "i", "love")
	src, err := q.Source()
	data, _ := json.Marshal(src)
	fmt.Println(string(data))
	res, err := client.Search().Query(q).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source))
	}
}

func SearchSortingBySorters(client *elastic.Client, ctx context.Context)  {
	//按照字段和评分排序
	q := elastic.NewMatchAllQuery()
	src, err := q.Source()
	data, _ := json.Marshal(src)
	fmt.Println(string(data))
	res, err := client.Search().Index("films").Query(q).SortBy(elastic.NewFieldSort("year").Asc(), elastic.NewScoreSort()).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, hit := range res.Hits.Hits {
		fmt.Println(string(*hit.Source))
	}
}