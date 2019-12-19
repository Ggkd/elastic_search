package es

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/olivere/elastic"
	"strings"
	"time"
)

func SearchWithBoolQuery(client *elastic.Client, ctx context.Context)  {
	query := elastic.NewBoolQuery()
	query = query.Must(elastic.NewTermQuery("user", "luffy"))
	query = query.Filter(elastic.NewTermQuery("age", 11))
	src, err := query.Source()
	if err != nil {
		fmt.Println(err)
		return
	}
	data, err := json.MarshalIndent(src, "", "  ")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(data))
}


const (
	indexName = "films"
	mapping   = `
	{
		"settings":{
			"number_of_shards":1,
			"number_of_replicas":0
		},
		"mappings":{
			"film":{
				"properties":{
					"title":{
						"type":"keyword"
					},
					"genre":{
						"type":"keyword"
					},
					"year":{
						"type":"long"
					},
					"director":{
						"type":"keyword"
					}
				}
			}
		}
	}
	`
)


type Film struct {
	Title    string   `json:"title"`
	Genre    []string `json:"genre"`
	Year     int      `json:"year"`
	Director string   `json:"director"`
}

func createAndPopulateIndex(client *elastic.Client) error {
	ctx := context.Background()
	exists, err := client.IndexExists(indexName).Do(ctx)
	if err != nil {
		return err
	}
	if exists {
		_, err = client.DeleteIndex(indexName).Do(ctx)
		if err != nil {
			return err
		}
	}
	// 创建索引
	_, err = client.CreateIndex(indexName).Body(mapping).Do(ctx)
	if err != nil {
		return err
	}
	// 填充数据
	films := []Film{
		{Title: "The Shawshank Redemption", Genre: []string{"Crime", "Drama"}, Year: 1994, Director: "Frank Darabont"},
		{Title: "The Godfather", Genre: []string{"Crime", "Drama"}, Year: 1972, Director: "Francis Ford Coppola"},
		{Title: "The Godfather: Part II", Genre: []string{"Crime", "Drama"}, Year: 1974, Director: "Francis Ford Coppola"},
		{Title: "The Dark Knight", Genre: []string{"Action", "Crime", "Drama"}, Year: 2008, Director: "Christopher Nolan"},
		{Title: "12 Angry Men", Genre: []string{"Crime", "Drama"}, Year: 1957, Director: "Sidney Lumet"},
		{Title: "Schindler's List", Genre: []string{"Biography", "Drama", "History"}, Year: 1993, Director: "Steven Spielberg"},
		{Title: "The Lord of the Rings: The Return of the King", Genre: []string{"Adventure", "Drama", "Fantasy"}, Year: 2003, Director: "Peter Jackson"},
		{Title: "Pulp Fiction", Genre: []string{"Crime", "Drama"}, Year: 1994, Director: "Quentin Tarantino"},
		{Title: "Il buono, il brutto, il cattivo", Genre: []string{"Western"}, Year: 1966, Director: "Sergio Leone"},
		{Title: "Fight Club", Genre: []string{"Drama"}, Year: 1999, Director: "David Fincher"},
		{Title: "功夫", Genre: []string{"喜剧"}, Year: 2004, Director: "周星驰"},
	}
	for _, film := range films {
		_, err = client.Index().
			Index(indexName).
			Type("film").
			BodyJson(film).
			Do(ctx)
		if err != nil {
			return err
		}
	}
	_, err = client.Flush(indexName).WaitIfOngoing(true).Do(ctx)
	return err
}

type Finder struct {
	genre      string
	year       int
	from, size int
	sort       []string
	pretty     bool
}

//创建一个finder示例
func NewFinder() *Finder {
	return &Finder{}
}

//设置电影风格
func (f *Finder) Genre(genre string) *Finder {
	f.genre = genre
	return f
}

//设置年份
func (f *Finder) Year(year int) *Finder {
	f.year = year
	return f
}

//设置起始页码
func (f *Finder) From(pageNo int) *Finder {
	f.from = pageNo
	return f
}

//设置每页展示的数据量
func (f *Finder) Size(pageSize int) *Finder {
	f.size = pageSize
	return f
}

//设置排序字段。在字段前加 - ，表示该字段倒序
func (f *Finder) Sort(sort... string) *Finder {
	if sort == nil {
		f.sort = make([]string, 0)
	}
	f.sort = append(f.sort, sort...)
	return f
}

//设置是否美化显示
func (f *Finder) Pretty(pretty bool) *Finder {
	f.pretty = pretty
	return f
}


// 搜搜返回的结果
type FinderResponse struct {
	Total          int64
	Films          []*Film
	Genres         map[string]int64
	YearsAndGenres map[int][]NameCount // {1994: [{"Crime":1}, {"Drama":2}], ...}
}

// 计数
type NameCount struct {
	Name  string
	Count int64
}

//设置查询
func (f *Finder) query(service *elastic.SearchService) *elastic.SearchService {
	if f.genre == "" && f.year == 0 {
		service = service.Query(elastic.NewMatchAllQuery())
	}
	q := elastic.NewBoolQuery()
	if f.genre != "" {
		q = q.Must(elastic.NewTermQuery("genre", f.genre))
	}
	if f.year > 0 {
		q = q.Must(elastic.NewTermQuery("year", f.year))
	}
	service = service.Query(q)
	return service
}

//设置聚合
func (f *Finder) aggregation(service *elastic.SearchService) *elastic.SearchService {
	//根据genre聚合
	agg := elastic.NewTermsAggregation().Field("genre")
	service = service.Aggregation("all_genres", agg)
	//根据year聚合，并添加到genre聚合中
	subAgg := elastic.NewTermsAggregation().Field("genre")
	agg = elastic.NewTermsAggregation().Field("year").SubAggregation("genres_by_year", subAgg)
	service = service.Aggregation("years_and_genres", agg)
	return service
}

//设置排序
func (f *Finder) sorting(service *elastic.SearchService) *elastic.SearchService {
	if len(f.sort) == 0{
		service = service.Sort("_score", false)
		return service
	}
	for _, sort := range f.sort {
		var field string
		var asc bool
		sort = strings.TrimSpace(sort)
		if strings.HasPrefix(sort, "-") {
			field = sort[1:]
			asc = false
		}else {
			field = sort
			asc = true
		}
		service = service.Sort(field, asc)
	}
	return service
}

//设置分页器
func (f *Finder) paginate(service *elastic.SearchService) *elastic.SearchService {
	if f.from > 0 {
		service = service.From(f.from)
	}
	if f.size > 0 {
		service = service.Size(f.size)
	}
	return service
}

//反序列化搜索结果
func (f *Finder) decodeFilms(res *elastic.SearchResult) ([]*Film, error) {
	if res.TotalHits() == 0 || res == nil {
		return nil, nil
	}
	var films []*Film
	for _, hit := range res.Hits.Hits {
		film := new(Film)
		err := json.Unmarshal(*hit.Source, film)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		films = append(films, film)
	}
	return films, nil
}

//执行搜索
func (f *Finder) Find(ctx context.Context, client *elastic.Client) (FinderResponse, error) {
	var resp FinderResponse
	search := client.Search().Index(indexName).Type("film").Pretty(f.pretty)
	search = f.query(search)
	search = f.aggregation(search)
	search = f.sorting(search)
	search = f.paginate(search)
	//执行查询
	searchResult, err := search.Do(ctx)
	if err != nil {
		fmt.Println(err)
		return resp, err
	}
	films, err := f.decodeFilms(searchResult)
	if err != nil {
		fmt.Println(err)
		return resp, err
	}
	resp.Films = films
	resp.Total = searchResult.TotalHits()
	//反序列化聚合的结果
	if agg, found := searchResult.Aggregations.Terms("all_genres"); found {
		resp.Genres = make(map[string]int64, 0)
		for _, bucket := range agg.Buckets {
			resp.Genres[bucket.Key.(string)] = bucket.DocCount
		}
	}
	if agg, found := searchResult.Aggregations.Terms("years_and_genres"); found {
		resp.YearsAndGenres = make(map[int][]NameCount)
		for _, bucket := range agg.Buckets {
			//json没有int类型，都转化为了float64
			floatValue, ok := bucket.Key.(float64)
			if !ok {
				return resp, errors.New("expected float64")
			}
			var (
				year          = int(floatValue)
				genresForYear []NameCount
			)
			if subAgg, found := bucket.Terms("genres_by_year"); found {
				for _, subBucket := range subAgg.Buckets {
					genresForYear = append(genresForYear, NameCount{
						Name: subBucket.Key.(string),
						Count: subBucket.DocCount,
					})
				}
			}
			resp.YearsAndGenres[year] = genresForYear
		}
	}
	return resp, nil
}


func FinderMain(client *elastic.Client)  {
	// 添加一些数据
	err := createAndPopulateIndex(client)
	if err != nil {
		fmt.Println(err)
		return
	}

	//创建一个Finder
	f := NewFinder()
	f = f.From(0).Size(100).Pretty(true)

	//设置5秒超时
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	//执行搜索
	result, err := f.Find(ctx, client)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("films count:",result.Total)
	fmt.Println("===========Films Found==============")
	for i, film := range result.Films {
		prefix := "├"
		if i == len(result.Films)-1 {
			prefix = "└"
		}
		fmt.Printf("%s %s from %d\n", prefix, film.Title, film.Year)
	}
	fmt.Println("=========Broken down by genre=========")
	for genre, count := range result.Genres {
		fmt.Printf("- %2d× %s\n", count, genre)
	}
	fmt.Println("=======Broken down by year and genres=======")
	for year, genre := range result.YearsAndGenres {
		fmt.Printf("- %4d\n", year)
		for i, nc := range genre {
			prefix := "├"
			if i == len(genre)-1 {
				prefix = "└"
			}
			fmt.Printf("  %s%2d× %s\n", prefix, nc.Count, nc.Name)
		}
	}
}