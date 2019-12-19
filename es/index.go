package es

import (
	"context"
	"fmt"
	"github.com/olivere/elastic"
)

func CreateIndex(client *elastic.Client, ctx context.Context)  {
	//创建索引
	createdIndex, err := client.CreateIndex("test").BodyString(mappings).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(createdIndex)
}

func IsExist(client *elastic.Client, ctx context.Context)  {
	//校验索引是否存在
	isExist, err := client.IndexExists("test").Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(isExist)
}

func DeleteIndex(client *elastic.Client, ctx context.Context)  {
	//删除索引
	res, err := client.DeleteIndex("test").Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(res)
}