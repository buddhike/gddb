package gddb

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
)

func TestEndToEnd(t *testing.T) {
	type Item struct {
		ID           string  `dynamodbav:"id,omitempty" gddb:"hash"`
		Name         string  `dynamodbav:"name,omitempty"`
		Price        float32 `dynamodbav:"price,omitempty"`
		FencingToken int     `dynamodbav:"fencingToken" gddb:"fence"`
	}

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")),
	)

	assert.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://localhost:8000")
	})

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("items"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		var inUse *types.ResourceInUseException
		if !errors.As(err, &inUse) {
			assert.NoError(t, err)
		}
	}

	table := NewTable[Item]("items", client)
	{
		err = DeleteByKey(ctx, table, "item-1")
		assert.NoError(t, err)
	}

	{
		item := Item{
			ID:    "item-1",
			Name:  "Test item",
			Price: 10.30,
		}

		err := InsertUnique(ctx, table, item)
		assert.NoError(t, err)
	}

	{
		item, err := FindByKey(ctx, table, "item-1")

		assert.NoError(t, err)
		assert.Equal(t, "item-1", item.ID)
		assert.Equal(t, "Test item", item.Name)
		assert.Equal(t, float32(10.30), item.Price)
	}

	{
		item := Item{
			ID:    "item-1",
			Price: 11.50,
		}

		err := UpdateByKey(ctx, table, "item-1", item)
		assert.NoError(t, err)
	}

	{
		item, err := FindByKey(ctx, table, "item-1")

		assert.NoError(t, err)

		assert.Equal(t, "item-1", item.ID)
		assert.Equal(t, "Test item", item.Name)
		assert.Equal(t, float32(11.50), item.Price)
	}

	{
		a, _ := FindByKey(ctx, table, "item-1")
		b, _ := FindByKey(ctx, table, "item-1")

		a.Price = 15
		l, err := UpdateByKeyOrGetLatest(ctx, table, "item-1", a)

		assert.NoError(t, err)
		assert.Equal(t, float32(15), l.Price)

		b.Price = 20
		l, err = UpdateByKeyOrGetLatest(ctx, table, "item-1", b)

		assert.NoError(t, err)

		assert.Equal(t, 1, l.FencingToken)
		assert.Equal(t, float32(15), l.Price)
	}
}
