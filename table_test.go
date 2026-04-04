package gddb

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEndToEnd(t *testing.T) {
	ctx := t.Context()
	client := getDDBClient(t)
	ensureTable(t, client, "TestEndToEnd", "id", "")

	type Item struct {
		ID           string  `dynamodbav:"id,omitempty" gddb:"hash"`
		Name         string  `dynamodbav:"name,omitempty"`
		Price        float32 `dynamodbav:"price,omitempty"`
		FencingToken int     `dynamodbav:"fencingToken" gddb:"fence"`
	}

	table := NewTable[Item]("TestEndToEnd", client)

	{
		err := DeleteByKey(ctx, table, "item-1")
		assert.NoError(t, err)
	}

	{
		item := Item{
			ID:    "item-1",
			Name:  "Test item",
			Price: 10.30,
		}

		err := Insert(ctx, table, item)
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
		l, err := FencedUpdateByKey(ctx, table, "item-1", a)

		assert.NoError(t, err)
		assert.Equal(t, float32(15), l.Price)

		b.Price = 20
		l, err = FencedUpdateByKey(ctx, table, "item-1", b)

		assert.NoError(t, err)

		assert.Equal(t, 1, l.FencingToken)
		assert.Equal(t, float32(15), l.Price)
	}
}

func TestNestedTypes(t *testing.T) {
	type address struct {
		Street   string `dynamodbav:"street"`
		Suburb   string `dynamodbav:"suburb"`
		State    string `dynamodbav:"state"`
		Postcode int    `dynamodbav:"postcode"`
	}
	type person struct {
		ID   string   `dynamodbav:"id" gddb:"hash"`
		Name string   `dynamodbav:"name"`
		Home address  `dynamodbav:"address"`
		Work *address `dynamodbav:"work"`
	}

	ctx := t.Context()
	client := getDDBClient(t)
	ensureTable(t, client, "TestNestedTypes", "id", "")

	tbl := NewTable[person]("TestNestedTypes", client)

	home := address{Street: "66 Anne Street", Suburb: "Sydney", State: "NSW", Postcode: 2000}
	work := address{Street: "1 George Street", Suburb: "Sydney", State: "NSW", Postcode: 2000}
	p := person{ID: "alice@corp.com", Name: "Alice", Home: home, Work: &work}

	err := Insert(ctx, tbl, p)
	assert.NoError(t, err)

	pa, err := FindByKey(ctx, tbl, "alice@corp.com")
	assert.NoError(t, err)
	assert.Equal(t, p, pa)
}

func ensureTable(t *testing.T, client *dynamodb.Client, name string, pk, sk string) {
	_, err := client.DeleteTable(t.Context(), &dynamodb.DeleteTableInput{TableName: aws.String(name)})
	if err != nil && !IsErrResourceNotFound(err) {
		require.NoError(t, err)
	}

	input := &dynamodb.CreateTableInput{
		TableName: aws.String(name),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	}
	if sk != "" {
		input.KeySchema = append(input.KeySchema, types.KeySchemaElement{AttributeName: aws.String(sk), KeyType: types.KeyTypeRange})
	}
	_, err = client.CreateTable(t.Context(), input)
	require.NoError(t, err)
}

func getDDBClient(t *testing.T) *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")),
	)

	assert.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://localhost:8000")
	})

	return client
}
