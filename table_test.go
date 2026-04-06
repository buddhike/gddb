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
		err := table.DeleteItemByKey(ctx, "item-1")
		assert.NoError(t, err)
	}

	{
		item := Item{
			ID:    "item-1",
			Name:  "Test item",
			Price: 10.30,
		}

		err := table.PutItem(ctx, item)
		assert.NoError(t, err)
	}

	{
		item, err := table.GetItemByKey(ctx, "item-1")

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

		err := table.UpdateItem(ctx, item)
		assert.NoError(t, err)
	}

	{
		item, err := table.GetItemByKey(ctx, "item-1")

		assert.NoError(t, err)

		assert.Equal(t, "item-1", item.ID)
		assert.Equal(t, "Test item", item.Name)
		assert.Equal(t, float32(11.50), item.Price)
	}

	{
		a, _ := table.GetItemByKey(ctx, "item-1")
		b, _ := table.GetItemByKey(ctx, "item-1")

		a.Price = 15
		l, err := table.FencedUpdateItem(ctx, &a)

		assert.NoError(t, err)
		assert.Equal(t, float32(15), l.Price)
		assert.Same(t, &a, l)

		b.Price = 20
		l, err = table.FencedUpdateItem(ctx, &b)
		assert.NoError(t, err)

		assert.Equal(t, 1, l.FencingToken)
		assert.Equal(t, float32(15), l.Price)
		assert.NotSame(t, l, &b)
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

	err := tbl.PutItem(ctx, p)
	assert.NoError(t, err)

	pa, err := tbl.GetItemByKey(ctx, "alice@corp.com")
	assert.NoError(t, err)
	assert.Equal(t, p, pa)
}

func TestPutItemOverwrite(t *testing.T) {
	ctx := t.Context()
	client := getDDBClient(t)
	ensureTable(t, client, "TestPutItemOverwrite", "id", "")

	type Item struct {
		ID    string  `dynamodbav:"id,omitempty" gddb:"hash"`
		Name  string  `dynamodbav:"name,omitempty"`
		Price float32 `dynamodbav:"price,omitempty"`
	}

	tbl := NewTable[Item]("TestPutItemOverwrite", client)

	require.NoError(t, tbl.PutItem(ctx, Item{ID: "x", Name: "first", Price: 1}))
	got, err := tbl.GetItemByKey(ctx, "x")
	require.NoError(t, err)
	assert.Equal(t, "first", got.Name)
	assert.Equal(t, float32(1), got.Price)

	require.NoError(t, tbl.PutItemOverwrite(ctx, Item{ID: "x", Name: "second", Price: 2}))
	got, err = tbl.GetItemByKey(ctx, "x")
	require.NoError(t, err)
	assert.Equal(t, "second", got.Name)
	assert.Equal(t, float32(2), got.Price)

	require.NoError(t, tbl.PutItemOverwrite(ctx, Item{ID: "y", Name: "only"}))
	got, err = tbl.GetItemByKey(ctx, "y")
	require.NoError(t, err)
	assert.Equal(t, "only", got.Name)
}

func ensureTable(t *testing.T, client *dynamodb.Client, name string, pk, sk string) {
	_, err := client.DeleteTable(t.Context(), &dynamodb.DeleteTableInput{TableName: aws.String(name)})
	if err != nil && !IsErrResourceNotFound(err) {
		require.NoError(t, err)
	}

	input := &dynamodb.CreateTableInput{
		TableName: aws.String(name),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(pk), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(pk), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	}

	if sk != "" {
		input.AttributeDefinitions = append(input.AttributeDefinitions, types.AttributeDefinition{AttributeName: aws.String(sk), AttributeType: types.ScalarAttributeTypeS})
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

func TestCompositeKey(t *testing.T) {
	ctx := t.Context()
	client := getDDBClient(t)
	ensureTable(t, client, "TestCompositeKey", "pk", "sk")

	type item struct {
		PK    string `dynamodbav:"pk" gddb:"hash"`
		SK    string `dynamodbav:"sk" gddb:"sort"`
		Value string `dynamodbav:"value"`
	}

	tbl := NewTable[item]("TestCompositeKey", client)
	ia := item{PK: "a", SK: "b", Value: "alice"}
	err := tbl.PutItem(ctx, ia)
	assert.NoError(t, err)

	ib, err := tbl.GetItemByCompositeKey(ctx, "a", "b")
	assert.NoError(t, err)
	assert.Equal(t, ia, ib)

	_, err = tbl.GetItemByCompositeKey(ctx, "a", "c")
	assert.ErrorIs(t, err, ErrItemNotFound)

	_, err = tbl.GetItemByCompositeKey(ctx, "c", "b")
	assert.ErrorIs(t, err, ErrItemNotFound)
}

func TestPutOrGetItem(t *testing.T) {
	ctx := t.Context()
	tablename := "TestPutOrGetItem"
	client := getDDBClient(t)
	ensureTable(t, client, tablename, "pk", "")

	type item struct {
		PK    string `dynamodbav:"pk" gddb:"hash"`
		Value string `dynamodbav:"value"`
	}

	tbl := NewTable[item](tablename, client)
	ia := item{"a", "b"}
	ib, err := tbl.PutOrGetItem(ctx, &ia)
	assert.NoError(t, err)
	assert.Same(t, &ia, ib)

	ic, err := tbl.PutOrGetItem(ctx, &ia)
	assert.NoError(t, err)
	assert.NotSame(t, &ia, &ic)
}
