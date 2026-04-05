# gddb

A small Go library that wraps the [AWS SDK for Go v2 DynamoDB client](https://aws.github.io/aws-sdk-go-v2/docs/) with **generic** helpers: you model rows as `struct` types, tag keys and optional optimistic-lock fields, and call typed functions for common read/write patterns.

## Install

```bash
go get github.com/buddhike/gddb
```

Requires **Go 1.25+** (see `go.mod`). The SDK pulls in `github.com/aws/aws-sdk-go-v2/service/dynamodb` and related modules.

## Model your table

Use standard `dynamodbav` tags for attribute names, and **`gddb` tags** so the library knows which fields are the partition key, sort key, and optional fencing token:

| `gddb` tag | Meaning |
|------------|---------|
| `hash` | Partition (hash) key attribute name is taken from this field’s `dynamodbav` name (or struct field name). |
| `sort` | Sort (range) key, for composite primary keys. Omit if the table has only a partition key. |
| `fence` | Numeric attribute used for optimistic locking in fenced updates. |

Example (partition key only):

```go
type Item struct {
    ID           string  `dynamodbav:"id,omitempty" gddb:"hash"`
    Name         string  `dynamodbav:"name,omitempty"`
    Price        float32 `dynamodbav:"price,omitempty"`
    FencingToken int     `dynamodbav:"fencingToken" gddb:"fence"`
}
```

Create a handle bound to your table name and client:

```go
table := gddb.NewTable[Item]("items", dynamoClient)
```

## Operations

All functions take a `context.Context` and a `*Table[T]`. Key arguments are generic (`K`, `PK`, `SK`) and are marshaled with the AWS attributevalue package, so you can pass simple scalars or types that marshal to the correct DynamoDB attribute shape.

| Function | Description |
|----------|-------------|
| `PutItem` | `PutItem` with a condition that the primary key does not exist yet. |
| `PutOrGetItem` | Tries to insert `*item`; if the key already exists, returns the stored row and does not write. Compare the returned pointer to the input pointer: same pointer means the write happened; different pointer means an existing item was loaded. |
| `GetItemByKey` | `GetItem` by partition key. Returns `gddb.ErrItemNotFound` if missing. |
| `GetItemByCompositeKey` | `GetItem` by partition + sort key. |
| `Query` | Runs `Query` with a pre-built `expression.Expression` (key condition, projection, filter); results are unmarshaled into `[]T`. |
| `UpdateItemByKey` / `UpdateItemByCompositeKey` | `UpdateItem`: sets every attribute from the value struct **except** the hash key. Nested structs are flattened into dotted paths for the update expression. |
| `FencedUpdateItemByKey` / `FencedUpdateItemByCompositeKey` | Optimistic lock: increments the fence and applies the update only if the fence still matches. On conflict, the current item is unmarshaled and returned with a **nil** error. Pass a `*T`; compare the returned pointer to the input pointer—same means the update succeeded, different means retry with the returned item. Requires a `gddb:"fence"` field. |
| `DeleteItemByKey` / `DeleteItemByCompositeKey` | `DeleteItem` by key(s). |

**Conditional writes:** Use `gddb.IsErrConditionalCheckFailed(err)` to detect `types.ConditionalCheckFailedException` (for example after `PutItem` when the key already exists).

## Example

```go
ctx := context.Background()
table := gddb.NewTable[Item]("items", client)

// Insert if absent
if err := gddb.PutItem(ctx, table, Item{ID: "a", Name: "foo"}); err != nil {
    if gddb.IsErrConditionalCheckFailed(err) {
        // already exists
    }
}

item, err := gddb.GetItemByKey(ctx, table, "a")
if errors.Is(err, gddb.ErrItemNotFound) {
    // handle missing
}

// Partial update: non-key fields from value are SET (nested maps flattened)
_ = gddb.UpdateItemByKey(ctx, table, "a", Item{ID: "a", Price: 9.99})

// Query: build with github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression
key := expression.Key("id").Equal(expression.Value("a"))
expr, _ := expression.NewBuilder().
    WithKeyCondition(key).
    Build()
rows, err := gddb.Query(ctx, table, expr)
```

## Local development and tests

The repo includes `docker-compose.yml` running [DynamoDB Local](https://docs.aws.amazon.com/amazon-dynamodb/latest/developerguide/DynamoDBLocal.html) on port **8000**. Point the SDK at it (as in `table_test.go`):

```go
client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})
```

Run tests (DynamoDB Local must be up):

```bash
docker compose up -d
go test ./...
```
