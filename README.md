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

All functions take a `context.Context` and a `*Table[T]`.

| Function | Description |
|----------|-------------|
| `InsertUnique` | `PutItem` with a condition that the primary key does not exist yet. |
| `FindByKey` | `GetItem` by partition key. Returns `gddb.ErrItemNotFound` if missing. |
| `FindByCompositeKey` | `GetItem` by partition + sort key. |
| `Query` | Runs `Query` with a pre-built `expression.Expression` (key condition, projection, filter); results are unmarshaled into `[]T`. |
| `UpdateByKey` / `UpdateByCompositeKey` | `UpdateItem`: sets every attribute from the value struct **except** the hash key. Nested structs are flattened into dotted paths for the update expression. |
| `FencedUpdateByKey` / `FencedUpdateByCompositeKey` | Optimistic lock: increments the fence and applies the update only if the fence still matches. On conflict, DynamoDB returns the current item; the function unmarshals it into `T` and returns it with a **nil** error so callers can retry. Requires a `gddb:"fence"` field. |
| `DeleteByKey` / `DeleteByCompositeKey` | `DeleteItem` by key(s). |

**Conditional writes:** Use `gddb.IsErrConditionalCheckFailed(err)` to detect `types.ConditionalCheckFailedException` (for example after `InsertUnique` when the key already exists).

## Example

```go
ctx := context.Background()
table := gddb.NewTable[Item]("items", client)

// Insert if absent
if err := gddb.InsertUnique(ctx, table, Item{ID: "a", Name: "foo"}); err != nil {
    if gddb.IsErrConditionalCheckFailed(err) {
        // already exists
    }
}

item, err := gddb.FindByKey(ctx, table, "a")
if errors.Is(err, gddb.ErrItemNotFound) {
    // handle missing
}

// Partial update: non-key fields from value are SET (nested maps flattened)
_ = gddb.UpdateByKey(ctx, table, "a", Item{ID: "a", Price: 9.99})

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
