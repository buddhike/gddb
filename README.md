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

## Comparison with the AWS SDK

gddb is a thin layer on top of the official v2 client: it still uses `attributevalue` and `expression` where DynamoDB requires them (for example `Query`). For the common **single-item** patterns, it collapses repeated boilerplate—table name, key attribute names, marshal/unmarshal, empty-item checks, and wiring expression outputs into `PutItem` / `GetItem` / `UpdateItem` inputs—into one typed call. **Updates** build the DynamoDB key from the hash (and sort) fields on the value you pass, so you do not pass the key as a separate argument.

Assume `Item` and `table` are set up as above, `ctx` is a `context.Context`, and `client` is a `*dynamodb.Client`.

The tables below use HTML so the **AWS SDK v2** and **gddb** snippets stay in two columns (for example on GitHub). Snippets omit imports.

### PutItem (insert only if key is new)

With the SDK you marshal the struct, build a condition that the partition (and sort) key attributes do not exist, then copy names and values into `PutItemInput`. With gddb, key attribute names come from struct tags.

<table>
<thead>
<tr>
<th align="left" width="50%">AWS SDK v2</th>
<th align="left" width="50%">gddb</th>
</tr>
</thead>
<tbody>
<tr>
<td valign="top">

<pre lang="go"><code>item := Item{ID: "a", Name: "foo"}
av, err := attributevalue.MarshalMap(item)
if err != nil {
    return err
}
cond, err := expression.NewBuilder().WithCondition(
    expression.AttributeNotExists(expression.Name("id")),
).Build()
if err != nil {
    return err
}
_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
    TableName:                 aws.String("items"),
    Item:                      av,
    ConditionExpression:       cond.Condition(),
    ExpressionAttributeNames:  cond.Names(),
    ExpressionAttributeValues: cond.Values(),
})
</code></pre>

</td>
<td valign="top">

<pre lang="go"><code>err := gddb.PutItem(ctx, table, Item{ID: "a", Name: "foo"})
</code></pre>

</td>
</tr>
</tbody>
</table>

### PutItem (overwrite; no condition)

To replace an item or create it in one write, omit `ConditionExpression`. The whole item you send becomes the stored row (unspecified `dynamodbav` fields are not merged with an existing item—they disappear unless present in the struct you marshal).

<table>
<thead>
<tr>
<th align="left" width="50%">AWS SDK v2</th>
<th align="left" width="50%">gddb</th>
</tr>
</thead>
<tbody>
<tr>
<td valign="top">

<pre lang="go"><code>item := Item{ID: "a", Name: "foo"}
av, err := attributevalue.MarshalMap(item)
if err != nil {
    return err
}
_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
    TableName: aws.String("items"),
    Item:      av,
})
</code></pre>

</td>
<td valign="top">

<pre lang="go"><code>err := gddb.PutItemOverwrite(ctx, table,
    Item{ID: "a", Name: "foo"})
</code></pre>

</td>
</tr>
</tbody>
</table>

### GetItem by partition key

The SDK returns an empty map when the item is missing; you check that and unmarshal yourself. gddb returns a typed value and `gddb.ErrItemNotFound` without repeating the hash key name.

<table>
<thead>
<tr>
<th align="left" width="50%">AWS SDK v2</th>
<th align="left" width="50%">gddb</th>
</tr>
</thead>
<tbody>
<tr>
<td valign="top">

<pre lang="go"><code>idAV, err := attributevalue.Marshal("a")
if err != nil {
    return Item{}, err
}
out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
    TableName: aws.String("items"),
    Key:       map[string]types.AttributeValue{"id": idAV},
})
if err != nil {
    return Item{}, err
}
if len(out.Item) == 0 {
    return Item{}, errors.New("not found") // or a sentinel
}
var item Item
if err := attributevalue.UnmarshalMap(out.Item, &item); err != nil {
    return Item{}, err
}
</code></pre>

</td>
<td valign="top">

<pre lang="go"><code>item, err := gddb.GetItemByKey(ctx, table, "a")
</code></pre>

</td>
</tr>
</tbody>
</table>

### UpdateItem (SET non-key fields from a struct)

The SDK needs an `UpdateExpression`, a separate key map, and marshaling per field. gddb marshals a single `value` struct: it uses the hash (and sort, if any) attributes from that struct for the item key, sets every **other** attribute from the same struct, flattens nested maps into dotted paths, and omits the key attributes from the `SET` clause.

<table>
<thead>
<tr>
<th align="left" width="50%">AWS SDK v2</th>
<th align="left" width="50%">gddb</th>
</tr>
</thead>
<tbody>
<tr>
<td valign="top">

<pre lang="go"><code>pk, err := attributevalue.Marshal("a")
if err != nil {
    return err
}
nameAV, err := attributevalue.Marshal("foo")
if err != nil {
    return err
}
priceAV, err := attributevalue.Marshal(float32(9.99))
if err != nil {
    return err
}
ub := expression.UpdateBuilder{}.
    Set(expression.Name("name"), expression.Value(nameAV)).
    Set(expression.Name("price"), expression.Value(priceAV))
expr, err := expression.NewBuilder().WithUpdate(ub).Build()
if err != nil {
    return err
}
_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
    TableName:                 aws.String("items"),
    Key:                       map[string]types.AttributeValue{"id": pk},
    UpdateExpression:          expr.Update(),
    ExpressionAttributeNames:  expr.Names(),
    ExpressionAttributeValues: expr.Values(),
})
</code></pre>

</td>
<td valign="top">

<pre lang="go"><code>err := gddb.UpdateItem(ctx, table,
    Item{ID: "a", Name: "foo", Price: 9.99})
</code></pre>

</td>
</tr>
</tbody>
</table>

For a **composite primary key**, include both hash and sort fields on `value` (for example `Item{PK: "a", SK: "b", ...}`); `UpdateItem` and `FencedUpdateItem` use them for the DynamoDB key the same way.

### Query

Both sides build `expr` with `expression` the same way (key condition, optional projection/filter). gddb mainly removes the **per-item unmarshal loop**.

<table>
<thead>
<tr>
<th align="left" width="50%">AWS SDK v2</th>
<th align="left" width="50%">gddb</th>
</tr>
</thead>
<tbody>
<tr>
<td valign="top">

<pre lang="go"><code>input := dynamodb.QueryInput{
    TableName:                 aws.String("items"),
    KeyConditionExpression:    expr.KeyCondition(),
    ExpressionAttributeNames:  expr.Names(),
    ProjectionExpression:      expr.Projection(),
    FilterExpression:          expr.Filter(),
    ExpressionAttributeValues: expr.Values(),
}
result, err := client.Query(ctx, &input)
if err != nil {
    return nil, err
}
rows := make([]Item, len(result.Items))
for i, m := range result.Items {
    if err := attributevalue.UnmarshalMap(m, &rows[i]); err != nil {
        return nil, err
    }
}
</code></pre>

</td>
<td valign="top">

<pre lang="go"><code>rows, err := gddb.Query(ctx, table, expr)
</code></pre>

</td>
</tr>
</tbody>
</table>

**Summary:** Where the SDK forces you to repeat table name, key names, AV maps, and expression fields, gddb encodes key metadata once on `NewTable` and keeps call sites short for put-if-absent, unconditional full-item puts (`PutItemOverwrite`), get-by-key, struct-shaped updates (key read from the value struct), deletes, and query result decoding.

## Operations

All functions take a `context.Context` and a `*Table[T]`. For helpers that take a key by itself (`GetItemByKey`, `DeleteItemByKey`, etc.), key arguments are generic (`K`, `PK`, `SK`) and are marshaled with the AWS `attributevalue` package. For `UpdateItem` / `FencedUpdateItem`, the partition key (and sort key, if the table has one) must be set on the `value` struct so the library can build the DynamoDB key and the update expression.

| Function | Description |
|----------|-------------|
| `PutItem` | `PutItem` with a condition that the primary key does not exist yet. |
| `PutItemOverwrite` | `PutItem` with no condition: creates or fully replaces the item for that primary key. |
| `PutOrGetItem` | Tries to insert `*item`; if the key already exists, returns the stored row and does not write. Compare the returned pointer to the input pointer: same pointer means the write happened; different pointer means an existing item was loaded. |
| `GetItemByKey` | `GetItem` by partition key. Returns `gddb.ErrItemNotFound` if missing. |
| `GetItemByCompositeKey` | `GetItem` by partition + sort key. |
| `Query` | Runs `Query` with a pre-built `expression.Expression` (key condition, projection, filter); results are unmarshaled into `[]T`. |
| `UpdateItem` | `UpdateItem` with key derived from `value`: hash (and sort) fields on `value` define the item key; every other field is `SET` in the update. Nested structs are flattened into dotted paths. |
| `FencedUpdateItem` | Optimistic lock: same key and `SET` behavior as `UpdateItem`, but increments the fence and adds a condition on the previous fence value. On conflict, the current item is unmarshaled and returned with a **nil** error. Pass `*value`; compare the returned pointer to the input—same means success, different means retry with the returned item. Requires a `gddb:"fence"` field. |
| `DeleteItemByKey` / `DeleteItemByCompositeKey` | `DeleteItem` by key(s). |

**Conditional writes:** Use `gddb.IsErrConditionalCheckFailed(err)` to detect `types.ConditionalCheckFailedException` (for example after `PutItem` when the key already exists). Unconditional puts (`PutItemOverwrite`) do not use a condition, so that error does not apply.

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

// Create or replace whole item (no attribute_not_exists check)
_ = gddb.PutItemOverwrite(ctx, table, Item{ID: "a", Name: "bar", Price: 1})

item, err := gddb.GetItemByKey(ctx, table, "a")
if errors.Is(err, gddb.ErrItemNotFound) {
    // handle missing
}

// Partial update: key comes from value; other fields are SET (nested maps flattened)
_ = gddb.UpdateItem(ctx, table, Item{ID: "a", Price: 9.99})

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
