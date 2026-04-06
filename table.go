package gddb

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	ErrItemExists   error = fmt.Errorf("item already exists")
	ErrItemNotFound error = fmt.Errorf("item not found")
)

type Table[T any] struct {
	tableName      string
	hashAttribute  string
	sortAttribute  string
	fenceAttribute string
	client         *dynamodb.Client
}

// NewTable returns a [Table] for the given DynamoDB table name and client.
// Hash, sort, and fence attribute names are inferred from T's struct fields using
// the gddb struct tag ("hash", "sort", "fence") together with dynamodbav names.
func NewTable[T any](tableName string, client *dynamodb.Client) *Table[T] {
	t := reflect.TypeFor[T]()
	hash, sort, fence := discoverAttributes(t)
	if hash == "" {
		panic("type must have a field with gddb:\"hash\" tag")
	}

	return &Table[T]{
		tableName:      tableName,
		hashAttribute:  hash,
		sortAttribute:  sort,
		fenceAttribute: fence,
		client:         client,
	}
}

func discoverAttributes(t reflect.Type) (string, string, string) {
	var hash, sort, fence string

	for i := range t.NumField() {
		f := t.Field(i)
		gddbtag := f.Tag.Get("gddb")
		ddbavtag := f.Tag.Get("dynamodbav")
		effectivename := f.Name
		if ddbavtag != "" {
			parts := strings.Split(ddbavtag, ",")
			effectivename = parts[0]
		}
		switch gddbtag {
		case "hash":
			hash = effectivename
		case "sort":
			sort = effectivename
		case "fence":
			fence = effectivename
		}
	}
	return hash, sort, fence
}

// PutItem writes item with a condition that the primary key does not already exist.
// It fails if an item with the same hash key (and sort key, when the table has one) is present.
func PutItem[T any](ctx context.Context, t *Table[T], item T) error {
	m, err := attributevalue.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("marshal item to av failed: %w", err)
	}

	cond := expression.AttributeNotExists(expression.Name(t.hashAttribute))
	if t.sortAttribute != "" {
		cond = cond.And(expression.AttributeNotExists(expression.Name(t.sortAttribute)))
	}
	unique, err := expression.NewBuilder().WithCondition(cond).Build()
	if err != nil {
		return fmt.Errorf("ddb build expression failed: %w", err)
	}

	pii := dynamodb.PutItemInput{
		TableName:                 &t.tableName,
		Item:                      m,
		ConditionExpression:       unique.Condition(),
		ExpressionAttributeNames:  unique.Names(),
		ExpressionAttributeValues: unique.Values(),
	}

	_, err = t.client.PutItem(ctx, &pii)
	if err != nil {
		return fmt.Errorf("ddb put item failed: %w", err)
	}

	return nil
}

// PutItemOverwrite writes item with PutItem and no condition expression.
// If an item with the same primary key already exists, it is fully replaced by item.
func PutItemOverwrite[T any](ctx context.Context, t *Table[T], item T) error {
	m, err := attributevalue.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("marshal item to av failed: %w", err)
	}

	_, err = t.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &t.tableName,
		Item:      m,
	})
	if err != nil {
		return fmt.Errorf("ddb put item failed: %w", err)
	}

	return nil
}

// PutOrGetItem attempts to insert the given item into the table, enforcing uniqueness on the primary key:
// if no item with the same key exists, the item is written and returned. If an item with the same key
// already exists, it is returned instead, and no write occurs. To determine whether the write actually took place,
// compare the returned pointer with the input pointer: if they are the same, the write was performed;
// if they differ, the item already existed and was loaded. On error, a non-nil error is returned.
func PutOrGetItem[T any](ctx context.Context, t *Table[T], item *T) (*T, error) {
	var v T
	m, err := attributevalue.MarshalMap(item)
	if err != nil {
		return nil, fmt.Errorf("marshal item to av failed: %w", err)
	}

	cond := expression.AttributeNotExists(expression.Name(t.hashAttribute))
	if t.sortAttribute != "" {
		cond = cond.And(expression.AttributeNotExists(expression.Name(t.sortAttribute)))
	}
	unique, err := expression.NewBuilder().WithCondition(cond).Build()
	if err != nil {
		return nil, fmt.Errorf("ddb build expression failed: %w", err)
	}

	pii := dynamodb.PutItemInput{
		TableName:                           &t.tableName,
		Item:                                m,
		ConditionExpression:                 unique.Condition(),
		ExpressionAttributeNames:            unique.Names(),
		ExpressionAttributeValues:           unique.Values(),
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	}

	_, err = t.client.PutItem(ctx, &pii)
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			err = attributevalue.UnmarshalMap(ccf.Item, &v)
			if err != nil {
				return nil, fmt.Errorf("unmarshal current value failed: %w", err)
			}
			return &v, nil
		}
		return nil, fmt.Errorf("ddb put item failed: %w", err)
	}

	return item, nil
}

// GetItemByKey loads a single item by partition key. It returns [ErrItemNotFound] when no item exists.
func GetItemByKey[T any, K any](ctx context.Context, t *Table[T], key K) (T, error) {
	var v T
	mk, err := attributevalue.Marshal(key)
	if err != nil {
		return v, fmt.Errorf("marshal key to av failed: %w", err)
	}

	getItemInput := &dynamodb.GetItemInput{
		TableName: &t.tableName,
		Key: map[string]types.AttributeValue{
			t.hashAttribute: mk,
		},
	}

	return ddbGetItem(ctx, t, getItemInput)
}

// GetItemByCompositeKey loads a single item by partition and sort key.
// It returns [ErrItemNotFound] when no item exists.
func GetItemByCompositeKey[T any, PK any, SK any](ctx context.Context, t *Table[T], pk PK, sk SK) (T, error) {
	var v T
	mpk, err := attributevalue.Marshal(pk)
	if err != nil {
		return v, fmt.Errorf("marshal pk to av failed: %w", err)
	}

	msk, err := attributevalue.Marshal(sk)
	if err != nil {
		return v, fmt.Errorf("marshal sk to av failed: %w", err)
	}

	getItemInput := &dynamodb.GetItemInput{
		TableName: &t.tableName,
		Key: map[string]types.AttributeValue{
			t.hashAttribute: mpk,
			t.sortAttribute: msk,
		},
	}

	return ddbGetItem(ctx, t, getItemInput)
}

func ddbGetItem[T any](ctx context.Context, t *Table[T], input *dynamodb.GetItemInput) (T, error) {
	var v T
	result, err := t.client.GetItem(ctx, input)
	if err != nil {
		return v, fmt.Errorf("ddb get item failed: %w", err)
	}
	if len(result.Item) == 0 {
		return v, ErrItemNotFound
	}

	err = attributevalue.UnmarshalMap(result.Item, &v)
	if err != nil {
		return v, fmt.Errorf("unmarshal av map to type T failed: %w", err)
	}

	return v, err
}

// Query runs a DynamoDB Query using expr for key condition, projection, and filter.
// Results are unmarshaled into []T.
func Query[T any](ctx context.Context, t *Table[T], expr expression.Expression) ([]T, error) {
	input := dynamodb.QueryInput{
		TableName:                 &t.tableName,
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ProjectionExpression:      expr.Projection(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeValues: expr.Values(),
	}

	result, err := t.client.Query(ctx, &input)
	if err != nil {
		return nil, fmt.Errorf("ddb query failed: %w", err)
	}

	r := make([]T, len(result.Items))
	for i, m := range result.Items {
		var v T
		err = attributevalue.UnmarshalMap(m, &v)
		if err != nil {
			return nil, fmt.Errorf("ddb umarshal av map to type T failed: %w", err)
		}
		r[i] = v
	}

	return r, nil
}

func updateItem[T any](ctx context.Context, t *Table[T], value *T, fenced bool) (*dynamodb.UpdateItemOutput, error) {
	iav, err := attributevalue.MarshalMap(value)
	if err != nil {
		return nil, fmt.Errorf("marshal type T to av map failed: %w", err)
	}

	var originalFencingToken int64
	iav = flattenMapRecursive(t, "", iav)
	update := expression.UpdateBuilder{}

	if fenced {
		if ft, ok := iav[t.fenceAttribute]; ok {
			n := ft.(*types.AttributeValueMemberN)
			originalFencingToken, err = strconv.ParseInt(n.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parse fencing token failed: %w", err)
			}

			iav[t.fenceAttribute], err = attributevalue.Marshal(originalFencingToken + 1)
			if err != nil {
				return nil, fmt.Errorf("marshal fencing token to av failed: %w", err)
			}
		} else {
			return nil, fmt.Errorf("fenced update requires requires setting gddb fence attribute on a field in type T")
		}
	}

	for k, v := range iav {
		if k != t.hashAttribute {
			update = update.Set(expression.Name(k), expression.Value(v))
		}
	}

	b := expression.NewBuilder().WithUpdate(update)
	if fenced {
		oft, err := attributevalue.Marshal(originalFencingToken)
		if err != nil {
			panic(fmt.Errorf("marshal original fencing token to av failed: %d: %w", originalFencingToken, err))
		}
		c := expression.Equal(expression.Name(t.fenceAttribute), expression.Value(oft))
		b = b.WithCondition(c)
	}

	expr, err := b.Build()
	if err != nil {
		return nil, fmt.Errorf("ddb expression build failed: %w", err)
	}

	key := map[string]types.AttributeValue{
		t.hashAttribute: iav[t.hashAttribute],
	}
	if t.sortAttribute != "" {
		key[t.sortAttribute] = iav[t.sortAttribute]
	}

	input := &dynamodb.UpdateItemInput{
		TableName:                           &t.tableName,
		Key:                                 key,
		UpdateExpression:                    expr.Update(),
		ConditionExpression:                 expr.Condition(),
		ExpressionAttributeNames:            expr.Names(),
		ExpressionAttributeValues:           expr.Values(),
		ReturnValues:                        types.ReturnValueNone,
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	}

	return t.client.UpdateItem(ctx, input)
}

// flattenMapRecursive will go through a given map of AttributeValues with nested maps and
// create a flat map where nested values are stored under keys constructed by joining their
// path with a period(.). This is useful when constructing update expressions.
func flattenMapRecursive[T any](t *Table[T], parent string, src map[string]types.AttributeValue) map[string]types.AttributeValue {
	dst := make(map[string]types.AttributeValue)

	for k, v := range src {
		switch av := v.(type) {

		case *types.AttributeValueMemberM:
			c := flattenMapRecursive(t, k, av.Value)
			maps.Copy(dst, c)

		default:
			kp := ""
			if parent != "" {
				kp = fmt.Sprintf("%s.%s", parent, k)
			} else {
				kp = k
			}
			dst[kp] = v
		}
	}

	return dst
}

// UpdateItem modifies the item identified by the key specification in type T, updating all attributes from the provided value except the hash key.
// Nested fields in the struct are flattened into dot-separated attribute paths for the DynamoDB update expression.
func UpdateItem[T any](ctx context.Context, t *Table[T], value T) error {
	_, err := updateItem(ctx, t, &value, false)
	return err
}

// FencedUpdateItem performs an optimistic-locking update using the fence attribute defined on type T.
// It increments the fence and applies the update only if the current item in the database still matches the previous fence value from the input.
// If the conditional check fails (due to concurrent modification), it returns the current version of the item (from the failed response) and a nil error.
//
// To check if the update was successful, compare the input pointer with the returned pointer:
// - If they are the same, the update succeeded.
// - If they differ, the update did not take place and the returned value is the current item from the database.
//
// Fields are matched using the hash (and sort) key as specified in the struct tags.
func FencedUpdateItem[T any](ctx context.Context, t *Table[T], value *T) (*T, error) {
	var current T
	_, err := updateItem(ctx, t, value, true)
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(errors.Unwrap(err), &ccf) {
			err = attributevalue.UnmarshalMap(ccf.Item, &current)
			if err != nil {
				return nil, fmt.Errorf("umarshal current av map to type T failed: %w", err)
			}
			return &current, nil
		}

		return nil, fmt.Errorf("ddb update item: %w", err)
	}
	return value, nil
}

// DeleteItemByKey deletes the item with the given partition key.
func DeleteItemByKey[T any, K any](ctx context.Context, t *Table[T], key K) error {
	kav, err := attributevalue.Marshal(key)
	if err != nil {
		return fmt.Errorf("marshal key to av failed: %w", err)
	}

	input := &dynamodb.DeleteItemInput{
		TableName: &t.tableName,
		Key: map[string]types.AttributeValue{
			t.hashAttribute: kav,
		},
	}

	return delete(ctx, t, input)
}

// DeleteItemByCompositeKey deletes the item with the given partition and sort keys.
func DeleteItemByCompositeKey[T any, PK any, SK any](ctx context.Context, t *Table[T], pk PK, sk SK) error {
	pkav, err := attributevalue.Marshal(pk)
	if err != nil {
		return fmt.Errorf("marshal key to av failed: %w", err)
	}

	skav, err := attributevalue.Marshal(sk)
	if err != nil {
		return fmt.Errorf("marshal key to av failed: %w", err)
	}

	input := &dynamodb.DeleteItemInput{
		TableName: &t.tableName,
		Key: map[string]types.AttributeValue{
			t.hashAttribute: pkav,
			t.sortAttribute: skav,
		},
	}

	return delete(ctx, t, input)
}

func delete[T any](ctx context.Context, t *Table[T], input *dynamodb.DeleteItemInput) error {
	_, err := t.client.DeleteItem(ctx, input)
	if err != nil {
		return fmt.Errorf("ddb delete item failed: %w", err)
	}
	return nil
}
