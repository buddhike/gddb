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

// Insert writes item with a condition that the primary key does not already exist.
// It fails if an item with the same hash key (and sort key, when the table has one) is present.
func Insert[T any](ctx context.Context, t *Table[T], item T) error {
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

// FindByKey loads a single item by partition key. It returns [ErrItemNotFound] when no item exists.
func FindByKey[T any, K any](ctx context.Context, t *Table[T], key K) (T, error) {
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

// FindByCompositeKey loads a single item by partition and sort key.
// It returns [ErrItemNotFound] when no item exists.
func FindByCompositeKey[T any, PK any, SK any](ctx context.Context, t *Table[T], pk PK, sk SK) (T, error) {
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

func updateItem[T any](ctx context.Context, t *Table[T], pk types.AttributeValue, sk types.AttributeValue, value T, fenced bool) (*dynamodb.UpdateItemOutput, error) {
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
		t.hashAttribute: pk,
	}
	if sk != nil {
		key[t.sortAttribute] = sk
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

// UpdateByKey updates the item identified by the partition key, setting every attribute
// from value except the hash key. Nested struct fields are flattened for the update expression.
func UpdateByKey[T any, K any](ctx context.Context, t *Table[T], key K, value T) error {
	pk, err := attributevalue.Marshal(key)
	if err != nil {
		return fmt.Errorf("marshal key to av failed %w", err)
	}
	_, err = updateItem(ctx, t, pk, nil, value, false)
	return err
}

// FencedUpdateByKey performs an optimistic-locking update using the fence attribute on T:
// it increments the fence and applies the update only if the fence still matches the previous value.
// On a conditional check failure, it returns the current item from the failed response and a nil error.
func FencedUpdateByKey[T any, K any](ctx context.Context, t *Table[T], key K, value T) (T, error) {
	var current T
	pk, err := attributevalue.Marshal(key)
	if err != nil {
		return current, fmt.Errorf("marshal key to av failed %w", err)
	}
	_, err = updateItem(ctx, t, pk, nil, value, true)
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(errors.Unwrap(err), &ccf) {
			err = attributevalue.UnmarshalMap(ccf.Item, &current)
			if err != nil {
				return current, fmt.Errorf("umarshal current av map to type T failed: %w", err)
			}
			return current, nil
		}

		return current, fmt.Errorf("ddb update item: %w", err)
	}
	return value, nil
}

// UpdateByCompositeKey updates the item identified by partition and sort keys, setting every
// attribute from value except the hash key. Nested struct fields are flattened for the update expression.
func UpdateByCompositeKey[T any, PK any, SK any](ctx context.Context, t *Table[T], pk PK, sk SK, value T) error {
	avpk, err := attributevalue.Marshal(pk)
	if err != nil {
		return fmt.Errorf("marshal key to pk failed %w", err)
	}
	avsk, err := attributevalue.Marshal(sk)
	if err != nil {
		return fmt.Errorf("marshal key to sk failed %w", err)
	}
	_, err = updateItem(ctx, t, avpk, avsk, value, false)
	return err
}

// FencedUpdateByCompositeKey is like [FencedUpdateByKey] but for tables with a composite primary key.
func FencedUpdateByCompositeKey[T any, PK any, SK any](ctx context.Context, t *Table[T], pk PK, sk SK, value T) (T, error) {
	var current T
	avpk, err := attributevalue.Marshal(pk)
	if err != nil {
		return current, fmt.Errorf("marshal key to pk failed %w", err)
	}
	avsk, err := attributevalue.Marshal(sk)
	if err != nil {
		return current, fmt.Errorf("marshal key to sk failed %w", err)
	}
	_, err = updateItem(ctx, t, avpk, avsk, value, true)
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(errors.Unwrap(err), &ccf) {
			err = attributevalue.UnmarshalMap(ccf.Item, &current)
			if err != nil {
				return current, fmt.Errorf("umarshal current av map to type T failed: %w", err)
			}
			return current, nil
		}

		return current, fmt.Errorf("ddb update item: %w", err)
	}
	return value, nil
}

// DeleteByKey deletes the item with the given partition key.
func DeleteByKey[T any, K any](ctx context.Context, t *Table[T], key K) error {
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

// DeleteByCompositeKey deletes the item with the given partition and sort keys.
func DeleteByCompositeKey[T any, PK any, SK any](ctx context.Context, t *Table[T], pk PK, sk SK) error {
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
