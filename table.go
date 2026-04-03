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

func InsertUnique[T any](ctx context.Context, t *Table[T], item T) error {
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

func IsErrConditionalCheckFailed(err error) bool {
	var ccfe *types.ConditionalCheckFailedException
	ok := errors.As(err, &ccfe)
	return ok
}

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

	result, err := t.client.GetItem(ctx, getItemInput)
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

func updateItem[T any, K any](ctx context.Context, t *Table[T], key K, value T, fenced bool) (*dynamodb.UpdateItemOutput, error) {
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

	kav, err := attributevalue.Marshal(key)
	if err != nil {
		return nil, fmt.Errorf("marshal key to av failed: %w", err)
	}

	input := &dynamodb.UpdateItemInput{
		TableName: &t.tableName,
		Key: map[string]types.AttributeValue{
			t.hashAttribute: kav,
		},
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

func UpdateByKey[T any, K any](ctx context.Context, t *Table[T], key K, value T) error {
	_, err := updateItem(ctx, t, key, value, false)
	return err
}

func UpdateByKeyOrGetLatest[T any, K any](ctx context.Context, t *Table[T], key K, value T) (T, error) {
	var current T
	_, err := updateItem(ctx, t, key, value, true)
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

func DeleteByKey[T any, K any](ctx context.Context, t *Table[T], key K) error {
	kav, err := attributevalue.Marshal(key)
	if err != nil {
		return fmt.Errorf("marshal key to av failed: %w", err)
	}

	input := dynamodb.DeleteItemInput{
		TableName: &t.tableName,
		Key: map[string]types.AttributeValue{
			t.hashAttribute: kav,
		},
	}

	_, err = t.client.DeleteItem(ctx, &input)
	if err != nil {
		return fmt.Errorf("ddb delete item failed: %w", err)
	}
	return nil
}
