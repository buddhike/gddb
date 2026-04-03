package gddb

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	ErrItemExists   error = fmt.Errorf("item already exists")
	ErrItemNotFound error = fmt.Errorf("item not found")
)

type Table[K any, V any] struct {
	tableName             string
	keyAttribute          string
	fencingTokenAttribute string
	client                *dynamodb.Client
}

func NewTable[K any, V any](tableName string, keyAttribute, fencingTokenAttribute string, client *dynamodb.Client) *Table[K, V] {
	return &Table[K, V]{
		tableName:             tableName,
		keyAttribute:          keyAttribute,
		fencingTokenAttribute: fencingTokenAttribute,
		client:                client,
	}
}

func (t *Table[K, V]) InsertUnique(ctx context.Context, item V) error {
	m, err := attributevalue.MarshalMap(item)

	if err != nil {
		return err
	}

	cond := expression.AttributeNotExists(expression.Name(t.keyAttribute))
	unique, err := expression.NewBuilder().WithCondition(cond).Build()

	if err != nil {
		return err
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
		var ccfe *types.ConditionalCheckFailedException
		if ok := errors.As(err, &ccfe); ok {
			return ErrItemExists
		}
		return err
	}

	return nil
}

func (t *Table[K, V]) FindByKey(ctx context.Context, key K, projection ...string) (V, error) {
	var v V
	mk, err := attributevalue.Marshal(key)

	if err != nil {
		return v, err
	}

	getItemInput := &dynamodb.GetItemInput{
		TableName: &t.tableName,
		Key: map[string]types.AttributeValue{
			t.keyAttribute: mk,
		},
	}

	if len(projection) > 0 {
		names := make([]expression.NameBuilder, len(projection))
		nl := expression.NamesList(names[0], names[1:]...)
		p, err := expression.NewBuilder().WithProjection(nl).Build()

		if err != nil {
			return v, err
		}

		getItemInput.ExpressionAttributeNames = p.Names()
		getItemInput.ProjectionExpression = p.Projection()
	}

	result, err := t.client.GetItem(ctx, getItemInput)

	if err != nil {
		return v, err
	}

	if len(result.Item) == 0 {
		return v, ErrItemNotFound
	}

	err = attributevalue.UnmarshalMap(result.Item, &v)

	return v, err
}

func (t *Table[K, V]) Query(ctx context.Context, expr expression.Expression) ([]V, error) {
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
		return nil, err
	}

	r := make([]V, len(result.Items))

	for i, m := range result.Items {
		var v V
		err = attributevalue.UnmarshalMap(m, &v)

		if err != nil {
			return nil, err
		}

		r[i] = v
	}

	return r, nil
}

func (t *Table[K, V]) updateItem(ctx context.Context, key K, value V, fenced bool) (*dynamodb.UpdateItemOutput, error) {
	iav, err := attributevalue.MarshalMap(value)

	if err != nil {
		return nil, fmt.Errorf("updateItem marshal value: %w", err)
	}

	var originalFencingToken int64
	iav = t.flattenMapRecursive("", iav)
	update := expression.UpdateBuilder{}

	if fenced {
		if ft, ok := iav[t.fencingTokenAttribute]; ok {
			n := ft.(*types.AttributeValueMemberN)
			originalFencingToken, err = strconv.ParseInt(n.Value, 10, 64)

			if err != nil {
				return nil, fmt.Errorf("updateItem parse fencing token: %w", err)
			}

			iav[t.fencingTokenAttribute], err = attributevalue.Marshal(originalFencingToken + 1)

			if err != nil {
				return nil, fmt.Errorf("updateItem marshal new fencing token: %w", err)
			}
		} else {
			return nil, fmt.Errorf("fenced updates require fencing token attribute : %s", t.fencingTokenAttribute)
		}
	}

	for k, v := range iav {
		if k != t.keyAttribute {
			update = update.Set(expression.Name(k), expression.Value(v))
		}
	}

	b := expression.NewBuilder().WithUpdate(update)

	if fenced {
		oft, err := attributevalue.Marshal(originalFencingToken)

		if err != nil {
			panic(fmt.Errorf("updateItem marshal original fencing token: %d: %w", originalFencingToken, err))
		}

		c := expression.Equal(expression.Name(t.fencingTokenAttribute), expression.Value(oft))
		b = b.WithCondition(c)
	}

	expr, err := b.Build()

	if err != nil {
		return nil, fmt.Errorf("updateItem build update expression: %w", err)
	}

	kav, err := attributevalue.Marshal(key)

	if err != nil {
		return nil, fmt.Errorf("update item marshal key: %w", err)
	}

	input := &dynamodb.UpdateItemInput{
		TableName: &t.tableName,
		Key: map[string]types.AttributeValue{
			t.keyAttribute: kav,
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
func (t *Table[K, V]) flattenMapRecursive(parent string, src map[string]types.AttributeValue) map[string]types.AttributeValue {
	dst := make(map[string]types.AttributeValue)

	for k, v := range src {
		switch av := v.(type) {

		case *types.AttributeValueMemberM:
			c := t.flattenMapRecursive(k, av.Value)
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

func (t *Table[K, V]) UpdateByKey(ctx context.Context, key K, value V) error {
	_, err := t.updateItem(ctx, key, value, false)
	return err
}

func (t *Table[K, V]) UpdateByKeyOrGetLatest(ctx context.Context, key K, value V) (V, error) {
	var current V
	_, err := t.updateItem(ctx, key, value, true)

	if err != nil {
		var ccf *types.ConditionalCheckFailedException

		if errors.As(errors.Unwrap(err), &ccf) {
			err = attributevalue.UnmarshalMap(ccf.Item, &current)

			if err != nil {
				return current, fmt.Errorf("updatebykeyorgetlatest unmarshal current version: %w", err)
			}

			return current, nil
		}

		return current, fmt.Errorf("update item: %w", err)
	}
	return value, nil
}

func (t *Table[K, V]) DeleteByKey(ctx context.Context, key K) error {
	kav, err := attributevalue.Marshal(key)

	if err != nil {
		return err
	}

	input := dynamodb.DeleteItemInput{
		TableName: &t.tableName,
		Key: map[string]types.AttributeValue{
			t.keyAttribute: kav,
		},
	}

	_, err = t.client.DeleteItem(ctx, &input)
	return err
}
