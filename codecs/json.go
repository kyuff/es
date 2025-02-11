package codecs

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/kyuff/es"
)

func NewJSON() *JSON {
	return &JSON{
		content: make(map[contentKey]reflect.Type),
	}
}

type JSON struct {
	content map[contentKey]reflect.Type
}

func (j *JSON) Encode(event es.Event) ([]byte, error) {
	return json.Marshal(event.Content)
}

func (j *JSON) Decode(entityType, contentName string, b []byte) (es.Content, error) {
	key := contentKey{
		entityType:  entityType,
		contentName: contentName,
	}
	tp, ok := j.content[key]
	if !ok {
		return nil, fmt.Errorf("unknown content type %s for entity type %s", contentName, entityType)
	}
	value := reflect.New(tp)
	err := json.Unmarshal(b, value.Interface())
	if err != nil {
		return nil, err
	}

	return value.Elem().Interface().(es.Content), nil
}

func (j *JSON) Register(entityType string, contentTypes ...es.Content) error {
	// TODO add mutex
	for _, contentType := range contentTypes {
		key := contentKey{
			entityType:  entityType,
			contentName: contentType.Name(),
		}
		j.content[key] = reflect.TypeOf(contentType)
	}

	return nil
}
