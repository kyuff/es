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

func (j *JSON) Decode(streamType, contentName string, b []byte) (es.Content, error) {
	key := contentKey{
		streamType:  streamType,
		contentName: contentName,
	}
	tp, ok := j.content[key]
	if !ok {
		return nil, fmt.Errorf("unknown content type %s for stream type %s", contentName, streamType)
	}
	value := reflect.New(tp)
	err := json.Unmarshal(b, value.Interface())
	if err != nil {
		return nil, err
	}

	return value.Elem().Interface().(es.Content), nil
}

func (j *JSON) Register(streamType string, contentTypes ...es.Content) error {
	// TODO add mutex
	for _, contentType := range contentTypes {
		key := contentKey{
			streamType:  streamType,
			contentName: contentType.EventName(),
		}
		j.content[key] = reflect.TypeOf(contentType)
	}

	return nil
}
