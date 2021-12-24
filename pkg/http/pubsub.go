package http

import (
	"fmt"
	contrib_metadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	diag "github.com/dapr/dapr/pkg/diagnostics"
	diag_utils "github.com/dapr/dapr/pkg/diagnostics/utils"
	"github.com/dapr/dapr/pkg/messages"
	runtime_pubsub "github.com/dapr/dapr/pkg/runtime/pubsub"
	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
)

// 构建pubsub端点
func (a *api) constructPubSubEndpoints() []Endpoint {
	return []Endpoint{
		{
			Methods: []string{fasthttp.MethodPost, fasthttp.MethodPut},
			Route:   "publish/{pubsubname}/{topic:*}",
			Version: apiVersionV1,
			Handler: a.onPublish,
		},
	}
}

func (a *api) onPublish(reqCtx *fasthttp.RequestCtx) {
	if a.pubsubAdapter == nil {
		msg := NewErrorResponse("ERR_PUBSUB_NOT_CONFIGURED", messages.ErrPubsubNotConfigured)
		respond(reqCtx, withError(fasthttp.StatusBadRequest, msg))
		log.Debug(msg)

		return
	}

	pubsubName := reqCtx.UserValue(pubsubnameparam).(string)
	if pubsubName == "" {
		msg := NewErrorResponse("ERR_PUBSUB_EMPTY", messages.ErrPubsubEmpty)
		respond(reqCtx, withError(fasthttp.StatusNotFound, msg))
		log.Debug(msg)

		return
	}

	thepubsub := a.pubsubAdapter.GetPubSub(pubsubName)
	if thepubsub == nil {
		msg := NewErrorResponse("ERR_PUBSUB_NOT_FOUND", fmt.Sprintf(messages.ErrPubsubNotFound, pubsubName))
		respond(reqCtx, withError(fasthttp.StatusNotFound, msg))
		log.Debug(msg)

		return
	}

	topic := reqCtx.UserValue(topicParam).(string)
	if topic == "" {
		msg := NewErrorResponse("ERR_TOPIC_EMPTY", fmt.Sprintf(messages.ErrTopicEmpty, pubsubName))
		respond(reqCtx, withError(fasthttp.StatusNotFound, msg))
		log.Debug(msg)

		return
	}

	body := reqCtx.PostBody()
	contentType := string(reqCtx.Request.Header.Peek("Content-Type"))
	metadata := getMetadataFromRequest(reqCtx)
	rawPayload, metaErr := contrib_metadata.IsRawPayload(metadata)
	if metaErr != nil {
		msg := NewErrorResponse("ERR_PUBSUB_REQUEST_METADATA",
			fmt.Sprintf(messages.ErrMetadataGet, metaErr.Error()))
		respond(reqCtx, withError(fasthttp.StatusBadRequest, msg))
		log.Debug(msg)

		return
	}

	// Extract trace context from context.
	span := diag_utils.SpanFromContext(reqCtx)
	// Populate W3C traceparent to cloudevent envelope
	corID := diag.SpanContextToW3CString(span.SpanContext())

	data := body

	if !rawPayload {
		envelope, err := runtime_pubsub.NewCloudEvent(&runtime_pubsub.CloudEvent{
			ID:              a.id,
			Topic:           topic,
			DataContentType: contentType,
			Data:            body,
			TraceID:         corID,
			Pubsub:          pubsubName,
		})
		if err != nil {
			msg := NewErrorResponse("ERR_PUBSUB_CLOUD_EVENTS_SER",
				fmt.Sprintf(messages.ErrPubsubCloudEventCreation, err.Error()))
			respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
			log.Debug(msg)

			return
		}

		features := thepubsub.Features()

		pubsub.ApplyMetadata(envelope, features, metadata)

		data, err = a.json.Marshal(envelope)
		if err != nil {
			msg := NewErrorResponse("ERR_PUBSUB_CLOUD_EVENTS_SER",
				fmt.Sprintf(messages.ErrPubsubCloudEventsSer, topic, pubsubName, err.Error()))
			respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
			log.Debug(msg)

			return
		}
	}

	req := pubsub.PublishRequest{
		PubsubName: pubsubName,
		Topic:      topic,
		Data:       data,
		Metadata:   metadata,
	}

	err := a.pubsubAdapter.Publish(&req)
	if err != nil {
		status := fasthttp.StatusInternalServerError
		msg := NewErrorResponse("ERR_PUBSUB_PUBLISH_MESSAGE",
			fmt.Sprintf(messages.ErrPubsubPublishMessage, topic, pubsubName, err.Error()))

		if errors.As(err, &runtime_pubsub.NotAllowedError{}) {
			msg = NewErrorResponse("ERR_PUBSUB_FORBIDDEN", err.Error())
			status = fasthttp.StatusForbidden
		}

		if errors.As(err, &runtime_pubsub.NotFoundError{}) {
			msg = NewErrorResponse("ERR_PUBSUB_NOT_FOUND", err.Error())
			status = fasthttp.StatusBadRequest
		}

		respond(reqCtx, withError(status, msg))
		log.Debug(msg)
	} else {
		respond(reqCtx, withEmpty())
	}
}
