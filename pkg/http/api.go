// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package http

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/dapr/components-contrib/bindings"
	contrib_metadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/dapr/pkg/actors"
	components_v1alpha1 "github.com/dapr/dapr/pkg/apis/components/v1alpha1"
	"github.com/dapr/dapr/pkg/channel"
	"github.com/dapr/dapr/pkg/channel/http"
	"github.com/dapr/dapr/pkg/config"
	diag "github.com/dapr/dapr/pkg/diagnostics"
	diag_utils "github.com/dapr/dapr/pkg/diagnostics/utils"
	"github.com/dapr/dapr/pkg/messages"
	"github.com/dapr/dapr/pkg/messaging"
	invokev1 "github.com/dapr/dapr/pkg/messaging/v1"
	runtime_pubsub "github.com/dapr/dapr/pkg/runtime/pubsub"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
	"go.opencensus.io/trace"
	"google.golang.org/grpc/codes"
)

// API 返回Dapr的HTTP端点的列表。
type API interface {
	APIEndpoints() []Endpoint
	PublicEndpoints() []Endpoint
	MarkStatusAsReady()
	MarkStatusAsOutboundReady()
	SetAppChannel(appChannel channel.AppChannel)
	SetDirectMessaging(directMessaging messaging.DirectMessaging)
	SetActorRuntime(actor actors.Actors)
}

type api struct {
	endpoints                []Endpoint
	publicEndpoints          []Endpoint
	directMessaging          messaging.DirectMessaging
	appChannel               channel.AppChannel
	getComponentsFn          func() []components_v1alpha1.Component
	stateStores              map[string]state.Store
	transactionalStateStores map[string]state.TransactionalStore
	secretStores             map[string]secretstores.SecretStore
	secretsConfiguration     map[string]config.SecretsScope
	json                     jsoniter.API
	actor                    actors.Actors
	pubsubAdapter            runtime_pubsub.Adapter
	sendToOutputBindingFn    func(name string, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error)
	id                       string
	extendedMetadata         sync.Map
	readyStatus              bool // 所有逻辑均准备后 ,置为true
	outboundReadyStatus      bool // http服务开启后，置为true
	tracingSpec              config.TracingSpec
	shutdown                 func()
}

type registeredComponent struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Version string `json:"version"`
}

type metadata struct {
	ID                   string                      `json:"id"`
	ActiveActorsCount    []actors.ActiveActorsCount  `json:"actors"`
	Extended             map[interface{}]interface{} `json:"extended"`
	RegisteredComponents []registeredComponent       `json:"components"`
}

const (
	apiVersionV1         = "v1.0"
	apiVersionV1alpha1   = "v1.0-alpha1"
	idParam              = "id"
	methodParam          = "method"
	topicParam           = "topic"
	actorTypeParam       = "actorType"
	actorIDParam         = "actorId"
	storeNameParam       = "storeName"
	stateKeyParam        = "key"
	secretStoreNameParam = "secretStoreName"
	secretNameParam      = "key"
	nameParam            = "name"
	consistencyParam     = "consistency" // 一致性
	concurrencyParam     = "concurrency" // 并发
	pubsubnameparam      = "pubsubname"
	traceparentHeader    = "traceparent"
	tracestateHeader     = "tracestate"
	daprAppID            = "dapr-app-id"
)

// NewAPI returns a new API.
// 无非就是将RuntimeConfig里的数据来回传,避免依赖
func NewAPI(
	appID string,
	appChannel channel.AppChannel,
	directMessaging messaging.DirectMessaging,
	getComponentsFn func() []components_v1alpha1.Component,
	stateStores map[string]state.Store,
	secretStores map[string]secretstores.SecretStore,
	secretsConfiguration map[string]config.SecretsScope,
	pubsubAdapter runtime_pubsub.Adapter,
	actor actors.Actors,
	sendToOutputBindingFn func(name string, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error),
	tracingSpec config.TracingSpec,
	shutdown func()) API {
	transactionalStateStores := map[string]state.TransactionalStore{}
	for key, store := range stateStores {
		if state.FeatureTransactional.IsPresent(store.Features()) {
			transactionalStateStores[key] = store.(state.TransactionalStore)
		}
	}
	api := &api{
		appChannel:               appChannel,
		getComponentsFn:          getComponentsFn,
		directMessaging:          directMessaging,
		stateStores:              stateStores,
		transactionalStateStores: transactionalStateStores,
		secretStores:             secretStores,
		secretsConfiguration:     secretsConfiguration,
		json:                     jsoniter.ConfigFastest,
		actor:                    actor,
		pubsubAdapter:            pubsubAdapter,
		sendToOutputBindingFn:    sendToOutputBindingFn,
		id:                       appID,
		tracingSpec:              tracingSpec,
		shutdown:                 shutdown,
	}

	//	注册路由
	metadataEndpoints := api.constructMetadataEndpoints()
	healthEndpoints := api.constructHealthzEndpoints()

	api.endpoints = append(api.endpoints, api.constructStateEndpoints()...)
	api.endpoints = append(api.endpoints, api.constructSecretEndpoints()...)
	api.endpoints = append(api.endpoints, api.constructPubSubEndpoints()...)
	api.endpoints = append(api.endpoints, api.constructActorEndpoints()...)
	api.endpoints = append(api.endpoints, api.constructDirectMessagingEndpoints()...)
	api.endpoints = append(api.endpoints, metadataEndpoints...)
	api.endpoints = append(api.endpoints, api.constructShutdownEndpoints()...)
	api.endpoints = append(api.endpoints, api.constructBindingsEndpoints()...)
	api.endpoints = append(api.endpoints, healthEndpoints...)

	api.publicEndpoints = append(api.publicEndpoints, metadataEndpoints...)
	api.publicEndpoints = append(api.publicEndpoints, healthEndpoints...)

	return api
}

// APIEndpoints 返回注册的端点
func (a *api) APIEndpoints() []Endpoint {
	return a.endpoints
}

// PublicEndpoints 返回公开的端点
func (a *api) PublicEndpoints() []Endpoint {
	return a.publicEndpoints
}

// MarkStatusAsReady 标记dapr状态已就绪
func (a *api) MarkStatusAsReady() {
	a.readyStatus = true
}

// MarkStatusAsOutboundReady 标记dapr出站流量的已就绪。
func (a *api) MarkStatusAsOutboundReady() {
	a.outboundReadyStatus = true
}

// ----------------------------------------

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

// 构建binging端点
func (a *api) constructBindingsEndpoints() []Endpoint {
	return []Endpoint{
		{
			Methods: []string{fasthttp.MethodPost, fasthttp.MethodPut},
			Route:   "bindings/{name}",
			Version: apiVersionV1,
			Handler: a.onOutputBindingMessage,
		},
	}
}

// actor
// 一个actor的并发量为1
//POST/GET/PUT/DELETE http://localhost:3500/v1.0/actors/<actorType>/<actorId>/<method/state/timers/reminders>
func (a *api) constructActorEndpoints() []Endpoint {
	return []Endpoint{
		{
			Methods: []string{fasthttp.MethodPost, fasthttp.MethodPut},
			Route:   "actors/{actorType}/{actorId}/state",
			Version: apiVersionV1,
			Handler: a.onActorStateTransaction,
		},
		{
			Methods: []string{fasthttp.MethodGet, fasthttp.MethodPost, fasthttp.MethodDelete, fasthttp.MethodPut},
			Route:   "actors/{actorType}/{actorId}/method/{method}",
			Version: apiVersionV1,
			Handler: a.onDirectActorMessage,
		},
		{
			Methods: []string{fasthttp.MethodGet},
			Route:   "actors/{actorType}/{actorId}/state/{key}",
			Version: apiVersionV1,
			Handler: a.onGetActorState,
		},
		{
			Methods: []string{fasthttp.MethodPost, fasthttp.MethodPut},
			Route:   "actors/{actorType}/{actorId}/reminders/{name}",
			Version: apiVersionV1,
			Handler: a.onCreateActorReminder,
		},
		{
			Methods: []string{fasthttp.MethodPost, fasthttp.MethodPut},
			Route:   "actors/{actorType}/{actorId}/timers/{name}",
			Version: apiVersionV1,
			Handler: a.onCreateActorTimer,
		},
		{
			Methods: []string{fasthttp.MethodDelete},
			Route:   "actors/{actorType}/{actorId}/reminders/{name}",
			Version: apiVersionV1,
			Handler: a.onDeleteActorReminder,
		},
		{
			Methods: []string{fasthttp.MethodDelete},
			Route:   "actors/{actorType}/{actorId}/timers/{name}",
			Version: apiVersionV1,
			Handler: a.onDeleteActorTimer,
		},
		{
			Methods: []string{fasthttp.MethodGet},
			Route:   "actors/{actorType}/{actorId}/reminders/{name}",
			Version: apiVersionV1,
			Handler: a.onGetActorReminder,
		},
	}
}

// 元信息
func (a *api) constructMetadataEndpoints() []Endpoint {
	return []Endpoint{
		{
			Methods: []string{fasthttp.MethodGet},
			Route:   "metadata",
			Version: apiVersionV1,
			Handler: a.onGetMetadata,
		},
		{
			Methods: []string{fasthttp.MethodPut},
			Route:   "metadata/{key}",
			Version: apiVersionV1,
			Handler: a.onPutMetadata,
		},
	}
}

// 停止
func (a *api) constructShutdownEndpoints() []Endpoint {
	return []Endpoint{
		{
			Methods: []string{fasthttp.MethodPost},
			Route:   "shutdown", //   curl -X POST http://localhost:3500/v1.0/shutdown
			Version: apiVersionV1,
			Handler: a.onShutdown,
		},
	}
}

// 健康检查
func (a *api) constructHealthzEndpoints() []Endpoint {
	return []Endpoint{
		{
			Methods: []string{fasthttp.MethodGet},
			Route:   "healthz", //      curl http://localhost:3500/v1.0/healthz
			Version: apiVersionV1,
			Handler: a.onGetHealthz,
		},
		{
			Methods: []string{fasthttp.MethodGet},
			Route:   "healthz/outbound", // curl http://localhost:3500/v1.0/healthz/outbound
			Version: apiVersionV1,
			Handler: a.onGetOutboundHealthz,
		},
	}
}

// ----------------------------------------

func (a *api) onOutputBindingMessage(reqCtx *fasthttp.RequestCtx) {
	name := reqCtx.UserValue(nameParam).(string)
	body := reqCtx.PostBody()

	var req OutputBindingRequest
	err := a.json.Unmarshal(body, &req)
	if err != nil {
		msg := NewErrorResponse("ERR_MALFORMED_REQUEST", fmt.Sprintf(messages.ErrMalformedRequest, err))
		respond(reqCtx, withError(fasthttp.StatusBadRequest, msg))
		log.Debug(msg)
		return
	}

	b, err := a.json.Marshal(req.Data)
	if err != nil {
		msg := NewErrorResponse("ERR_MALFORMED_REQUEST_DATA", fmt.Sprintf(messages.ErrMalformedRequestData, err))
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
		return
	}

	// pass the trace context to output binding in metadata
	if span := diag_utils.SpanFromContext(reqCtx); span != nil {
		sc := span.SpanContext()
		if req.Metadata == nil {
			req.Metadata = map[string]string{}
		}
		// if sc is not empty context, set traceparent Header.
		if sc != (trace.SpanContext{}) {
			req.Metadata[traceparentHeader] = diag.SpanContextToW3CString(sc)
		}
		if sc.Tracestate != nil {
			req.Metadata[tracestateHeader] = diag.TraceStateToW3CString(sc)
		}
	}

	resp, err := a.sendToOutputBindingFn(name, &bindings.InvokeRequest{
		Metadata:  req.Metadata,
		Data:      b,
		Operation: bindings.OperationKind(req.Operation),
	})
	if err != nil {
		msg := NewErrorResponse("ERR_INVOKE_OUTPUT_BINDING", fmt.Sprintf(messages.ErrInvokeOutputBinding, name, err))
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
		return
	}
	if resp == nil {
		respond(reqCtx, withEmpty())
	} else {
		respond(reqCtx, withMetadata(resp.Metadata), withJSON(fasthttp.StatusOK, resp.Data))
	}
}

// stateErrorResponse 接收一个状态存储错误，并返回相应的状态代码、错误信息和修改后的用户错误。
func (a *api) stateErrorResponse(err error, errorCode string) (int, string, ErrorResponse) {
	var message string
	var code int
	var etag bool
	etag, code, message = a.etagError(err)

	r := ErrorResponse{
		ErrorCode: errorCode,
	}
	if etag {
		return code, message, r
	}
	message = err.Error()

	return fasthttp.StatusInternalServerError, message, r
}

// findTargetID 获取请求中的目标应用ID
func (a *api) findTargetID(reqCtx *fasthttp.RequestCtx) string {
	// 1、url 中的 app id
	// 2、 header 中的 "dapr-app-id"
	// 3、 basic auth 			header 中的Authorization: Basic base64encode(username+":"+password)
	if id := reqCtx.UserValue(idParam); id == nil {
		if appID := reqCtx.Request.Header.Peek(daprAppID); appID == nil {
			if auth := reqCtx.Request.Header.Peek(fasthttp.HeaderAuthorization); auth != nil &&
				strings.HasPrefix(string(auth), "Basic ") {
				if s, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(string(auth), "Basic ")); err == nil {
					pair := strings.Split(string(s), ":")
					//
					if len(pair) == 2 && pair[0] == daprAppID {
						return pair[1]
					}
				}
			}
		} else {
			return string(appID)
		}
	} else {
		return id.(string)
	}

	return ""
}

func (a *api) onCreateActorReminder(reqCtx *fasthttp.RequestCtx) {
	if a.actor == nil {
		msg := NewErrorResponse("ERR_ACTOR_RUNTIME_NOT_FOUND", messages.ErrActorRuntimeNotFound)
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		return
	}

	actorType := reqCtx.UserValue(actorTypeParam).(string)
	actorID := reqCtx.UserValue(actorIDParam).(string)
	name := reqCtx.UserValue(nameParam).(string)

	var req actors.CreateReminderRequest
	err := a.json.Unmarshal(reqCtx.PostBody(), &req)
	if err != nil {
		msg := NewErrorResponse("ERR_MALFORMED_REQUEST", fmt.Sprintf(messages.ErrMalformedRequest, err))
		respond(reqCtx, withError(fasthttp.StatusBadRequest, msg))
		log.Debug(msg)
		return
	}

	req.Name = name
	req.ActorType = actorType
	req.ActorID = actorID

	err = a.actor.CreateReminder(reqCtx, &req)
	if err != nil {
		msg := NewErrorResponse("ERR_ACTOR_REMINDER_CREATE", fmt.Sprintf(messages.ErrActorReminderCreate, err))
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
	} else {
		respond(reqCtx, withEmpty())
	}
}

func (a *api) onCreateActorTimer(reqCtx *fasthttp.RequestCtx) {
	if a.actor == nil {
		msg := NewErrorResponse("ERR_ACTOR_RUNTIME_NOT_FOUND", messages.ErrActorRuntimeNotFound)
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
		return
	}

	actorType := reqCtx.UserValue(actorTypeParam).(string)
	actorID := reqCtx.UserValue(actorIDParam).(string)
	name := reqCtx.UserValue(nameParam).(string)

	var req actors.CreateTimerRequest
	err := a.json.Unmarshal(reqCtx.PostBody(), &req)
	if err != nil {
		msg := NewErrorResponse("ERR_MALFORMED_REQUEST", fmt.Sprintf(messages.ErrMalformedRequest, err))
		respond(reqCtx, withError(fasthttp.StatusBadRequest, msg))
		log.Debug(msg)
		return
	}

	req.Name = name
	req.ActorType = actorType
	req.ActorID = actorID

	err = a.actor.CreateTimer(reqCtx, &req)
	if err != nil {
		msg := NewErrorResponse("ERR_ACTOR_TIMER_CREATE", fmt.Sprintf(messages.ErrActorTimerCreate, err))
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
	} else {
		respond(reqCtx, withEmpty())
	}
}

func (a *api) onDeleteActorReminder(reqCtx *fasthttp.RequestCtx) {
	if a.actor == nil {
		msg := NewErrorResponse("ERR_ACTOR_RUNTIME_NOT_FOUND", messages.ErrActorRuntimeNotFound)
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
		return
	}

	actorType := reqCtx.UserValue(actorTypeParam).(string)
	actorID := reqCtx.UserValue(actorIDParam).(string)
	name := reqCtx.UserValue(nameParam).(string)

	req := actors.DeleteReminderRequest{
		Name:      name,
		ActorID:   actorID,
		ActorType: actorType,
	}

	err := a.actor.DeleteReminder(reqCtx, &req)
	if err != nil {
		msg := NewErrorResponse("ERR_ACTOR_REMINDER_DELETE", fmt.Sprintf(messages.ErrActorReminderDelete, err))
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
	} else {
		respond(reqCtx, withEmpty())
	}
}

func (a *api) onActorStateTransaction(reqCtx *fasthttp.RequestCtx) {
	if a.actor == nil {
		msg := NewErrorResponse("ERR_ACTOR_RUNTIME_NOT_FOUND", messages.ErrActorRuntimeNotFound)
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
		return
	}

	actorType := reqCtx.UserValue(actorTypeParam).(string)
	actorID := reqCtx.UserValue(actorIDParam).(string)
	body := reqCtx.PostBody()

	var ops []actors.TransactionalOperation
	err := a.json.Unmarshal(body, &ops)
	if err != nil {
		msg := NewErrorResponse("ERR_MALFORMED_REQUEST", err.Error())
		respond(reqCtx, withError(fasthttp.StatusBadRequest, msg))
		log.Debug(msg)
		return
	}

	hosted := a.actor.IsActorHosted(reqCtx, &actors.ActorHostedRequest{
		ActorType: actorType,
		ActorID:   actorID,
	})

	if !hosted {
		msg := NewErrorResponse("ERR_ACTOR_INSTANCE_MISSING", messages.ErrActorInstanceMissing)
		respond(reqCtx, withError(fasthttp.StatusBadRequest, msg))
		log.Debug(msg)
		return
	}

	req := actors.TransactionalRequest{
		ActorID:    actorID,
		ActorType:  actorType,
		Operations: ops,
	}

	err = a.actor.TransactionalStateOperation(reqCtx, &req)
	if err != nil {
		msg := NewErrorResponse("ERR_ACTOR_STATE_TRANSACTION_SAVE", fmt.Sprintf(messages.ErrActorStateTransactionSave, err))
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
	} else {
		respond(reqCtx, withEmpty())
	}
}

func (a *api) onGetActorReminder(reqCtx *fasthttp.RequestCtx) {
	if a.actor == nil {
		msg := NewErrorResponse("ERR_ACTOR_RUNTIME_NOT_FOUND", messages.ErrActorRuntimeNotFound)
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
		return
	}

	actorType := reqCtx.UserValue(actorTypeParam).(string)
	actorID := reqCtx.UserValue(actorIDParam).(string)
	name := reqCtx.UserValue(nameParam).(string)

	resp, err := a.actor.GetReminder(reqCtx, &actors.GetReminderRequest{
		ActorType: actorType,
		ActorID:   actorID,
		Name:      name,
	})
	if err != nil {
		msg := NewErrorResponse("ERR_ACTOR_REMINDER_GET", fmt.Sprintf(messages.ErrActorReminderGet, err))
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
		return
	}
	b, err := a.json.Marshal(resp)
	if err != nil {
		msg := NewErrorResponse("ERR_ACTOR_REMINDER_GET", fmt.Sprintf(messages.ErrActorReminderGet, err))
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
		return
	}

	respond(reqCtx, withJSON(fasthttp.StatusOK, b))
}

func (a *api) onDeleteActorTimer(reqCtx *fasthttp.RequestCtx) {
	if a.actor == nil {
		msg := NewErrorResponse("ERR_ACTOR_RUNTIME_NOT_FOUND", messages.ErrActorRuntimeNotFound)
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
		return
	}

	actorType := reqCtx.UserValue(actorTypeParam).(string)
	actorID := reqCtx.UserValue(actorIDParam).(string)
	name := reqCtx.UserValue(nameParam).(string)

	req := actors.DeleteTimerRequest{
		Name:      name,
		ActorID:   actorID,
		ActorType: actorType,
	}
	err := a.actor.DeleteTimer(reqCtx, &req)
	if err != nil {
		msg := NewErrorResponse("ERR_ACTOR_TIMER_DELETE", fmt.Sprintf(messages.ErrActorTimerDelete, err))
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
	} else {
		respond(reqCtx, withEmpty())
	}
}

func (a *api) onDirectActorMessage(reqCtx *fasthttp.RequestCtx) {
	if a.actor == nil {
		msg := NewErrorResponse("ERR_ACTOR_RUNTIME_NOT_FOUND", messages.ErrActorRuntimeNotFound)
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
		return
	}

	actorType := reqCtx.UserValue(actorTypeParam).(string)
	actorID := reqCtx.UserValue(actorIDParam).(string)
	verb := strings.ToUpper(string(reqCtx.Method()))
	method := reqCtx.UserValue(methodParam).(string)
	body := reqCtx.PostBody()

	req := invokev1.NewInvokeMethodRequest(method)
	req.WithActor(actorType, actorID)
	req.WithHTTPExtension(verb, reqCtx.QueryArgs().String())
	req.WithRawData(body, string(reqCtx.Request.Header.ContentType()))

	// Save headers to metadata
	metadata := map[string][]string{}
	reqCtx.Request.Header.VisitAll(func(key []byte, value []byte) {
		metadata[string(key)] = []string{string(value)}
	})
	req.WithMetadata(metadata)

	resp, err := a.actor.Call(reqCtx, req)
	if err != nil {
		msg := NewErrorResponse("ERR_ACTOR_INVOKE_METHOD", fmt.Sprintf(messages.ErrActorInvoke, err))
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
		return
	}

	invokev1.InternalMetadataToHTTPHeader(reqCtx, resp.Headers(), reqCtx.Response.Header.Set)
	contentType, body := resp.RawData()
	reqCtx.Response.Header.SetContentType(contentType)

	// Construct response
	statusCode := int(resp.Status().Code)
	if !resp.IsHTTPResponse() {
		statusCode = invokev1.HTTPStatusFromCode(codes.Code(statusCode))
	}
	respond(reqCtx, with(statusCode, body))
}

func (a *api) onGetActorState(reqCtx *fasthttp.RequestCtx) {
	if a.actor == nil {
		msg := NewErrorResponse("ERR_ACTOR_RUNTIME_NOT_FOUND", messages.ErrActorRuntimeNotFound)
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
		return
	}

	actorType := reqCtx.UserValue(actorTypeParam).(string)
	actorID := reqCtx.UserValue(actorIDParam).(string)
	key := reqCtx.UserValue(stateKeyParam).(string)

	hosted := a.actor.IsActorHosted(reqCtx, &actors.ActorHostedRequest{
		ActorType: actorType,
		ActorID:   actorID,
	})

	if !hosted {
		msg := NewErrorResponse("ERR_ACTOR_INSTANCE_MISSING", messages.ErrActorInstanceMissing)
		respond(reqCtx, withError(fasthttp.StatusBadRequest, msg))
		log.Debug(msg)
		return
	}

	req := actors.GetStateRequest{
		ActorType: actorType,
		ActorID:   actorID,
		Key:       key,
	}

	resp, err := a.actor.GetState(reqCtx, &req)
	if err != nil {
		msg := NewErrorResponse("ERR_ACTOR_STATE_GET", fmt.Sprintf(messages.ErrActorStateGet, err))
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
	} else {
		if resp == nil || resp.Data == nil {
			respond(reqCtx, withEmpty())
			return
		}
		respond(reqCtx, withJSON(fasthttp.StatusOK, resp.Data))
	}
}

func (a *api) onGetMetadata(reqCtx *fasthttp.RequestCtx) {
	temp := make(map[interface{}]interface{})

	// Copy synchronously so it can be serialized to JSON.
	a.extendedMetadata.Range(func(key, value interface{}) bool {
		temp[key] = value

		return true
	})

	activeActorsCount := []actors.ActiveActorsCount{}
	if a.actor != nil {
		activeActorsCount = a.actor.GetActiveActorsCount(reqCtx)
	}

	components := a.getComponentsFn()
	registeredComponents := make([]registeredComponent, 0, len(components))

	for _, comp := range components {
		registeredComp := registeredComponent{
			Name:    comp.Name,
			Version: comp.Spec.Version,
			Type:    comp.Spec.Type,
		}
		registeredComponents = append(registeredComponents, registeredComp)
	}

	mtd := metadata{
		ID:                   a.id,
		ActiveActorsCount:    activeActorsCount,
		Extended:             temp,
		RegisteredComponents: registeredComponents,
	}

	mtdBytes, err := a.json.Marshal(mtd)
	if err != nil {
		msg := NewErrorResponse("ERR_METADATA_GET", fmt.Sprintf(messages.ErrMetadataGet, err))
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
	} else {
		respond(reqCtx, withJSON(fasthttp.StatusOK, mtdBytes))
	}
}

func (a *api) onPutMetadata(reqCtx *fasthttp.RequestCtx) {
	key := fmt.Sprintf("%v", reqCtx.UserValue("key"))
	body := reqCtx.PostBody()
	a.extendedMetadata.Store(key, string(body))
	respond(reqCtx, withEmpty())
}

func (a *api) onShutdown(reqCtx *fasthttp.RequestCtx) {
	if !reqCtx.IsPost() {
		log.Warn("Please use POST method when invoking shutdown API")
	}

	respond(reqCtx, withEmpty())
	go func() {
		a.shutdown()
	}()
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

// GetStatusCodeFromMetadata extracts the http status code from the metadata if it exists.
func GetStatusCodeFromMetadata(metadata map[string]string) int {
	code := metadata[http.HTTPStatusCode]
	if code != "" {
		statusCode, err := strconv.Atoi(code)
		if err == nil {
			return statusCode
		}
	}

	return fasthttp.StatusOK
}

func (a *api) onGetHealthz(reqCtx *fasthttp.RequestCtx) {
	if !a.readyStatus {
		msg := NewErrorResponse("ERR_HEALTH_NOT_READY", messages.ErrHealthNotReady)
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
	} else {
		respond(reqCtx, withEmpty())
	}
}

func (a *api) onGetOutboundHealthz(reqCtx *fasthttp.RequestCtx) {
	if !a.outboundReadyStatus {
		msg := NewErrorResponse("ERR_HEALTH_NOT_READY", messages.ErrHealthNotReady)
		respond(reqCtx, withError(fasthttp.StatusInternalServerError, msg))
		log.Debug(msg)
	} else {
		respond(reqCtx, withEmpty())
	}
}

func (a *api) isSecretAllowed(storeName, key string) bool {
	if config, ok := a.secretsConfiguration[storeName]; ok {
		return config.IsSecretAllowed(key)
	}
	// By default, if a configuration is not defined for a secret store, return true.
	return true
}

func (a *api) SetAppChannel(appChannel channel.AppChannel) {
	a.appChannel = appChannel
}

func (a *api) SetDirectMessaging(directMessaging messaging.DirectMessaging) {
	a.directMessaging = directMessaging
}

func (a *api) SetActorRuntime(actor actors.Actors) {
	a.actor = actor
}
