// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package actors

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	nethttp "net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"

	"github.com/dapr/dapr/pkg/actors/internal"
	"github.com/dapr/dapr/pkg/channel"
	"github.com/dapr/dapr/pkg/concurrency"
	configuration "github.com/dapr/dapr/pkg/config"
	dapr_credentials "github.com/dapr/dapr/pkg/credentials"
	diag "github.com/dapr/dapr/pkg/diagnostics"
	diag_utils "github.com/dapr/dapr/pkg/diagnostics/utils"
	"github.com/dapr/dapr/pkg/health"
	invokev1 "github.com/dapr/dapr/pkg/messaging/v1"
	"github.com/dapr/dapr/pkg/modes"
	commonv1pb "github.com/dapr/dapr/pkg/proto/common/v1"
	internalv1pb "github.com/dapr/dapr/pkg/proto/internals/v1"
	"github.com/dapr/dapr/pkg/retry"
)

const (
	daprSeparator        = "||"
	metadataPartitionKey = "partitionKey"
)

var log = logger.NewLogger("dapr.runtime.actor")

var pattern = regexp.MustCompile(`^(R(?P<repetition>\d+)/)?P((?P<year>\d+)Y)?((?P<month>\d+)M)?((?P<week>\d+)W)?((?P<day>\d+)D)?(T((?P<hour>\d+)H)?((?P<minute>\d+)M)?((?P<second>\d+)S)?)?$`)

// Actors 允许调用虚拟actors以及actor的状态管理。
type Actors interface {
	Call(ctx context.Context, req *invokev1.InvokeMethodRequest) (*invokev1.InvokeMethodResponse, error)
	Init() error
	Stop()
	GetState(ctx context.Context, req *GetStateRequest) (*StateResponse, error)
	TransactionalStateOperation(ctx context.Context, req *TransactionalRequest) error
	GetReminder(ctx context.Context, req *GetReminderRequest) (*Reminder, error)
	CreateReminder(ctx context.Context, req *CreateReminderRequest) error
	DeleteReminder(ctx context.Context, req *DeleteReminderRequest) error
	CreateTimer(ctx context.Context, req *CreateTimerRequest) error
	DeleteTimer(ctx context.Context, req *DeleteTimerRequest) error
	IsActorHosted(ctx context.Context, req *ActorHostedRequest) bool
	GetActiveActorsCount(ctx context.Context) []ActiveActorsCount
}

type actorsRuntime struct {
	appChannel               channel.AppChannel
	store                    state.Store
	transactionalStore       state.TransactionalStore
	placement                *internal.ActorPlacement
	grpcConnectionFn         func(ctx context.Context, address, id string, namespace string, skipTLS, recreateIfExists, enableSSL bool, customOpts ...grpc.DialOption) (*grpc.ClientConn, error)
	config                   Config
	actorsTable              *sync.Map
	activeTimers             *sync.Map
	activeTimersLock         *sync.RWMutex
	activeReminders          *sync.Map
	remindersLock            *sync.RWMutex
	activeRemindersLock      *sync.RWMutex
	reminders                map[string][]actorReminderReference
	evaluationLock           *sync.RWMutex // 评估锁
	evaluationBusy           bool          // 评估 是否忙碌
	evaluationChan           chan bool     // 释放 评估不忙碌的信号
	appHealthy               *atomic.Bool
	certChain                *dapr_credentials.CertChain
	tracingSpec              configuration.TracingSpec
	reentrancyEnabled        bool
	actorTypeMetadataEnabled bool // actor 是否支持元数据
}

// ActiveActorsCount contain actorType and count of actors each type has.
type ActiveActorsCount struct {
	Type  string `json:"type"`
	Count int    `json:"count"`
}

// ActorMetadata represents information about the actor type.
type ActorMetadata struct {
	ID                string                 `json:"id"`
	RemindersMetadata ActorRemindersMetadata `json:"actorRemindersMetadata"`
	Etag              *string                `json:"-"`
}

// ActorRemindersMetadata represents information about actor's reminders.
type ActorRemindersMetadata struct {
	PartitionCount int                `json:"partitionCount"`
	partitionsEtag map[uint32]*string `json:"-"`
}

type actorReminderReference struct {
	actorMetadataID           string
	actorRemindersPartitionID uint32
	reminder                  *Reminder
}

const (
	incompatibleStateStore = "state store does not support transactions which actors require to save state - please see https://docs.dapr.io/operations/components/setup-state-store/supported-state-stores/"
)

// NewActors 使用提供的actor配置创建actor实例
func NewActors(
	stateStore state.Store,
	appChannel channel.AppChannel,
	grpcConnectionFn func(ctx context.Context, address, id string, namespace string, skipTLS, recreateIfExists, enableSSL bool, customOpts ...grpc.DialOption) (*grpc.ClientConn, error),
	config Config,
	certChain *dapr_credentials.CertChain,
	tracingSpec configuration.TracingSpec,
	features []configuration.FeatureSpec) Actors {
	var transactionalStore state.TransactionalStore
	if stateStore != nil {
		features := stateStore.Features()
		if state.FeatureETag.IsPresent(features) && state.FeatureTransactional.IsPresent(features) {
			transactionalStore = stateStore.(state.TransactionalStore)
		}
	}

	return &actorsRuntime{
		appChannel:               appChannel,
		config:                   config,
		store:                    stateStore,
		transactionalStore:       transactionalStore, // 事务存储
		grpcConnectionFn:         grpcConnectionFn,
		actorsTable:              &sync.Map{},
		activeTimers:             &sync.Map{},
		activeTimersLock:         &sync.RWMutex{},
		activeReminders:          &sync.Map{},
		remindersLock:            &sync.RWMutex{},
		activeRemindersLock:      &sync.RWMutex{},
		reminders:                map[string][]actorReminderReference{},
		evaluationLock:           &sync.RWMutex{},
		evaluationBusy:           false,
		evaluationChan:           make(chan bool),
		appHealthy:               atomic.NewBool(true),
		certChain:                certChain,
		tracingSpec:              tracingSpec,
		reentrancyEnabled:        configuration.IsFeatureEnabled(features, configuration.ActorReentrancy) && config.Reentrancy.Enabled,
		actorTypeMetadataEnabled: configuration.IsFeatureEnabled(features, configuration.ActorTypeMetadata),
	}
}

func (a *actorsRuntime) Init() error {
	if len(a.config.PlacementAddresses) == 0 {
		return errors.New("actor：无法连接到placement服务：地址为空")
	}
	// 与 pkg/runtime/runtime.go:616 功能一致
	if len(a.config.HostedActorTypes) > 0 {
		if a.store == nil {
			log.Warn("actors: 状态存储必须存在，以初始化actor的运行时间")
		} else {
			features := a.store.Features()
			// 如果用户强制设置了actorStateStore 需要判断是不是支持这两个特性
			if !state.FeatureETag.IsPresent(features) || !state.FeatureTransactional.IsPresent(features) {
				return errors.New(incompatibleStateStore)
			}
		}
	}

	hostname := fmt.Sprintf("%s:%d", a.config.HostAddress, a.config.Port) //daprd 的内部grpc端口
	// 表更新之后调用的函数
	afterTableUpdateFn := func() {
		a.drainRebalancedActors()
		a.evaluateReminders()
	}
	appHealthFn := func() bool { return a.appHealthy.Load() } // 应用健康检查函数
	//初始化actor服务的 ActorPlacement结构
	a.placement = internal.NewActorPlacement(
		a.config.PlacementAddresses, a.certChain,
		a.config.AppID, hostname, a.config.HostedActorTypes,
		appHealthFn,
		afterTableUpdateFn)

	go a.placement.Start() // 1、注册自身 2、接收来自placement的信息， 包括所有的节点
	//a.placement.Start() // 1、注册自身 2、接收来自placement的信息， 包括所有的节点
	// todo  默认30s,1h
	a.startDeactivationTicker(a.config.ActorDeactivationScanInterval, a.config.ActorIdleTimeout)

	log.Infof("actor运行时已启动. actor 空闲超时: %s. actor 扫描间隔: %s",
		a.config.ActorIdleTimeout.String(), a.config.ActorDeactivationScanInterval.String())

	//在配置healthz端点选项时要小心。
	//如果app healthz返回不健康状态，Dapr将断开放置，将节点从一致的哈希环中移除。
	//例如，如果应用程序是忙碌的状态，健康状态将是不稳定的，这导致频繁的演员再平衡。这将影响整个服务。
	go a.startAppHealthCheck(
		health.WithFailureThreshold(4),           // 失败次数
		health.WithInterval(5*time.Second),       // 检查周期
		health.WithRequestTimeout(2*time.Second)) // 请求超时

	return nil
}

// 启动应用健康检查
func (a *actorsRuntime) startAppHealthCheck(opts ...health.Option) {
	if len(a.config.HostedActorTypes) == 0 {
		return
	}
	//http://127.0.0.1:3001/healthz
	healthAddress := fmt.Sprintf("%s/healthz", a.appChannel.GetBaseAddress())
	ch := health.StartEndpointHealthCheck(healthAddress, opts...)
	for {
		appHealthy := <-ch
		a.appHealthy.Store(appHealthy)
	}
}

// 拼接key
func constructCompositeKey(keys ...string) string {
	return strings.Join(keys, daprSeparator)
}

// 拆分key
func decomposeCompositeKey(compositeKey string) []string {
	return strings.Split(compositeKey, daprSeparator)
}

func (a *actorsRuntime) deactivateActor(actorType, actorID string) error {
	req := invokev1.NewInvokeMethodRequest(fmt.Sprintf("actors/%s/%s", actorType, actorID))
	req.WithHTTPExtension(nethttp.MethodDelete, "")
	req.WithRawData(nil, invokev1.JSONContentType)

	// TODO Propagate context
	ctx := context.Background()
	resp, err := a.appChannel.InvokeMethod(ctx, req)
	if err != nil {
		diag.DefaultMonitoring.ActorDeactivationFailed(actorType, "invoke")
		return err
	}

	if resp.Status().Code != nethttp.StatusOK {
		diag.DefaultMonitoring.ActorDeactivationFailed(actorType, fmt.Sprintf("status_code_%d", resp.Status().Code))
		_, body := resp.RawData()
		return errors.Errorf("error from actor service: %s", string(body))
	}

	actorKey := constructCompositeKey(actorType, actorID)
	a.actorsTable.Delete(actorKey)
	diag.DefaultMonitoring.ActorDeactivated(actorType)
	log.Debugf("deactivated actor type=%s, id=%s\n", actorType, actorID)

	return nil
}

func (a *actorsRuntime) getActorTypeAndIDFromKey(key string) (string, string) {
	arr := decomposeCompositeKey(key)
	return arr[0], arr[1]
}

// 创建失活触发器   30s 60m
func (a *actorsRuntime) startDeactivationTicker(interval, actorIdleTimeout time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for t := range ticker.C {
			a.actorsTable.Range(func(key, value interface{}) bool {
				log.Info(key, value)
				actorInstance := value.(*actor)

				if actorInstance.isBusy() {
					return true
				}

				durationPassed := t.Sub(actorInstance.lastUsedTime)
				if durationPassed >= actorIdleTimeout {
					go func(actorKey string) {
						actorType, actorID := a.getActorTypeAndIDFromKey(actorKey)
						err := a.deactivateActor(actorType, actorID)
						if err != nil {
							log.Errorf("停止actor失败 %s: %s", actorKey, err)
						}
					}(key.(string))
				}

				return true
			})
		}
	}()
}

// Call ok
func (a *actorsRuntime) Call(ctx context.Context, req *invokev1.InvokeMethodRequest) (*invokev1.InvokeMethodResponse, error) {
	a.placement.WaitUntilPlacementTableIsReady()

	actor := req.Actor()
	targetActorAddress, appID := a.placement.LookupActor(actor.GetActorType(), actor.GetActorId())
	if targetActorAddress == "" {
		return nil, errors.Errorf("查找actor type %s; id %s 的地址出错", actor.GetActorType(), actor.GetActorId())
	}

	var resp *invokev1.InvokeMethodResponse
	var err error

	if a.isActorLocal(targetActorAddress, a.config.HostAddress, a.config.Port) {
		resp, err = a.callLocalActor(ctx, req) // 直接调用本身actor
	} else {
		// 重试次数、重试间隔、func、目标主机地址、目标主机应用ID、请求
		resp, err = a.callRemoteActorWithRetry(ctx, retry.DefaultLinearRetryCount,
			retry.DefaultLinearBackoffInterval, a.callRemoteActor, targetActorAddress, appID, req,
		)
	}

	if err != nil {
		return nil, err
	}
	return resp, nil
}

// callRemoteActorWithRetry
//将在指定的重试次数内调用一个远程行为体，并且只在瞬时失败的情况下重试。
func (a *actorsRuntime) callRemoteActorWithRetry(
	ctx context.Context,
	numRetries int,
	backoffInterval time.Duration,
	fn func(ctx context.Context, targetAddress, targetID string, req *invokev1.InvokeMethodRequest) (*invokev1.InvokeMethodResponse, error),
	targetAddress, targetID string, req *invokev1.InvokeMethodRequest) (*invokev1.InvokeMethodResponse, error) {
	for i := 0; i < numRetries; i++ {
		resp, err := fn(ctx, targetAddress, targetID, req)
		if err == nil {
			return resp, nil
		}
		time.Sleep(backoffInterval)

		code := status.Code(err)
		if code == codes.Unavailable || code == codes.Unauthenticated {
			_, err = a.grpcConnectionFn(context.TODO(), targetAddress, targetID, a.config.Namespace, false, true, false)
			if err != nil {
				return nil, err
			}
			continue
		}
		return resp, err
	}
	return nil, errors.Errorf("failed to invoke target %s after %v retries", targetAddress, numRetries)
}

// 获取或者创建一个actor
func (a *actorsRuntime) getOrCreateActor(actorType, actorID string) *actor {
	//构造组合键    actorType||actorID
	key := constructCompositeKey(actorType, actorID)

	// 这就避免了在每次调用actor时都调用newActor来分配多个actor。
	// 当首先存储actor key时，有机会调用newActor，但这是微不足道的。
	val, ok := a.actorsTable.Load(key)
	if !ok {
		val, _ = a.actorsTable.LoadOrStore(key, newActor(actorType, actorID, a.config.Reentrancy.MaxStackDepth))
	}

	return val.(*actor)
}

// 直接调用本身actor
func (a *actorsRuntime) callLocalActor(ctx context.Context, req *invokev1.InvokeMethodRequest) (*invokev1.InvokeMethodResponse, error) {
	actorTypeID := req.Actor()

	act := a.getOrCreateActor(actorTypeID.GetActorType(), actorTypeID.GetActorId())

	// 重入性来决定我们如何锁定。  可重入,则一定是线程安全的;类似于 递归,需要保存栈
	var reentrancyID *string
	if a.reentrancyEnabled {
		if headerValue, ok := req.Metadata()["Dapr-Reentrancy-Id"]; ok {
			reentrancyID = &headerValue.GetValues()[0]
		} else {
			reentrancyHeader := fasthttp.RequestHeader{}
			uuid := uuid.New().String()
			reentrancyHeader.Add("Dapr-Reentrancy-Id", uuid)
			req.AddHeaders(&reentrancyHeader)
			reentrancyID = &uuid
		}
	}

	err := act.lock(reentrancyID)
	if err != nil {
		return nil, status.Error(codes.ResourceExhausted, err.Error())
	}
	defer act.unlock()

	// 替换方法为actor方法
	req.Message().Method = fmt.Sprintf("actors/%s/%s/method/%s",
		actorTypeID.GetActorType(), actorTypeID.GetActorId(), req.Message().Method)
	// 原代码用PUT覆盖了方法。为什么？
	if req.Message().GetHttpExtension() == nil {
		req.WithHTTPExtension(nethttp.MethodPut, "")
	} else {
		req.Message().HttpExtension.Verb = commonv1pb.HTTPExtension_PUT
	}
	resp, err := a.appChannel.InvokeMethod(ctx, req)
	if err != nil {
		return nil, err
	}

	_, respData := resp.RawData()

	if resp.Status().Code != nethttp.StatusOK {
		return nil, errors.Errorf("error from actor service: %s", string(respData))
	}

	return resp, nil
}

// OK
func (a *actorsRuntime) callRemoteActor(
	ctx context.Context,
	targetAddress, targetID string,
	req *invokev1.InvokeMethodRequest) (*invokev1.InvokeMethodResponse, error) {
	conn, err := a.grpcConnectionFn(context.TODO(), targetAddress, targetID,
		a.config.Namespace, false, false, false)
	if err != nil {
		return nil, err
	}

	span := diag_utils.SpanFromContext(ctx)
	ctx = diag.SpanContextToGRPCMetadata(ctx, span.SpanContext())
	client := internalv1pb.NewServiceInvocationClient(conn)
	resp, err := client.CallActor(ctx, req.Proto()) // 远程sidecar的CallActor
	if err != nil {
		return nil, err
	}

	return invokev1.InternalInvokeResponse(resp)
}

// 判断目标actor地址是不是在本机,以及是不是actor本身
func (a *actorsRuntime) isActorLocal(targetActorAddress, hostAddress string, grpcPort int) bool {
	return strings.Contains(targetActorAddress, "localhost") ||
		strings.Contains(targetActorAddress, "127.0.0.1") ||
		targetActorAddress == fmt.Sprintf("%s:%v", hostAddress, grpcPort)
}

// GetState OK
func (a *actorsRuntime) GetState(ctx context.Context, req *GetStateRequest) (*StateResponse, error) {
	if a.store == nil {
		return nil, errors.New("actors: 状态存储不存在或配置不正确")
	}

	partitionKey := constructCompositeKey(a.config.AppID, req.ActorType, req.ActorID)
	metadata := map[string]string{metadataPartitionKey: partitionKey}

	key := a.constructActorStateKey(req.ActorType, req.ActorID, req.Key)
	resp, err := a.store.Get(&state.GetRequest{
		Key:      key,
		Metadata: metadata,
	})
	if err != nil {
		return nil, err
	}

	return &StateResponse{
		Data: resp.Data,
	}, nil
}

// TransactionalStateOperation 状态事务操作
func (a *actorsRuntime) TransactionalStateOperation(ctx context.Context, req *TransactionalRequest) error {
	if a.store == nil || a.transactionalStore == nil {
		return errors.New("actors：状态存储不存在或配置不正确")
	}
	var operations []state.TransactionalStateOperation
	partitionKey := constructCompositeKey(a.config.AppID, req.ActorType, req.ActorID)
	metadata := map[string]string{metadataPartitionKey: partitionKey} // 元数据分区key

	for _, o := range req.Operations {
		// 防止用户输入的Operation不是我们提供的
		switch o.Operation {
		case Upsert:
			var upsert TransactionalUpsert
			err := mapstructure.Decode(o.Request, &upsert)
			if err != nil {
				return err
			}
			key := a.constructActorStateKey(req.ActorType, req.ActorID, upsert.Key)
			operations = append(operations, state.TransactionalStateOperation{
				Request: state.SetRequest{
					Key:      key,
					Value:    upsert.Value,
					Metadata: metadata,
				},
				Operation: state.Upsert,
			})
		case Delete:
			var delete TransactionalDelete
			err := mapstructure.Decode(o.Request, &delete)
			if err != nil {
				return err
			}

			key := a.constructActorStateKey(req.ActorType, req.ActorID, delete.Key)
			operations = append(operations, state.TransactionalStateOperation{
				Request: state.DeleteRequest{
					Key:      key,
					Metadata: metadata,
				},
				Operation: state.Delete,
			})
		default:
			return errors.Errorf("操作类型不支持 %s ", o.Operation)
		}
	}

	err := a.transactionalStore.Multi(&state.TransactionalStateRequest{
		Operations: operations,
		Metadata:   metadata,
	})
	return err
}

func (a *actorsRuntime) IsActorHosted(ctx context.Context, req *ActorHostedRequest) bool {
	key := constructCompositeKey(req.ActorType, req.ActorID)
	// 需要actorsTable中同步其余数据
	_, exists := a.actorsTable.Load(key)
	return exists
}

// 构建actor状态存储的key
func (a *actorsRuntime) constructActorStateKey(actorType, actorID, key string) string {
	return constructCompositeKey(a.config.AppID, actorType, actorID, key)
}

// 重新平衡actor
func (a *actorsRuntime) drainRebalancedActors() {
	// 访问所有目前活跃的actor
	var wg sync.WaitGroup

	a.actorsTable.Range(func(key interface{}, value interface{}) bool {
		wg.Add(1)
		go func(key interface{}, value interface{}, wg *sync.WaitGroup) {
			defer wg.Done()
			// for each actor, deactivate if no longer hosted locally
			actorKey := key.(string)
			actorType, actorID := a.getActorTypeAndIDFromKey(actorKey)
			address, _ := a.placement.LookupActor(actorType, actorID)
			if address != "" && !a.isActorLocal(address, a.config.HostAddress, a.config.Port) {
				// actor has been moved to a different host, deactivate when calls are done cancel any reminders
				// each item in reminders contain a struct with some metadata + the actual reminder struct
				reminders := a.reminders[actorType]
				for _, r := range reminders {
					// r.reminder refers to the actual reminder struct that is saved in the db
					if r.reminder.ActorType == actorType && r.reminder.ActorID == actorID {
						reminderKey := constructCompositeKey(actorKey, r.reminder.Name)
						stopChan, exists := a.activeReminders.Load(reminderKey)
						if exists {
							close(stopChan.(chan bool))
							a.activeReminders.Delete(reminderKey)
						}
					}
				}

				actor := value.(*actor)
				if a.config.DrainRebalancedActors {
					// wait until actor isn't busy or timeout hits
					if actor.isBusy() {
						select {
						case <-time.After(a.config.DrainOngoingCallTimeout):
							break
						case <-actor.channel():
							// if a call comes in from the actor for state changes, that's still allowed
							break
						}
					}
				}

				// don't allow state changes
				a.actorsTable.Delete(key)

				diag.DefaultMonitoring.ActorRebalanced(actorType)

				for {
					// wait until actor is not busy, then deactivate
					if !actor.isBusy() {
						err := a.deactivateActor(actorType, actorID)
						if err != nil {
							log.Errorf("failed to deactivate actor %s: %s", actorKey, err)
						}
						break
					}
					time.Sleep(time.Millisecond * 500)
				}
			}
		}(key, value, &wg)
		return true
	})
}

//评估提示
func (a *actorsRuntime) evaluateReminders() {
	a.evaluationLock.Lock()
	defer a.evaluationLock.Unlock()

	a.evaluationBusy = true
	a.evaluationChan = make(chan bool)

	var wg sync.WaitGroup
	for _, t := range a.config.HostedActorTypes {
		vals, _, err := a.getRemindersForActorType(t, true)
		if err != nil {
			log.Errorf("error getting reminders for actor type %s: %s", t, err)
		} else {
			a.remindersLock.Lock()
			a.reminders[t] = vals
			a.remindersLock.Unlock()

			wg.Add(1)
			go func(wg *sync.WaitGroup, reminders []actorReminderReference) {
				defer wg.Done()

				for i := range reminders {
					r := reminders[i] // Make a copy since we will refer to this as a reference in this loop.
					targetActorAddress, _ := a.placement.LookupActor(r.reminder.ActorType, r.reminder.ActorID)
					if targetActorAddress == "" {
						continue
					}

					if a.isActorLocal(targetActorAddress, a.config.HostAddress, a.config.Port) {
						actorKey := constructCompositeKey(r.reminder.ActorType, r.reminder.ActorID)
						reminderKey := constructCompositeKey(actorKey, r.reminder.Name)
						_, exists := a.activeReminders.Load(reminderKey)

						if !exists {
							stop := make(chan bool)
							a.activeReminders.Store(reminderKey, stop)
							err := a.startReminder(r.reminder, stop)
							if err != nil {
								log.Errorf("error starting reminder: %s", err)
							}
						}
					}
				}
			}(&wg, vals)
		}
	}
	wg.Wait()
	close(a.evaluationChan)
	a.evaluationBusy = false
}

func (a *actorsRuntime) getReminderTrack(actorKey, name string) (*ReminderTrack, error) {
	if a.store == nil {
		return nil, errors.New("actors: 状态存储不存在或配置不正确")
	}

	resp, err := a.store.Get(&state.GetRequest{
		Key: constructCompositeKey(actorKey, name),
	})
	if err != nil {
		return nil, err
	}

	track := ReminderTrack{
		RepetitionLeft: -1,
	}
	json.Unmarshal(resp.Data, &track)
	return &track, nil
}

func (a *actorsRuntime) updateReminderTrack(actorKey, name string, repetition int, lastInvokeTime time.Time) error {
	if a.store == nil {
		return errors.New("actors: 状态存储不存在或配置不正确")
	}

	track := ReminderTrack{
		LastFiredTime:  lastInvokeTime.Format(time.RFC3339),
		RepetitionLeft: repetition,
	}

	err := a.store.Set(&state.SetRequest{
		Key:   constructCompositeKey(actorKey, name),
		Value: track,
	})
	return err
}

func (a *actorsRuntime) startReminder(reminder *Reminder, stopChannel chan bool) error { // 启动reminder
	actorKey := constructCompositeKey(reminder.ActorType, reminder.ActorID)
	reminderKey := constructCompositeKey(actorKey, reminder.Name)

	var (
		nextTime, ttl            time.Time
		period                   time.Duration
		repeats, repetitionsLeft int
	)
	// 获取注册时间
	registeredTime, err := time.Parse(time.RFC3339, reminder.RegisteredTime)
	if err != nil {
		return errors.Wrap(err, "解析reminder注册时间失败")
	}
	if len(reminder.ExpirationTime) != 0 {
		if ttl, err = time.Parse(time.RFC3339, reminder.ExpirationTime); err != nil {
			return errors.Wrap(err, "解析reminder过期时间失败")
		}
	}

	repeats = -1 // set to default
	if len(reminder.Period) != 0 {
		if period, repeats, err = parseDuration(reminder.Period); err != nil {
			return errors.Wrap(err, "解析reminder周期失败")
		}
	}
	//是一个持久化的对象，它记录了最后一次提醒的时间。
	track, err := a.getReminderTrack(actorKey, reminder.Name)
	if err != nil {
		return errors.Wrap(err, "获取reminder执行信息失败")
	}

	if track != nil && len(track.LastFiredTime) != 0 {
		lastFiredTime, err := time.Parse(time.RFC3339, track.LastFiredTime)
		if err != nil {
			return errors.Wrap(err, "获取reminder上一次执行时间失败")
		}
		repetitionsLeft = track.RepetitionLeft
		nextTime = lastFiredTime.Add(period)
	} else {
		repetitionsLeft = repeats
		nextTime = registeredTime
	}

	go func(reminder *Reminder, period time.Duration, nextTime, ttl time.Time, repetitionsLeft int, stop chan bool) {
		var (
			ttlTimer, nextTimer *time.Timer
			ttlTimerC           <-chan time.Time
			err                 error
		)
		// 不是零值
		if !ttl.IsZero() {
			ttlTimer = time.NewTimer(time.Until(ttl))
			ttlTimerC = ttlTimer.C
		}
		nextTimer = time.NewTimer(time.Until(nextTime))
		defer func() {
			if nextTimer.Stop() {
				<-nextTimer.C
			}
			if ttlTimer != nil && ttlTimer.Stop() {
				<-ttlTimerC
			}
		}()
	L:
		for {
			select {
			case v := <-nextTimer.C:
				log.Debug(v)
				// noop
			case <-ttlTimerC:
				// 继续删除提醒信息
				log.Infof("reminder %s 过期了", reminder.Name)
				break L
			case <-stop:
				// reminder 被删除
				log.Infof("reminder %s with parameters: dueTime: %s, period: %s, data: %v 被删除了", reminder.Name, reminder.RegisteredTime, reminder.Period, reminder.Data)
				return
			}

			_, exists := a.activeReminders.Load(reminderKey)
			if !exists {
				log.Errorf("不能查找到活跃的 reminder  by key: %s", reminderKey)
				return
			}
			// 如果所有的重复都已完成，则继续进行提醒删除。
			if repetitionsLeft == 0 {
				log.Infof("reminder %q 完成了 %d ", reminder.Name, repeats)
				break L
			}
			if err = a.executeReminder(reminder); err != nil {
				log.Errorf("error 执行reminder %q ;actor type %s; id %s; err: %v",
					reminder.Name, reminder.ActorType, reminder.ActorID, err)
			}
			if repetitionsLeft > 0 {
				repetitionsLeft--
			}
			if err = a.updateReminderTrack(actorKey, reminder.Name, repetitionsLeft, nextTime); err != nil {
				log.Errorf("更新reminder执行信息失败 %v", err)
			}
			// 如果reminder不是重复的，则继续删除reminder
			if period == 0 {
				break L
			}
			nextTime = nextTime.Add(period)
			if nextTimer.Stop() {
				<-nextTimer.C
			}
			nextTimer.Reset(time.Until(nextTime))
		}
		err = a.DeleteReminder(context.TODO(), &DeleteReminderRequest{
			Name:      reminder.Name,
			ActorID:   reminder.ActorID,
			ActorType: reminder.ActorType,
		})
		if err != nil {
			log.Errorf("error deleting reminder: %s", err)
		}
	}(reminder, period, nextTime, ttl, repetitionsLeft, stopChannel)

	return nil
}

//type Reminder struct {
//	ActorID        actorId-a
//	ActorType      actorType-a
//	Name           demo
//	RegisteredTime 2022-01-04T14:35:37+08:00
//}
// 执行某个reminder
func (a *actorsRuntime) executeReminder(reminder *Reminder) error {
	r := ReminderResponse{
		DueTime: reminder.DueTime,
		Period:  reminder.Period,
		Data:    reminder.Data,
	}
	b, err := json.Marshal(&r)
	if err != nil {
		return err
	}

	log.Debugf("执行 reminder %s ;actor type %s ; id %s", reminder.Name, reminder.ActorType, reminder.ActorID)
	req := invokev1.NewInvokeMethodRequest(fmt.Sprintf("remind/%s", reminder.Name))
	req.WithActor(reminder.ActorType, reminder.ActorID)
	req.WithRawData(b, invokev1.JSONContentType)

	_, err = a.callLocalActor(context.Background(), req)
	return err
}

// ok
func (a *actorsRuntime) reminderRequiresUpdate(req *CreateReminderRequest, reminder *Reminder) bool {
	if reminder.ActorID == req.ActorID && reminder.ActorType == req.ActorType && reminder.Name == req.Name &&
		(!reflect.DeepEqual(reminder.Data, req.Data) || reminder.DueTime != req.DueTime || reminder.Period != req.Period ||
			len(req.TTL) != 0 || (len(reminder.ExpirationTime) != 0 && len(req.TTL) == 0)) {
		return true
	}

	return false
}

// ok
func (a *actorsRuntime) getReminder(req *CreateReminderRequest) (*Reminder, bool) {
	a.remindersLock.RLock()
	reminders := a.reminders[req.ActorType] // 用户随便写
	a.remindersLock.RUnlock()
	//判断有没有已经存在的 reminder
	for _, r := range reminders {
		if r.reminder.ActorID == req.ActorID && r.reminder.ActorType == req.ActorType && r.reminder.Name == req.Name {
			return r.reminder, true
		}
	}

	return nil, false
}

func (m *ActorMetadata) calculateReminderPartition(actorID, reminderName string) uint32 {
	if m.RemindersMetadata.PartitionCount <= 0 {
		return 0
	}

	// do not change this hash function because it would be a breaking change.
	h := fnv.New32a()
	h.Write([]byte(actorID))
	h.Write([]byte(reminderName))
	return (h.Sum32() % uint32(m.RemindersMetadata.PartitionCount)) + 1
}

func (m *ActorMetadata) createReminderReference(reminder *Reminder) actorReminderReference {
	if m.RemindersMetadata.PartitionCount > 0 {
		return actorReminderReference{
			actorMetadataID:           m.ID,
			actorRemindersPartitionID: m.calculateReminderPartition(reminder.ActorID, reminder.Name),
			reminder:                  reminder,
		}
	}

	return actorReminderReference{
		actorMetadataID:           "",
		actorRemindersPartitionID: 0,
		reminder:                  reminder,
	}
}

func (m *ActorMetadata) calculateRemindersStateKey(actorType string, remindersPartitionID uint32) string {
	if remindersPartitionID == 0 {
		return constructCompositeKey("actors", actorType)
	}

	return constructCompositeKey(
		"actors",
		actorType,
		m.ID,
		"reminders",
		strconv.Itoa(int(remindersPartitionID)))
}

func (m *ActorMetadata) calculateEtag(partitionID uint32) *string {
	return m.RemindersMetadata.partitionsEtag[partitionID]
}

func (m *ActorMetadata) removeReminderFromPartition(reminderRefs []actorReminderReference, actorType, actorID, reminderName string) ([]Reminder, string, *string) {
	// First, we find the partition
	var partitionID uint32 = 0
	if m.RemindersMetadata.PartitionCount > 0 {
		for _, reminderRef := range reminderRefs {
			if reminderRef.reminder.ActorType == actorType && reminderRef.reminder.ActorID == actorID && reminderRef.reminder.Name == reminderName {
				partitionID = reminderRef.actorRemindersPartitionID
			}
		}
	}

	var remindersInPartitionAfterRemoval []Reminder
	for _, reminderRef := range reminderRefs {
		if reminderRef.reminder.ActorType == actorType && reminderRef.reminder.ActorID == actorID && reminderRef.reminder.Name == reminderName {
			continue
		}

		// Only the items in the partition to be updated.
		if reminderRef.actorRemindersPartitionID == partitionID {
			remindersInPartitionAfterRemoval = append(remindersInPartitionAfterRemoval, *reminderRef.reminder)
		}
	}

	stateKey := m.calculateRemindersStateKey(actorType, partitionID)
	return remindersInPartitionAfterRemoval, stateKey, m.calculateEtag(partitionID)
}

func (m *ActorMetadata) insertReminderInPartition(reminderRefs []actorReminderReference, reminder *Reminder) ([]Reminder, actorReminderReference, string, *string) {
	newReminderRef := m.createReminderReference(reminder)

	var remindersInPartitionAfterInsertion []Reminder
	for _, reminderRef := range reminderRefs {
		// Only the items in the partition to be updated.
		if reminderRef.actorRemindersPartitionID == newReminderRef.actorRemindersPartitionID {
			remindersInPartitionAfterInsertion = append(remindersInPartitionAfterInsertion, *reminderRef.reminder)
		}
	}

	remindersInPartitionAfterInsertion = append(remindersInPartitionAfterInsertion, *reminder)

	stateKey := m.calculateRemindersStateKey(newReminderRef.reminder.ActorType, newReminderRef.actorRemindersPartitionID)
	return remindersInPartitionAfterInsertion, newReminderRef, stateKey, m.calculateEtag(newReminderRef.actorRemindersPartitionID)
}

func (m *ActorMetadata) calculateDatabasePartitionKey(stateKey string) string {
	if m.RemindersMetadata.PartitionCount > 0 {
		return m.ID
	}

	return stateKey
}

// ok
func (a *actorsRuntime) CreateReminder(ctx context.Context, req *CreateReminderRequest) error {
	if a.store == nil {
		return errors.New("actors: 状态存储不存在或配置不正确")
	}

	a.activeRemindersLock.Lock()
	defer a.activeRemindersLock.Unlock()
	// 如果存在符合条件的reminder
	if r, exists := a.getReminder(req); exists {
		if a.reminderRequiresUpdate(req, r) {
			err := a.DeleteReminder(ctx, &DeleteReminderRequest{
				ActorID:   req.ActorID,
				ActorType: req.ActorType,
				Name:      req.Name,
			})
			if err != nil {
				return err
			}
		} else {
			return nil
		}
	}

	// 在活动提醒列表中存储提醒信息
	actorKey := constructCompositeKey(req.ActorType, req.ActorID)
	reminderKey := constructCompositeKey(actorKey, req.Name)

	if a.evaluationBusy {
		select {
		case <-time.After(time.Second * 5):
			return errors.New("创建提醒时出错：5秒后超时了")
		case <-a.evaluationChan:
			break
		}
	}

	now := time.Now()
	reminder := Reminder{
		ActorID:   req.ActorID,
		ActorType: req.ActorType,
		Name:      req.Name,
		Data:      req.Data,
		Period:    req.Period,
		DueTime:   req.DueTime,
	}

	// 检查输入是否正确
	var (
		dueTime, ttl time.Time
		repeats      int
		err          error
	)
	if len(req.DueTime) != 0 {
		if dueTime, err = parseTime(req.DueTime, nil); err != nil {
			return errors.Wrap(err, "错误解析提醒到期时间")
		}
	} else {
		dueTime = now
	}
	reminder.RegisteredTime = dueTime.Format(time.RFC3339)

	if len(req.Period) != 0 {
		_, repeats, err = parseDuration(req.Period)
		if err != nil {
			return errors.Wrap(err, "错误解析提醒周期")
		}
		// 对重复次数为零的计时器有错误
		if repeats == 0 {
			return errors.Errorf("提醒%s的重复次数为零", reminder.Name)
		}
	}
	if len(req.TTL) > 0 {
		if ttl, err = parseTime(req.TTL, &dueTime); err != nil {
			return errors.Wrap(err, "error parsing reminder TTL")
		}
		if now.After(ttl) || dueTime.After(ttl) {
			return errors.Errorf("reminder %s 已经过期: registeredTime: %s TTL:%s",
				reminderKey, reminder.RegisteredTime, req.TTL)
		}
		reminder.ExpirationTime = ttl.UTC().Format(time.RFC3339)
	}

	stop := make(chan bool)
	a.activeReminders.Store(reminderKey, stop)

	err = backoff.Retry(func() error {
		// 将数据存储到actorStateStore中
		reminders, actorMetadata, err2 := a.getRemindersForActorType(req.ActorType, true)
		if err2 != nil {
			return err2
		}

		// 首先，我们把它添加到分区列表中。
		remindersInPartition, reminderRef, stateKey, etag := actorMetadata.insertReminderInPartition(reminders, &reminder)

		// 获取数据库分区密钥（CosmosDB需要）。
		databasePartitionKey := actorMetadata.calculateDatabasePartitionKey(stateKey)

		// 现在我们可以把它添加到 "全局 "列表中。
		reminders = append(reminders, reminderRef)

		// 然后，将该分区保存到数据库。
		err2 = a.saveRemindersInPartition(ctx, stateKey, remindersInPartition, etag, databasePartitionKey)
		if err2 != nil {
			return err2
		}

		// Finally, we must save metadata to get a new eTag.
		// This avoids a race condition between an update and a repartitioning.
		a.saveActorTypeMetadata(req.ActorType, actorMetadata)

		a.remindersLock.Lock()
		a.reminders[req.ActorType] = reminders
		a.remindersLock.Unlock()
		return nil
	}, backoff.NewExponentialBackOff())
	if err != nil {
		return err
	}
	return a.startReminder(&reminder, stop)
}

// CreateTimer ok
func (a *actorsRuntime) CreateTimer(ctx context.Context, req *CreateTimerRequest) error {
	var (
		err          error
		repeats      int
		dueTime, ttl time.Time
		period       time.Duration
	)
	a.activeTimersLock.Lock()
	defer a.activeTimersLock.Unlock()
	actorKey := constructCompositeKey(req.ActorType, req.ActorID)
	timerKey := constructCompositeKey(actorKey, req.Name)

	_, exists := a.actorsTable.Load(actorKey) // 判断actor存不存在
	if !exists {
		return errors.Errorf("不能创建actor: %s定时器: actor未激活", actorKey)
	}

	stopChan, exists := a.activeTimers.Load(timerKey) // 判断有没有创建过
	if exists {
		// 如果存在,关闭 stopChan
		close(stopChan.(chan bool))
	}

	if len(req.DueTime) != 0 {
		// 0h30m0s、R5/PT30M、P1MT2H10M3S、time.Now().Truncate(time.Minute).Add(time.Minute).Format(time.RFC3339)
		if dueTime, err = parseTime(req.DueTime, nil); err != nil {
			return errors.Wrap(err, "解析过期时间出错")
		}
		if time.Now().After(dueTime) {
			return errors.Errorf("定时器 %s 已经过期: 过期时间: %s 存活时间: %s", timerKey, req.DueTime, req.TTL)
		}
	} else {
		dueTime = time.Now()
	}

	repeats = -1 // set to default
	if len(req.Period) != 0 {
		// 解析时间、获的有多少秒数    0h30m0s  ---> 18好几个0 , -1 ,nil
		// R5/PT30M --->  18好几个0 , 5 ,nil
		if period, repeats, err = parseDuration(req.Period); err != nil {
			return errors.Wrap(err, "解析触发时长出错")
		}
		if repeats == 0 {
			return errors.Errorf("timer %s 0次触发", timerKey)
		}
	}

	if len(req.TTL) > 0 {
		//在过期时间上加上生存时间
		if ttl, err = parseTime(req.TTL, &dueTime); err != nil {
			return errors.Wrap(err, "解析定时器TTL出错")
		}
		//  为啥不判断  dueTime 相对于当前时间

		//  ------------------------------------------->
		//       👆🏻dueTime   👆🏻ttl            👆🏻now
		if time.Now().After(ttl) || dueTime.After(ttl) {
			return errors.Errorf("定时器 %s 已经过期: 过期时间: %s 存活时间: %s", timerKey, req.DueTime, req.TTL)
		}
	}

	log.Debugf("创建定时器 %q 到期时间:%s 周期:%s 重复:%d次 存活时间:%s",
		req.Name, dueTime.String(), period.String(), repeats, ttl.String())
	stop := make(chan bool, 1)
	a.activeTimers.Store(timerKey, stop)

	go func(stop chan bool, req *CreateTimerRequest) {
		var (
			ttlTimer, nextTimer *time.Timer
			ttlTimerC           <-chan time.Time
			err                 error
		)
		if !ttl.IsZero() {
			ttlTimer = time.NewTimer(time.Until(ttl))
			ttlTimerC = ttlTimer.C
		}
		nextTime := dueTime
		nextTimer = time.NewTimer(time.Until(nextTime)) // 定时器 , 只执行一次，如果时间是以前，那么现在执行一次
		defer func() {
			if nextTimer.Stop() {
				<-nextTimer.C
			}
			if ttlTimer != nil && ttlTimer.Stop() {
				<-ttlTimerC
			}
		}()
	L:
		for {
			select {
			case <-nextTimer.C:
				// noop
			case <-ttlTimerC:
				// 计时器已经过期，继续删除
				log.Infof("参数为 dueTime: %s, period: %s, TTL: %s, data: %v 的定时器 %s 已经过期。", timerKey, req.DueTime, req.Period, req.TTL, req.Data)
				break L
			case <-stop:
				// 计时器已被删除
				log.Infof("参数为 dueTime: %s, period: %s, TTL: %s, data: %v 的定时器 %s 已被删除。", timerKey, req.DueTime, req.Period, req.TTL, req.Data)
				return
			}

			if _, exists := a.actorsTable.Load(actorKey); exists {
				// 判断对应类型的actor存不存在
				if err = a.executeTimer(req.ActorType, req.ActorID, req.Name, req.DueTime, req.Period, req.Callback, req.Data); err != nil {
					log.Errorf("在actor:%s上调用定时器出错：%s", actorKey, err)
				}
				if repeats > 0 {
					repeats--
				}
			} else {
				log.Errorf("不能找到活跃的定时器 %s", timerKey)
				return
			}
			if repeats == 0 || period == 0 {
				log.Infof("定时器 %s 已完成", timerKey)
				break L
			}
			nextTime = nextTime.Add(period)
			if nextTimer.Stop() {
				<-nextTimer.C
			}
			nextTimer.Reset(time.Until(nextTime))
		}
		err = a.DeleteTimer(ctx, &DeleteTimerRequest{
			Name:      req.Name,
			ActorID:   req.ActorID,
			ActorType: req.ActorType,
		})
		if err != nil {
			log.Errorf("删除定时器出错 %s: %v", timerKey, err)
		}
	}(stop, req)
	return nil
}

// ok
func (a *actorsRuntime) executeTimer(actorType, actorID, name, dueTime, period, callback string, data interface{}) error {
	t := TimerResponse{
		Callback: callback,
		Data:     data,
		DueTime:  dueTime,
		Period:   period,
	}
	b, err := json.Marshal(&t)
	if err != nil {
		return err
	}

	log.Debugf("执行计时器:%s actor类型:%s ID:%s  ", name, actorType, actorID)
	req := invokev1.NewInvokeMethodRequest(fmt.Sprintf("timer/%s", name))
	req.WithActor(actorType, actorID)
	req.WithRawData(b, invokev1.JSONContentType)
	_, err = a.Call(context.Background(), req)
	if err != nil {
		log.Errorf("执行计时器:%s actor类型:%s ID:%s 出错", name, actorType, actorID, err)
	}
	return err
}

// 保存actor type 的元数据
func (a *actorsRuntime) saveActorTypeMetadata(actorType string, actorMetadata *ActorMetadata) error {
	if !a.actorTypeMetadataEnabled {
		return nil
	}

	metadataKey := constructCompositeKey("actors", actorType, "metadata")
	return a.store.Set(&state.SetRequest{
		Key:   metadataKey,
		Value: actorMetadata,
		ETag:  actorMetadata.Etag,
	})
}

// 获取actor type的元数据
func (a *actorsRuntime) getActorTypeMetadata(actorType string, migrate bool) (*ActorMetadata, error) {
	if a.store == nil {
		return nil, errors.New("actors: 状态存储不存在或配置不正确")
	}

	if !a.actorTypeMetadataEnabled {
		return &ActorMetadata{
			ID: uuid.NewString(),
			RemindersMetadata: ActorRemindersMetadata{
				partitionsEtag: nil,
				PartitionCount: 0,
			},
			Etag: nil,
		}, nil
	}

	metadataKey := constructCompositeKey("actors", actorType, "metadata")
	resp, err := a.store.Get(&state.GetRequest{
		Key: metadataKey,
	})
	if err != nil || len(resp.Data) == 0 {
		// 元数据字段不存在或读取失败。我们回退到默认的 "零 "分区行为。
		actorMetadata := ActorMetadata{
			ID: uuid.NewString(),
			RemindersMetadata: ActorRemindersMetadata{
				partitionsEtag: nil,
				PartitionCount: 0,
			},
			Etag: nil,
		}

		// 保存元数据字段，以确保错误是由于没有找到记录。如果之前的错误是由于数据库造成的，那么这次写入将失败，原因是。
		//   1. 数据库仍然没有反应，或者
		//   2. etag不匹配，因为该项目已经存在。
		// 之所以需要这种写操作，也是因为我们要避免出现另一个挎包试图做同样事情的竞赛条件。
		etag := ""
		if resp != nil && resp.ETag != nil {
			etag = *resp.ETag
		}

		actorMetadata.Etag = &etag
		err = a.saveActorTypeMetadata(actorType, &actorMetadata)
		if err != nil {
			return nil, err
		}

		// 需要去读并获取etag
		resp, err = a.store.Get(&state.GetRequest{
			Key: metadataKey,
		})
		if err != nil {
			return nil, err
		}
	}

	var actorMetadata ActorMetadata
	err = json.Unmarshal(resp.Data, &actorMetadata)
	if err != nil {
		return nil, fmt.Errorf("不能解析actor type 元信息 %s (%s): %w", actorType, string(resp.Data), err)
	}
	actorMetadata.Etag = resp.ETag // 版本号
	if !migrate {
		return &actorMetadata, nil
	}

	return a.migrateRemindersForActorType(actorType, &actorMetadata)
}

// 将元信息 合并到actor type中
func (a *actorsRuntime) migrateRemindersForActorType(actorType string, actorMetadata *ActorMetadata) (*ActorMetadata, error) {
	if !a.actorTypeMetadataEnabled {
		return actorMetadata, nil
	}

	// 如果元信息的分区数、与全局配置的分区数一致
	if actorMetadata.RemindersMetadata.PartitionCount == a.config.RemindersStoragePartitions {
		return actorMetadata, nil
	}

	if actorMetadata.RemindersMetadata.PartitionCount > a.config.RemindersStoragePartitions {
		log.Warnf("不能减少reminder分区数量%s", actorType)
		return actorMetadata, nil
	}

	log.Warnf("迁移reminder元数据记录 %s", actorType)

	// 取出所有reminder
	reminderRefs, refreshedActorMetadata, err := a.getRemindersForActorType(actorType, false)
	if err != nil {
		return nil, err
	}
	if refreshedActorMetadata.ID != actorMetadata.ID {
		return nil, errors.Errorf("could not migrate reminders for actor type %s due to race condition in actor metadata", actorType)
	}

	// Recreate as a new metadata identifier.
	actorMetadata.ID = uuid.NewString()
	actorMetadata.RemindersMetadata.PartitionCount = a.config.RemindersStoragePartitions
	actorRemindersPartitions := make([][]*Reminder, actorMetadata.RemindersMetadata.PartitionCount)
	for i := 0; i < actorMetadata.RemindersMetadata.PartitionCount; i++ {
		actorRemindersPartitions[i] = make([]*Reminder, 0)
	}

	// Recalculate partition for each reminder.
	for _, reminderRef := range reminderRefs {
		partitionID := actorMetadata.calculateReminderPartition(reminderRef.reminder.ActorID, reminderRef.reminder.Name)
		actorRemindersPartitions[partitionID-1] = append(actorRemindersPartitions[partitionID-1], reminderRef.reminder)
	}

	// Save to database.
	metadata := map[string]string{metadataPartitionKey: actorMetadata.ID}
	transaction := state.TransactionalStateRequest{
		Metadata: metadata,
	}
	for i := 0; i < actorMetadata.RemindersMetadata.PartitionCount; i++ {
		partitionID := i + 1
		stateKey := actorMetadata.calculateRemindersStateKey(actorType, uint32(partitionID))
		stateValue := actorRemindersPartitions[i]
		transaction.Operations = append(transaction.Operations, state.TransactionalStateOperation{
			Operation: state.Upsert,
			Request: state.SetRequest{
				Key:      stateKey,
				Value:    stateValue,
				Metadata: metadata,
			},
		})
	}
	err = a.transactionalStore.Multi(&transaction)
	if err != nil {
		return nil, err
	}

	// Save new metadata so the new "metadataID" becomes the new de factor referenced list for reminders.
	err = a.saveActorTypeMetadata(actorType, actorMetadata)
	if err != nil {
		return nil, err
	}
	log.Warnf(
		"completed actor metadata record migration for actor type %s, new metadata ID = %s",
		actorType, actorMetadata.ID)
	return actorMetadata, nil
}

// 根据actor type 获取reminders
func (a *actorsRuntime) getRemindersForActorType(actorType string, migrate bool) ([]actorReminderReference, *ActorMetadata, error) {
	if a.store == nil {
		return nil, nil, errors.New("actors: 状态存储不存在或配置不正确")
	}

	actorMetadata, merr := a.getActorTypeMetadata(actorType, migrate)
	if merr != nil {
		return nil, nil, fmt.Errorf("不能读取actor type元数据: %w", merr)
	}

	if actorMetadata.RemindersMetadata.PartitionCount >= 1 {
		metadata := map[string]string{metadataPartitionKey: actorMetadata.ID}
		actorMetadata.RemindersMetadata.partitionsEtag = map[uint32]*string{}
		var reminders []actorReminderReference

		keyPartitionMap := map[string]uint32{}
		var getRequests []state.GetRequest
		for i := 1; i <= actorMetadata.RemindersMetadata.PartitionCount; i++ {
			partition := uint32(i)
			key := actorMetadata.calculateRemindersStateKey(actorType, partition)
			keyPartitionMap[key] = partition
			getRequests = append(getRequests, state.GetRequest{
				Key:      key,
				Metadata: metadata,
			})
		}

		bulkGet, bulkResponse, err := a.store.BulkGet(getRequests)
		if bulkGet {
			if err != nil {
				return nil, nil, err
			}
		} else {
			// TODO(artursouza): refactor this fallback into default implementation in contrib.
			// if store doesn't support bulk get, fallback to call get() method one by one
			limiter := concurrency.NewLimiter(actorMetadata.RemindersMetadata.PartitionCount)
			bulkResponse = make([]state.BulkGetResponse, len(getRequests))
			for i := range getRequests {
				getRequest := getRequests[i]
				bulkResponse[i].Key = getRequest.Key

				fn := func(param interface{}) {
					r := param.(*state.BulkGetResponse)
					resp, ferr := a.store.Get(&getRequest)
					if ferr != nil {
						r.Error = ferr.Error()
					} else if resp != nil {
						r.Data = jsoniter.RawMessage(resp.Data)
						r.ETag = resp.ETag
						r.Metadata = resp.Metadata
					}
				}

				limiter.Execute(fn, &bulkResponse[i])
			}
			limiter.Wait()
		}

		for _, resp := range bulkResponse {
			partition := keyPartitionMap[resp.Key]
			actorMetadata.RemindersMetadata.partitionsEtag[partition] = resp.ETag
			if resp.Error != "" {
				return nil, nil, fmt.Errorf("could not get reminders partition %v: %v", resp.Key, resp.Error)
			}

			var batch []Reminder
			if len(resp.Data) > 0 {
				err = json.Unmarshal(resp.Data, &batch)
				if err != nil {
					return nil, nil, fmt.Errorf("could not parse actor reminders partition %v: %w", resp.Key, err)
				}
			}

			for j := range batch {
				reminders = append(reminders, actorReminderReference{
					actorMetadataID:           actorMetadata.ID,
					actorRemindersPartitionID: partition,
					reminder:                  &batch[j],
				})
			}
		}

		return reminders, actorMetadata, nil
	}

	key := constructCompositeKey("actors", actorType)
	resp, err := a.store.Get(&state.GetRequest{
		Key: key,
	})
	if err != nil {
		return nil, nil, err
	}

	var reminders []Reminder
	if len(resp.Data) > 0 {
		err = json.Unmarshal(resp.Data, &reminders)
		if err != nil {
			return nil, nil, fmt.Errorf("could not parse actor reminders: %v", err)
		}
	}

	reminderRefs := make([]actorReminderReference, len(reminders))
	for j := range reminders {
		reminderRefs[j] = actorReminderReference{
			actorMetadataID:           "",
			actorRemindersPartitionID: 0,
			reminder:                  &reminders[j],
		}
	}

	actorMetadata.RemindersMetadata.partitionsEtag = map[uint32]*string{
		0: resp.ETag,
	}
	return reminderRefs, actorMetadata, nil
}

func (a *actorsRuntime) saveRemindersInPartition(ctx context.Context, stateKey string, reminders []Reminder, etag *string, databasePartitionKey string) error {
	// Even when data is not partitioned, the save operation is the same.
	// The only difference is stateKey.
	return a.store.Set(&state.SetRequest{
		Key:      stateKey,
		Value:    reminders,
		ETag:     etag,
		Metadata: map[string]string{metadataPartitionKey: databasePartitionKey},
	})
}

// DeleteReminder OK
func (a *actorsRuntime) DeleteReminder(ctx context.Context, req *DeleteReminderRequest) error {
	if a.store == nil {
		return errors.New("actors: 状态存储不存在或配置不正确")
	}

	if a.evaluationBusy {
		select {
		case <-time.After(time.Second * 5):
			return errors.New("删除reminder 失败:5秒超时")
		case <-a.evaluationChan:
			break
		}
	}

	actorKey := constructCompositeKey(req.ActorType, req.ActorID)
	reminderKey := constructCompositeKey(actorKey, req.Name)

	stop, exists := a.activeReminders.Load(reminderKey)
	if exists {
		log.Infof("发现reminder: %v. 删除reminder", reminderKey)
		close(stop.(chan bool))
		a.activeReminders.Delete(reminderKey)
	}

	err := backoff.Retry(func() error {
		reminders, actorMetadata, err := a.getRemindersForActorType(req.ActorType, true)
		if err != nil {
			return err
		}

		// remove from partition first.
		remindersInPartition, stateKey, etag := actorMetadata.removeReminderFromPartition(reminders, req.ActorType, req.ActorID, req.Name)

		// now, we can remove from the "global" list.
		for i := len(reminders) - 1; i >= 0; i-- {
			if reminders[i].reminder.ActorType == req.ActorType && reminders[i].reminder.ActorID == req.ActorID && reminders[i].reminder.Name == req.Name {
				reminders = append(reminders[:i], reminders[i+1:]...)
			}
		}

		// Get the database partiton key (needed for CosmosDB)
		databasePartitionKey := actorMetadata.calculateDatabasePartitionKey(stateKey)

		// Then, save the partition to the database.
		err = a.saveRemindersInPartition(ctx, stateKey, remindersInPartition, etag, databasePartitionKey)
		if err != nil {
			return err
		}

		// Finally, we must save metadata to get a new eTag.
		// This avoids a race condition between an update and a repartitioning.
		a.saveActorTypeMetadata(req.ActorType, actorMetadata)

		a.remindersLock.Lock()
		a.reminders[req.ActorType] = reminders
		a.remindersLock.Unlock()
		return nil
	}, backoff.NewExponentialBackOff())
	if err != nil {
		return err
	}

	err = a.store.Delete(&state.DeleteRequest{
		Key: reminderKey,
	})
	if err != nil {
		return err
	}

	return nil
}

func (a *actorsRuntime) GetReminder(ctx context.Context, req *GetReminderRequest) (*Reminder, error) {
	reminders, _, err := a.getRemindersForActorType(req.ActorType, true)
	if err != nil {
		return nil, err
	}

	for _, r := range reminders {
		if r.reminder.ActorID == req.ActorID && r.reminder.Name == req.Name {
			return &Reminder{
				Data:    r.reminder.Data,
				DueTime: r.reminder.DueTime,
				Period:  r.reminder.Period,
			}, nil
		}
	}
	return nil, nil
}

// DeleteTimer ok
func (a *actorsRuntime) DeleteTimer(ctx context.Context, req *DeleteTimerRequest) error {
	actorKey := constructCompositeKey(req.ActorType, req.ActorID)
	timerKey := constructCompositeKey(actorKey, req.Name)

	stopChan, exists := a.activeTimers.Load(timerKey)
	if exists {
		close(stopChan.(chan bool))
		a.activeTimers.Delete(timerKey)
	}

	return nil
}

func (a *actorsRuntime) GetActiveActorsCount(ctx context.Context) []ActiveActorsCount {
	actorCountMap := map[string]int{}
	for _, actorType := range a.config.HostedActorTypes {
		actorCountMap[actorType] = 0
	}
	a.actorsTable.Range(func(key, value interface{}) bool {
		actorType, _ := a.getActorTypeAndIDFromKey(key.(string))
		actorCountMap[actorType]++
		return true
	})

	activeActorsCount := make([]ActiveActorsCount, 0, len(actorCountMap))
	for actorType, count := range actorCountMap {
		activeActorsCount = append(activeActorsCount, ActiveActorsCount{Type: actorType, Count: count})
	}

	return activeActorsCount
}

// Stop closes all network connections and resources used in actor runtime.
func (a *actorsRuntime) Stop() {
	if a.placement != nil {
		a.placement.Stop()
	}
}

// ValidateHostEnvironment 验证在给定的一组参数和运行时的操作模式下，actor 可以被正确初始化。
func ValidateHostEnvironment(mTLSEnabled bool, mode modes.DaprMode, namespace string) error {
	switch mode {
	case modes.KubernetesMode:
		if mTLSEnabled && namespace == "" {
			return errors.New("actors 在Kubernetes模式下运行时，必须有一个配置好的命名空间")
		}
	}
	return nil
}

func parseISO8601Duration(from string) (time.Duration, int, error) {
	match := pattern.FindStringSubmatch(from)
	if match == nil {
		return 0, 0, errors.Errorf("unsupported ISO8601 duration format %q", from)
	}
	duration := time.Duration(0)
	// -1 signifies infinite repetition
	repetition := -1
	for i, name := range pattern.SubexpNames() {
		part := match[i]
		if i == 0 || name == "" || part == "" {
			continue
		}
		val, err := strconv.Atoi(part)
		if err != nil {
			return 0, 0, err
		}
		switch name {
		case "year":
			duration += time.Hour * 24 * 365 * time.Duration(val)
		case "month":
			duration += time.Hour * 24 * 30 * time.Duration(val)
		case "week":
			duration += time.Hour * 24 * 7 * time.Duration(val)
		case "day":
			duration += time.Hour * 24 * time.Duration(val)
		case "hour":
			duration += time.Hour * time.Duration(val)
		case "minute":
			duration += time.Minute * time.Duration(val)
		case "second":
			duration += time.Second * time.Duration(val)
		case "repetition":
			repetition = val
		default:
			return 0, 0, fmt.Errorf("unsupported ISO8601 duration field %s", name)
		}
	}
	return duration, repetition, nil
}

// parseDuration creates time.Duration from either:
// - ISO8601 duration format,
// - time.Duration string format.
func parseDuration(from string) (time.Duration, int, error) {
	d, r, err := parseISO8601Duration(from)
	if err == nil {
		return d, r, nil
	}
	d, err = time.ParseDuration(from)
	if err == nil {
		return d, -1, nil
	}
	return 0, 0, errors.Errorf("unsupported duration format %q", from)
}

// parseTime creates time.Duration from either:
// - ISO8601 duration format,
// - time.Duration string format,
// - RFC3339 datetime format.
// For duration formats, an offset is added.
func parseTime(from string, offset *time.Time) (time.Time, error) {
	var start time.Time
	if offset != nil {
		start = *offset
	} else {
		start = time.Now()
	}
	d, r, err := parseISO8601Duration(from)
	if err == nil {
		if r != -1 {
			return time.Time{}, errors.Errorf("repetitions are not allowed")
		}
		return start.Add(d), nil
	}
	if d, err = time.ParseDuration(from); err == nil {
		return start.Add(d), nil
	}
	if t, err := time.Parse(time.RFC3339, from); err == nil {
		return t, nil
	}
	return time.Time{}, errors.Errorf("unsupported time/duration format %q", from)
}

func GetParseTime(from string, offset *time.Time) (time.Time, error) {
	return parseTime(from, offset)
}
