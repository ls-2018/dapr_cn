// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package injector

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	v1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"

	scheme "github.com/dapr/dapr/pkg/client/clientset/versioned"
	"github.com/dapr/dapr/pkg/credentials"
	auth "github.com/dapr/dapr/pkg/runtime/security"
	"github.com/dapr/dapr/pkg/sentry/certs"
	"github.com/dapr/dapr/pkg/validation" // ok
	"github.com/dapr/dapr/utils"
)

const (
	sidecarContainerName              = "daprd"
	daprEnabledKey                    = "dapr.io/enabled"
	daprAppPortKey                    = "dapr.io/app-port"
	daprConfigKey                     = "dapr.io/config"
	daprAppProtocolKey                = "dapr.io/app-protocol"
	appIDKey                          = "dapr.io/app-id"
	daprEnableProfilingKey            = "dapr.io/enable-profiling"
	daprLogLevel                      = "dapr.io/log-level"
	daprAPITokenSecret                = "dapr.io/api-token-secret" /* #nosec */
	daprAppTokenSecret                = "dapr.io/app-token-secret" /* #nosec */
	daprLogAsJSON                     = "dapr.io/log-as-json"
	daprAppMaxConcurrencyKey          = "dapr.io/app-max-concurrency"
	daprEnableMetricsKey              = "dapr.io/enable-metrics"
	daprMetricsPortKey                = "dapr.io/metrics-port"
	daprEnableDebugKey                = "dapr.io/enable-debug"
	daprDebugPortKey                  = "dapr.io/debug-port"
	daprEnvKey                        = "dapr.io/env"
	daprCPULimitKey                   = "dapr.io/sidecar-cpu-limit"
	daprMemoryLimitKey                = "dapr.io/sidecar-memory-limit"
	daprCPURequestKey                 = "dapr.io/sidecar-cpu-request"
	daprMemoryRequestKey              = "dapr.io/sidecar-memory-request"
	daprListenAddresses               = "dapr.io/sidecar-listen-addresses"
	daprLivenessProbeDelayKey         = "dapr.io/sidecar-liveness-probe-delay-seconds"
	daprLivenessProbeTimeoutKey       = "dapr.io/sidecar-liveness-probe-timeout-seconds"
	daprLivenessProbePeriodKey        = "dapr.io/sidecar-liveness-probe-period-seconds"
	daprLivenessProbeThresholdKey     = "dapr.io/sidecar-liveness-probe-threshold"
	daprReadinessProbeDelayKey        = "dapr.io/sidecar-readiness-probe-delay-seconds"
	daprReadinessProbeTimeoutKey      = "dapr.io/sidecar-readiness-probe-timeout-seconds"
	daprReadinessProbePeriodKey       = "dapr.io/sidecar-readiness-probe-period-seconds"
	daprReadinessProbeThresholdKey    = "dapr.io/sidecar-readiness-probe-threshold"
	daprImage                         = "dapr.io/sidecar-image"
	daprAppSSLKey                     = "dapr.io/app-ssl"
	daprMaxRequestBodySize            = "dapr.io/http-max-request-size"
	daprReadBufferSize                = "dapr.io/http-read-buffer-size"
	daprHTTPStreamRequestBody         = "dapr.io/http-stream-request-body"
	containersPath                    = "/spec/containers"
	sidecarHTTPPort                   = 3500
	sidecarAPIGRPCPort                = 50001
	sidecarInternalGRPCPort           = 50002
	sidecarPublicPort                 = 3501
	userContainerDaprHTTPPortName     = "DAPR_HTTP_PORT"
	userContainerDaprGRPCPortName     = "DAPR_GRPC_PORT"
	apiAddress                        = "dapr-api"
	placementService                  = "dapr-placement-server"
	sentryService                     = "dapr-sentry"
	apiPort                           = 80
	placementServicePort              = 50005
	sentryServicePort                 = 80
	sidecarHTTPPortName               = "dapr-http"
	sidecarGRPCPortName               = "dapr-grpc"
	sidecarInternalGRPCPortName       = "dapr-internal"
	sidecarMetricsPortName            = "dapr-metrics"
	sidecarDebugPortName              = "dapr-debug"
	defaultLogLevel                   = "info"
	defaultLogAsJSON                  = false
	defaultAppSSL                     = false
	kubernetesMountPath               = "/var/run/secrets/kubernetes.io/serviceaccount"
	defaultConfig                     = "daprsystem"
	defaultEnabledMetric              = true
	defaultMetricsPort                = 9090
	defaultSidecarDebug               = false
	defaultSidecarDebugPort           = 40000
	defaultSidecarListenAddresses     = "[::1],127.0.0.1"
	sidecarHealthzPath                = "healthz"
	defaultHealthzProbeDelaySeconds   = 3
	defaultHealthzProbeTimeoutSeconds = 3
	defaultHealthzProbePeriodSeconds  = 6
	defaultHealthzProbeThreshold      = 3
	apiVersionV1                      = "v1.0"
	defaultMtlsEnabled                = true
	trueString                        = "true"
	defaultDaprHTTPStreamRequestBody  = false
)

// ??????pod???????????????
func (i *injector) getPodPatchOperations(ar *v1.AdmissionReview,
	namespace, image, imagePullPolicy string, kubeClient kubernetes.Interface, daprClient scheme.Interface) ([]PatchOperation, error) {
	req := ar.Request
	var pod corev1.Pod
	if err := json.Unmarshal(req.Object.Raw, &pod); err != nil {
		errors.Wrap(err, "could not unmarshal raw object")
		return nil, err
	}

	log.Infof(
		"AdmissionReview for Kind=%v, Namespace=%v Name=%v (%v) UID=%v "+
			"patchOperation=%v UserInfo=%v",
		req.Kind,
		req.Namespace,
		req.Name,
		pod.Name,
		req.UID,
		req.Operation,
		req.UserInfo,
	)
	// ????????????dapr || pod????????????daprd ?????????????????????
	if !isResourceDaprEnabled(pod.Annotations) || podContainsSidecarContainer(&pod) {
		return nil, nil
	}
	// ?????? pod ????????????????????????
	id := getAppID(pod)
	// ?????? app id ????????????k8s?????????????????????
	err := validation.ValidateKubernetesAppID(id)
	if err != nil {
		return nil, err
	}
	//ENV
	//KUBE_CLUSTER_DOMAIN: cluster.local
	//NAMESPACE: fieldRef(v1:metadata.namespace)
	//SIDECAR_IMAGE: docker.io/daprio/daprd:1.3.0
	//SIDECAR_IMAGE_PULL_POLICY: IfNotPresent
	//TLS_CERT_FILE: /dapr/cert/tls.crt
	//TLS_KEY_FILE: /dapr/cert/tls.key
	// ???DNS???????????????getSidecarContainer????????????????????????????????????
	// "dapr-placement-server.dapr.svc.cluster.local:50005"
	placementAddress := getServiceAddress(placementService, namespace, i.config.KubeClusterDomain, placementServicePort)
	// "dapr-sentry.dapr.svc.cluster.local:80"
	sentryAddress := getServiceAddress(sentryService, namespace, i.config.KubeClusterDomain, sentryServicePort)
	//"dapr-api.dapr.svc.cluster.local:80"    ??????operator
	apiSvcAddress := getServiceAddress(apiAddress, namespace, i.config.KubeClusterDomain, apiPort)

	var trustAnchors string
	var certChain string
	var certKey string
	var identity string

	// ?????????????????????mtls,
	mtlsEnabled := mTLSEnabled(daprClient)
	//?????????????????????????????????
	trustAnchors, certChain, certKey = getTrustAnchorsAndCertChain(kubeClient, namespace)
	identity = fmt.Sprintf("%s:%s", req.Namespace, pod.Spec.ServiceAccountName)

	// ???????????????????????? namespace ,ca.crt , token ????????????
	tokenMount := getTokenVolumeMount(pod)
	// ??????daprd?????????????????????????????????????????????patchOps
	sidecarContainer, err := getSidecarContainer(pod.Annotations, id, image, imagePullPolicy, req.Namespace, apiSvcAddress, placementAddress, tokenMount, trustAnchors, certChain, certKey, sentryAddress, mtlsEnabled, identity)
	if err != nil {
		return nil, err
	}

	patchOps := []PatchOperation{}
	envPatchOps := []PatchOperation{}
	var path string
	var value interface{}
	if len(pod.Spec.Containers) == 0 {
		// ????????????????????????????????????
		path = containersPath
		value = []corev1.Container{*sidecarContainer}
	} else {
		envPatchOps = addDaprEnvVarsToContainers(pod.Spec.Containers)
		path = "/spec/containers/-"
		value = sidecarContainer
	}

	patchOps = append(
		patchOps,
		PatchOperation{
			Op:    "add",
			Path:  path,
			Value: value,
		},
	)
	patchOps = append(patchOps, envPatchOps...)

	return patchOps, nil
}

// ???DAPR_HTTP_PORT???DAPR_GRPC_PORT ?????????????????? ??????????????????daprd??????????????????????????????
func addDaprEnvVarsToContainers(containers []corev1.Container) []PatchOperation {
	portEnv := []corev1.EnvVar{
		{
			Name:  userContainerDaprHTTPPortName,
			Value: strconv.Itoa(sidecarHTTPPort),
		},
		{
			Name:  userContainerDaprGRPCPortName,
			Value: strconv.Itoa(sidecarAPIGRPCPort),
		},
	}
	envPatchOps := make([]PatchOperation, 0, len(containers))
	for i, container := range containers {
		path := fmt.Sprintf("%s/%d/env", containersPath, i) // spec/containers/1/env
		patchOps := getEnvPatchOperations(container.Env, portEnv, path)
		envPatchOps = append(envPatchOps, patchOps...)
	}
	return envPatchOps
}

// ??????????????????????????????????????????
func getEnvPatchOperations(envs []corev1.EnvVar, addEnv []corev1.EnvVar, path string) []PatchOperation {

	if len(envs) == 0 {
		// ??????????????????????????????
		return []PatchOperation{
			{
				Op:    "add",
				Path:  path,   //spec/containers/1/env
				Value: addEnv, // daprd ???????????????
			},
		}
	}
	path += "/-"

	var patchOps []PatchOperation
LoopEnv:
	for _, env := range addEnv {
		for _, actual := range envs {
			if actual.Name == env.Name {
				// Add only env vars that do not conflict with existing user defined/injected env vars.
				continue LoopEnv
			}
		}
		patchOps = append(patchOps, PatchOperation{
			Op:    "add",
			Path:  path,
			Value: env,
		})
	}
	return patchOps
}

// ??????dapr ??????????????????dapr-trust-bundle secret ??????
func getTrustAnchorsAndCertChain(kubeClient kubernetes.Interface, namespace string) (string, string, string) {
	secret, err := kubeClient.CoreV1().Secrets(namespace).Get(context.TODO(), certs.KubeScrtName, meta_v1.GetOptions{})
	if err != nil {
		return "", "", ""
	}
	rootCert := secret.Data[credentials.RootCertFilename]
	certChain := secret.Data[credentials.IssuerCertFilename]
	certKey := secret.Data[credentials.IssuerKeyFilename]
	return string(rootCert), string(certChain), string(certKey)
}
func GetTrustAnchorsAndCertChain(kubeClient kubernetes.Interface, namespace string) (string, string, string) {
	return getTrustAnchorsAndCertChain(kubeClient, namespace)
}

func mTLSEnabled(daprClient scheme.Interface) bool {
	resp, err := daprClient.ConfigurationV1alpha1().Configurations(meta_v1.NamespaceAll).List(meta_v1.ListOptions{})
	if err != nil {
		log.Errorf("Failed to load dapr configuration from k8s, use default value %t for mTLSEnabled: %s", defaultMtlsEnabled, err)
		return defaultMtlsEnabled
	}
	//?????????????????????????????? dapr.io Configurations
	for _, c := range resp.Items {
		if c.GetName() == defaultConfig { // ?????????dapr-system ???????????????????????????daprsystem ???Configurations
			return c.Spec.MTLSSpec.Enabled
		}
	}
	log.Infof("Dapr system configuration (%s) is not found, use default value %t for mTLSEnabled", defaultConfig, defaultMtlsEnabled)
	return defaultMtlsEnabled
}
func MTLSEnabled(daprClient scheme.Interface) bool {
	_ = `{
  "metadata": {
    "resourceVersion": "19996757"
  },
  "items": [
    {
      "kind": "Configuration",
      "apiVersion": "dapr.io/v1alpha1",
      "metadata": {
        "name": "daprsystem",
        "namespace": "dapr-system",
        "uid": "5484cda3-ada8-44f3-af4d-c8e9868280e4",
        "resourceVersion": "13639",
        "generation": 1,
        "creationTimestamp": "2021-08-20T10:06:35Z",
        "labels": {
          "app.kubernetes.io/managed-by": "Helm"
        },
        "annotations": {
          "meta.helm.sh/release-name": "dapr",
          "meta.helm.sh/release-namespace": "dapr-system"
        },
        "managedFields": [
          {
            "manager": "Go-http-client",
            "operation": "Update",
            "apiVersion": "dapr.io/v1alpha1",
            "time": "2021-08-20T10:06:35Z",
            "fieldsType": "FieldsV1",
            "fieldsV1": {
              "f:metadata": {
                "f:annotations": {
                  ".": {},
                  "f:meta.helm.sh/release-name": {},
                  "f:meta.helm.sh/release-namespace": {}
                },
                "f:labels": {
                  ".": {},
                  "f:app.kubernetes.io/managed-by": {}
                }
              },
              "f:spec": {
                ".": {},
                "f:metric": {
                  ".": {},
                  "f:enabled": {}
                },
                "f:mtls": {
                  ".": {},
                  "f:allowedClockSkew": {},
                  "f:enabled": {},
                  "f:workloadCertTTL": {}
                }
              }
            }
          }
        ]
      },
      "spec": {
        "httpPipeline": {
          "handlers": null
        },
        "tracing": {
          "samplingRate": "",
          "zipkin": {
            "endpointAddress": ""
          }
        },
        "metric": {
          "enabled": true
        },
        "mtls": {
          "enabled": true,
          "workloadCertTTL": "24h",
          "allowedClockSkew": "15m"
        },
        "secrets": {
          "scopes": null
        },
        "accessControl": {
          "defaultAction": "",
          "trustDomain": "",
          "policies": null
        },
        "nameResolution": {
          "component": "",
          "version": "",
          "configuration": null
        },
        "api": {}
      }
    },
    {
      "kind": "Configuration",
      "apiVersion": "dapr.io/v1alpha1",
      "metadata": {
        "name": "appconfig",
        "namespace": "liushuo",
        "uid": "d3274f39-a4e2-480b-88b5-17890f0ee1f9",
        "resourceVersion": "5496521",
        "generation": 1,
        "creationTimestamp": "2021-09-14T08:30:04Z",
        "annotations": {
          "kubectl.kubernetes.io/last-applied-configuration": "{\"apiVersion\":\"dapr.io/v1alpha1\",\"kind\":\"Configuration\",\"metadata\":{\"annotations\":{},\"name\":\"appconfig\",\"namespace\":\"liushuo\"},\"spec\":{\"tracing\":{\"samplingRate\":\"1\",\"zipkin\":{\"endpointAddress\":\"http://zipkin.liushuo.svc.cluster.local:9411/api/v2/spans\"}}}}\n"
        },
        "managedFields": [
          {
            "manager": "kubectl-client-side-apply",
            "operation": "Update",
            "apiVersion": "dapr.io/v1alpha1",
            "time": "2021-09-14T08:30:04Z",
            "fieldsType": "FieldsV1",
            "fieldsV1": {
              "f:metadata": {
                "f:annotations": {
                  ".": {},
                  "f:kubectl.kubernetes.io/last-applied-configuration": {}
                }
              },
              "f:spec": {
                ".": {},
                "f:metric": {
                  ".": {},
                  "f:enabled": {}
                },
                "f:tracing": {
                  ".": {},
                  "f:samplingRate": {},
                  "f:zipkin": {
                    ".": {},
                    "f:endpointAddress": {}
                  }
                }
              }
            }
          }
        ]
      },
      "spec": {
        "httpPipeline": {
          "handlers": null
        },
        "tracing": {
          "samplingRate": "1",
          "zipkin": {
            "endpointAddress": "http://zipkin.liushuo.svc.cluster.local:9411/api/v2/spans"
          }
        },
        "metric": {
          "enabled": true
        },
        "mtls": {
          "enabled": false,
          "workloadCertTTL": "",
          "allowedClockSkew": ""
        },
        "secrets": {
          "scopes": null
        },
        "accessControl": {
          "defaultAction": "",
          "trustDomain": "",
          "policies": null
        },
        "nameResolution": {
          "component": "",
          "version": "",
          "configuration": null
        },
        "api": {}
      }
    },
    {
      "kind": "Configuration",
      "apiVersion": "dapr.io/v1alpha1",
      "metadata": {
        "name": "appconfig",
        "namespace": "mesoid",
        "uid": "5b5a487b-68a3-4240-8ee5-54f9a4ee8c75",
        "resourceVersion": "2101768",
        "generation": 1,
        "creationTimestamp": "2021-08-30T10:17:31Z",
        "annotations": {
          "kubectl.kubernetes.io/last-applied-configuration": "{\"apiVersion\":\"dapr.io/v1alpha1\",\"kind\":\"Configuration\",\"metadata\":{\"annotations\":{},\"name\":\"appconfig\",\"namespace\":\"mesoid\"},\"spec\":{\"tracing\":{\"samplingRate\":\"1\",\"zipkin\":{\"endpointAddress\":\"http://zipkin.mesoid.svc.cluster.local:9411/api/v2/spans\"}}}}\n"
        },
        "managedFields": [
          {
            "manager": "kubectl-client-side-apply",
            "operation": "Update",
            "apiVersion": "dapr.io/v1alpha1",
            "time": "2021-08-30T10:17:31Z",
            "fieldsType": "FieldsV1",
            "fieldsV1": {
              "f:metadata": {
                "f:annotations": {
                  ".": {},
                  "f:kubectl.kubernetes.io/last-applied-configuration": {}
                }
              },
              "f:spec": {
                ".": {},
                "f:metric": {
                  ".": {},
                  "f:enabled": {}
                },
                "f:tracing": {
                  ".": {},
                  "f:samplingRate": {},
                  "f:zipkin": {
                    ".": {},
                    "f:endpointAddress": {}
                  }
                }
              }
            }
          }
        ]
      },
      "spec": {
        "httpPipeline": {
          "handlers": null
        },
        "tracing": {
          "samplingRate": "1",
          "zipkin": {
            "endpointAddress": "http://zipkin.mesoid.svc.cluster.local:9411/api/v2/spans"
          }
        },
        "metric": {
          "enabled": true
        },
        "mtls": {
          "enabled": false,
          "workloadCertTTL": "",
          "allowedClockSkew": ""
        },
        "secrets": {
          "scopes": null
        },
        "accessControl": {
          "defaultAction": "",
          "trustDomain": "",
          "policies": null
        },
        "nameResolution": {
          "component": "",
          "version": "",
          "configuration": null
        },
        "api": {}
      }
    }
  ]
}
`
	return mTLSEnabled(daprClient)

}

func getTokenVolumeMount(pod corev1.Pod) *corev1.VolumeMount {
	for _, c := range pod.Spec.Containers {
		for _, v := range c.VolumeMounts {
			if v.MountPath == kubernetesMountPath {
				return &v
			}
		}
	}
	return nil
}

func podContainsSidecarContainer(pod *corev1.Pod) bool {
	for _, c := range pod.Spec.Containers {
		if c.Name == sidecarContainerName {
			return true
		}
	}
	return false
}

func getMaxConcurrency(annotations map[string]string) (int32, error) {
	return getInt32Annotation(annotations, daprAppMaxConcurrencyKey)
}

func getAppPort(annotations map[string]string) (int32, error) {
	return getInt32Annotation(annotations, daprAppPortKey)
}

func getConfig(annotations map[string]string) string {
	return getStringAnnotation(annotations, daprConfigKey)
}

func getProtocol(annotations map[string]string) string {
	return getStringAnnotationOrDefault(annotations, daprAppProtocolKey, "http")
}

func getEnableMetrics(annotations map[string]string) bool {
	return getBoolAnnotationOrDefault(annotations, daprEnableMetricsKey, defaultEnabledMetric)
}

func getMetricsPort(annotations map[string]string) int {
	return int(getInt32AnnotationOrDefault(annotations, daprMetricsPortKey, defaultMetricsPort))
}

func getEnableDebug(annotations map[string]string) bool {
	return getBoolAnnotationOrDefault(annotations, daprEnableDebugKey, defaultSidecarDebug)
}

func getDebugPort(annotations map[string]string) int {
	return int(getInt32AnnotationOrDefault(annotations, daprDebugPortKey, defaultSidecarDebugPort))
}

func getAppID(pod corev1.Pod) string {
	return getStringAnnotationOrDefault(pod.Annotations, appIDKey, pod.GetName())
}

func getLogLevel(annotations map[string]string) string {
	return getStringAnnotationOrDefault(annotations, daprLogLevel, defaultLogLevel)
}

func logAsJSONEnabled(annotations map[string]string) bool {
	return getBoolAnnotationOrDefault(annotations, daprLogAsJSON, defaultLogAsJSON)
}

func profilingEnabled(annotations map[string]string) bool {
	return getBoolAnnotationOrDefault(annotations, daprEnableProfilingKey, false)
}

func appSSLEnabled(annotations map[string]string) bool {
	return getBoolAnnotationOrDefault(annotations, daprAppSSLKey, defaultAppSSL)
}

func getAPITokenSecret(annotations map[string]string) string {
	return getStringAnnotationOrDefault(annotations, daprAPITokenSecret, "")
}

func GetAppTokenSecret(annotations map[string]string) string {
	return getStringAnnotationOrDefault(annotations, daprAppTokenSecret, "")
}

func getMaxRequestBodySize(annotations map[string]string) (int32, error) {
	return getInt32Annotation(annotations, daprMaxRequestBodySize)
}

func getListenAddresses(annotations map[string]string) string {
	return getStringAnnotationOrDefault(annotations, daprListenAddresses, defaultSidecarListenAddresses)
}

func getReadBufferSize(annotations map[string]string) (int32, error) {
	return getInt32Annotation(annotations, daprReadBufferSize)
}

func HTTPStreamRequestBodyEnabled(annotations map[string]string) bool {
	return getBoolAnnotationOrDefault(annotations, daprHTTPStreamRequestBody, defaultDaprHTTPStreamRequestBody)
}

func getBoolAnnotationOrDefault(annotations map[string]string, key string, defaultValue bool) bool {
	enabled, ok := annotations[key]
	if !ok {
		return defaultValue
	}
	s := strings.ToLower(enabled)
	// trueString is used to silence a lint error.
	return (s == "y") || (s == "yes") || (s == trueString) || (s == "on") || (s == "1")
}

func getStringAnnotationOrDefault(annotations map[string]string, key, defaultValue string) string {
	if val, ok := annotations[key]; ok && val != "" {
		return val
	}
	return defaultValue
}

func getStringAnnotation(annotations map[string]string, key string) string {
	return annotations[key]
}

func getInt32AnnotationOrDefault(annotations map[string]string, key string, defaultValue int) int32 {
	s, ok := annotations[key]
	if !ok {
		return int32(defaultValue)
	}
	value, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return int32(defaultValue)
	}
	return int32(value)
}

func getInt32Annotation(annotations map[string]string, key string) (int32, error) {
	s, ok := annotations[key]
	if !ok {
		return -1, nil
	}
	value, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return -1, errors.Wrapf(err, "error parsing %s int value %s ", key, s)
	}
	return int32(value), nil
}

func getProbeHTTPHandler(port int32, pathElements ...string) corev1.Handler {
	return corev1.Handler{
		HTTPGet: &corev1.HTTPGetAction{
			Path: formatProbePath(pathElements...),
			Port: intstr.IntOrString{IntVal: port},
		},
	}
}

// ok
func formatProbePath(elements ...string) string {
	// v1.0 healthz
	pathStr := path.Join(elements...)
	if !strings.HasPrefix(pathStr, "/") {
		pathStr = fmt.Sprintf("/%s", pathStr)
	}
	return pathStr
}

// ok
func appendQuantityToResourceList(quantity string, resourceName corev1.ResourceName, resourceList corev1.ResourceList) (*corev1.ResourceList, error) {
	q, err := resource.ParseQuantity(quantity) // ?????????K8S?????????????????????
	if err != nil {
		return nil, err
	}
	resourceList[resourceName] = q
	//???Limits ??????CPU?????? ResourceCPU
	return &resourceList, nil
}

// ok
func getResourceRequirements(annotations map[string]string) (*corev1.ResourceRequirements, error) {
	r := corev1.ResourceRequirements{
		Limits:   corev1.ResourceList{},
		Requests: corev1.ResourceList{},
	}
	cpuLimit, ok := annotations[daprCPULimitKey]
	if ok {
		list, err := appendQuantityToResourceList(cpuLimit, corev1.ResourceCPU, r.Limits)
		if err != nil {
			return nil, errors.Wrap(err, "error parsing sidecar cpu limit")
		}
		r.Limits = *list
	}
	memLimit, ok := annotations[daprMemoryLimitKey]
	if ok {
		list, err := appendQuantityToResourceList(memLimit, corev1.ResourceMemory, r.Limits)
		if err != nil {
			return nil, errors.Wrap(err, "error parsing sidecar memory limit")
		}
		r.Limits = *list
	}
	cpuRequest, ok := annotations[daprCPURequestKey]
	if ok {
		list, err := appendQuantityToResourceList(cpuRequest, corev1.ResourceCPU, r.Requests)
		if err != nil {
			return nil, errors.Wrap(err, "error parsing sidecar cpu request")
		}
		r.Requests = *list
	}
	memRequest, ok := annotations[daprMemoryRequestKey]
	if ok {
		list, err := appendQuantityToResourceList(memRequest, corev1.ResourceMemory, r.Requests)
		if err != nil {
			return nil, errors.Wrap(err, "error parsing sidecar memory request")
		}
		r.Requests = *list
	}

	// ??????Limits???Requests??????????????????????????????
	if len(r.Limits) > 0 || len(r.Requests) > 0 {
		return &r, nil
	}
	return nil, nil
}

func isResourceDaprEnabled(annotations map[string]string) bool {
	return getBoolAnnotationOrDefault(annotations, daprEnabledKey, false)
}

//ok
func getServiceAddress(name, namespace, clusterDomain string, port int) string {
	return fmt.Sprintf("%s.%s.svc.%s:%d", name, namespace, clusterDomain, port)
}

// ok
func getPullPolicy(pullPolicy string) corev1.PullPolicy {
	switch pullPolicy {
	case "Always":
		return corev1.PullAlways
	case "Never":
		return corev1.PullNever
	case "IfNotPresent":
		return corev1.PullIfNotPresent
	default:
		return corev1.PullIfNotPresent
	}
}

func getSidecarContainer(annotations map[string]string, id, daprSidecarImage, imagePullPolicy, namespace, controlPlaneAddress, placementServiceAddress string, tokenVolumeMount *corev1.VolumeMount, trustAnchors, certChain, certKey, sentryAddress string, mtlsEnabled bool, identity string) (*corev1.Container, error) {
	// ???????????????????????????
	appPort, err := getAppPort(annotations)
	if err != nil {
		return nil, err
	}
	appPortStr := ""
	if appPort > 0 {
		appPortStr = fmt.Sprintf("%v", appPort)
	}

	// dapr?????? ??????????????????,??????true
	metricsEnabled := getEnableMetrics(annotations)
	// dapr?????? ?????????port
	metricsPort := getMetricsPort(annotations)
	// dapr?????? ???????????????
	maxConcurrency, err := getMaxConcurrency(annotations)
	// dapr?????? ???????????? 127.0.0.1
	sidecarListenAddresses := getListenAddresses(annotations)
	if err != nil {
		log.Warn(err)
	}
	// ?????????????????????https
	sslEnabled := appSSLEnabled(annotations)
	// dapr ??????????????????
	pullPolicy := getPullPolicy(imagePullPolicy)
	// todo dapr?????? ??????http???????????????  ?????????3501,??????????????????3500
	httpHandler := getProbeHTTPHandler(sidecarPublicPort, apiVersionV1, sidecarHealthzPath)
	//??????????????????
	allowPrivilegeEscalation := false
	// dapr??????????????? ??????
	requestBodySize, err := getMaxRequestBodySize(annotations)
	if err != nil {
		log.Warn(err)
	}
	// dapr ???????????????????????????
	readBufferSize, err := getReadBufferSize(annotations)
	if err != nil {
		log.Warn(err)
	}
	// ???????????????http????????????
	HTTPStreamRequestBodyEnabled := HTTPStreamRequestBodyEnabled(annotations)

	ports := []corev1.ContainerPort{
		{
			ContainerPort: int32(sidecarHTTPPort), //3500
			Name:          sidecarHTTPPortName,
		},
		{
			ContainerPort: int32(sidecarAPIGRPCPort), //50001      ????????????-----> 50001 [daprd] 50002 <-----> app
			Name:          sidecarGRPCPortName,
		},
		{
			ContainerPort: int32(sidecarInternalGRPCPort), //50002
			Name:          sidecarInternalGRPCPortName,
		},
		{
			ContainerPort: int32(metricsPort), //9090 ,????????????
			Name:          sidecarMetricsPortName,
		},
	}

	cmd := []string{"/daprd"}
	//        - '--mode'
	//        - kubernetes
	//        - '--dapr-http-port'
	//        - '3500'
	//        - '--dapr-grpc-port'
	//        - '50001'
	//        - '--dapr-internal-grpc-port'
	//        - '50002'
	//        - '--app-port'
	//        - '3002'
	//        - '--app-id'
	//        - dp-618b5e4aa5ebc3924db86860-workerapp
	//        - '--control-plane-address'
	//        - 'dapr-api.dapr-system.svc.cluster.local:80'
	//        - '--app-protocol'
	//        - http
	//        - '--placement-host-address'
	//        - 'dapr-placement-server.dapr-system.svc.cluster.local:50005'
	//        - '--config'
	//        - appconfig
	//        - '--log-level'
	//        - info
	//        - '--app-max-concurrency'
	//        - '-1'
	//        - '--sentry-address'
	//        - 'dapr-sentry.dapr-system.svc.cluster.local:80'
	//        - '--enable-metrics=true'
	//        - '--metrics-port'
	//        - '9090'
	//        - '--dapr-http-max-request-size'
	//        - '-1'
	//        - '--enable-mtls'
	args := []string{
		"--mode", "kubernetes",
		"--dapr-http-port", fmt.Sprintf("%v", sidecarHTTPPort),
		"--dapr-grpc-port", fmt.Sprintf("%v", sidecarAPIGRPCPort),
		"--dapr-internal-grpc-port", fmt.Sprintf("%v", sidecarInternalGRPCPort),
		"--dapr-listen-addresses", sidecarListenAddresses,
		"--dapr-public-port", fmt.Sprintf("%v", sidecarPublicPort),
		"--app-port", appPortStr,
		"--app-id", id,
		"--control-plane-address", controlPlaneAddress, // ???????????? operator ????????????
		//	//controlPlaneAddress = "dapr-api.dapr.svc.cluster.local:80"
		//	//placementServiceAddress = "dapr-placement-server.dapr.svc.cluster.local:50005"
		"--app-protocol", getProtocol(annotations), // ??????http
		"--placement-host-address", placementServiceAddress, // ?????? placementAddress  ????????????raft???????????? ??????????????? placement ????????????
		"--config", getConfig(annotations), // app??? /config ??????????????????daprd???????????????
		"--log-level", getLogLevel(annotations), // ????????????   info
		"--app-max-concurrency", fmt.Sprintf("%v", maxConcurrency),
		"--sentry-address", sentryAddress, // "dapr-sentry.dapr.svc.cluster.local:80" ??????????????? sentry ???????????? ,
		fmt.Sprintf("--enable-metrics=%t", metricsEnabled),
		"--metrics-port", fmt.Sprintf("%v", metricsPort),
		"--dapr-http-max-request-size", fmt.Sprintf("%v", requestBodySize),
		"--dapr-http-read-buffer-size", fmt.Sprintf("%v", readBufferSize),
	}
	debugEnabled := getEnableDebug(annotations)
	debugPort := getDebugPort(annotations)
	if debugEnabled {
		ports = append(ports, corev1.ContainerPort{
			Name:          sidecarDebugPortName,
			ContainerPort: int32(debugPort),
		})

		cmd = []string{"/dlv"}

		args = append([]string{
			fmt.Sprintf("--listen=:%v", debugPort),
			"--accept-multiclient",
			"--headless=true",
			"--log",
			"--api-version=2",
			"exec",
			"/daprd",
			"--",
		}, args...)
	}

	// ???????????????dapr???????????????
	if image := getStringAnnotation(annotations, daprImage); image != "" {
		daprSidecarImage = image
	}

	c := &corev1.Container{
		Name:            sidecarContainerName,
		Image:           daprSidecarImage,
		ImagePullPolicy: pullPolicy,
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: &allowPrivilegeEscalation,
		},
		Ports:   ports,
		Command: cmd,
		Env: []corev1.EnvVar{
			{
				Name:  "NAMESPACE",
				Value: namespace,
			},
		},
		Args: args,
		// ???????????????, ?????????pod?????????service???
		ReadinessProbe: &corev1.Probe{
			Handler:             httpHandler, // /v1.0/healthz
			InitialDelaySeconds: getInt32AnnotationOrDefault(annotations, daprReadinessProbeDelayKey, defaultHealthzProbeDelaySeconds),
			TimeoutSeconds:      getInt32AnnotationOrDefault(annotations, daprReadinessProbeTimeoutKey, defaultHealthzProbeTimeoutSeconds),
			PeriodSeconds:       getInt32AnnotationOrDefault(annotations, daprReadinessProbePeriodKey, defaultHealthzProbePeriodSeconds),
			FailureThreshold:    getInt32AnnotationOrDefault(annotations, daprReadinessProbeThresholdKey, defaultHealthzProbeThreshold),
		},
		// ???????????????
		LivenessProbe: &corev1.Probe{
			Handler:             httpHandler,
			InitialDelaySeconds: getInt32AnnotationOrDefault(annotations, daprLivenessProbeDelayKey, defaultHealthzProbeDelaySeconds),
			TimeoutSeconds:      getInt32AnnotationOrDefault(annotations, daprLivenessProbeTimeoutKey, defaultHealthzProbeTimeoutSeconds),
			PeriodSeconds:       getInt32AnnotationOrDefault(annotations, daprLivenessProbePeriodKey, defaultHealthzProbePeriodSeconds),
			FailureThreshold:    getInt32AnnotationOrDefault(annotations, daprLivenessProbeThresholdKey, defaultHealthzProbeThreshold),
		},
	}

	c.Env = append(c.Env, utils.ParseEnvString(annotations[daprEnvKey])...)
	// ???????????????????????????????????????token ?????????daprd??????
	if tokenVolumeMount != nil {
		c.VolumeMounts = []corev1.VolumeMount{
			*tokenVolumeMount,
		}
	}
	// ???????????????json??????,??????false
	if logAsJSONEnabled(annotations) {
		c.Args = append(c.Args, "--log-as-json")
	}
	// ????????????daprd?????????????????????
	if profilingEnabled(annotations) {
		c.Args = append(c.Args, "--enable-profiling")
	}

	c.Env = append(c.Env, corev1.EnvVar{
		Name:  certs.TrustAnchorsEnvVar, // ???????????????
		Value: trustAnchors,
	},
		corev1.EnvVar{
			Name:  certs.CertChainEnvVar, // ??????
			Value: certChain,
		},
		corev1.EnvVar{
			Name:  certs.CertKeyEnvVar, // ??????
			Value: certKey,
		},
		corev1.EnvVar{
			Name:  "SENTRY_LOCAL_IDENTITY", // req.Namespace:pod.Spec.ServiceAccountName
			Value: identity,
		})

	//???????????????????????? ,daprd??????
	if mtlsEnabled {
		c.Args = append(c.Args, "--enable-mtls")
	}
	// daprd ???app ?????????????????????ssl
	if sslEnabled {
		c.Args = append(c.Args, "--app-ssl")
	}
	// ????????????http????????????
	if HTTPStreamRequestBodyEnabled {
		c.Args = append(c.Args, "--http-stream-request-body")
	}

	// ????????????daprd???api???????????? ??????k8s???secret???????????? ,todo, ?????????????????????env?????????
	secret := getAPITokenSecret(annotations)
	if secret != "" {
		c.Env = append(c.Env, corev1.EnvVar{
			Name: auth.APITokenEnvVar,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					Key: "token",
					LocalObjectReference: corev1.LocalObjectReference{
						Name: secret,
					},
				},
			},
		})
	}
	// ????????????daprd???app???????????? ??????k8s???secret???????????? ,todo, ?????????????????????env?????????
	appSecret := GetAppTokenSecret(annotations)
	if appSecret != "" {
		c.Env = append(c.Env, corev1.EnvVar{
			Name: auth.AppAPITokenEnvVar,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					Key: "token",
					LocalObjectReference: corev1.LocalObjectReference{
						Name: appSecret,
					},
				},
			},
		})
	}
	// daprd ?????????????????????
	resources, err := getResourceRequirements(annotations)
	if err != nil {
		log.Warnf("couldn't set container resource requirements: %s. using defaults", err)
	}
	if resources != nil {
		c.Resources = *resources
	}
	return c, nil
}
