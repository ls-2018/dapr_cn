package config

import (
	"encoding/json"
	"os"
	"time"

	"github.com/pkg/errors" // ok
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/dapr/kit/logger" // ok

	scheme "github.com/dapr/dapr/pkg/client/clientset/versioned"
	dapr_config "github.com/dapr/dapr/pkg/config"
	"github.com/dapr/dapr/utils" // ok
)

const (
	kubernetesServiceHostEnvVar = "KUBERNETES_SERVICE_HOST"
	kubernetesConfig            = "kubernetes"
	selfHostedConfig            = "selfhosted"
	defaultPort                 = 50001
	defaultWorkloadCertTTL      = time.Hour * 24
	defaultAllowedClockSkew     = time.Minute * 15

	// defaultDaprSystemConfigName
	defaultDaprSystemConfigName = "daprsystem"
)

var log = logger.NewLogger("dapr.sentry.config")

// SentryConfig 认证证书配置
type SentryConfig struct {
	Port             int
	TrustDomain      string
	CAStore          string
	WorkloadCertTTL  time.Duration
	AllowedClockSkew time.Duration
	RootCertPath     string
	IssuerCertPath   string
	IssuerKeyPath    string
}

var configGetters = map[string]func(string) (SentryConfig, error){
	selfHostedConfig: getSelfhostedConfig,
	kubernetesConfig: getKubernetesConfig,
}

// FromConfigName 返回一个基于配置规范的哨兵配置。在出现错误时，将加载一个默认配置。
func FromConfigName(configName string) (SentryConfig, error) {
	var confGetterFn func(string) (SentryConfig, error)

	// 本代码中没有找到 KUBERNETES_SERVICE_HOST 的设置
	if IsKubernetesHosted() {
		confGetterFn = configGetters[kubernetesConfig]
	} else {
		confGetterFn = configGetters[selfHostedConfig]
	}

	conf, err := confGetterFn(configName)
	if err != nil {
		err = errors.Wrapf(err, "loading default config. couldn't find config name: %s", configName)
		conf = getDefaultConfig()
	}

	printConfig(conf)
	return conf, err
}

func printConfig(config SentryConfig) {
	caStore := "default"
	if config.CAStore != "" {
		caStore = config.CAStore
	}

	log.Infof("configuration: [port]: %v, [ca store]: %s, [allowed clock skew]: %s, [workload cert ttl]: %s",
		config.Port, caStore, config.AllowedClockSkew.String(), config.WorkloadCertTTL.String())
}

// 30

func IsKubernetesHosted() bool {
	return os.Getenv(kubernetesServiceHostEnvVar) != ""
}

func getDefaultConfig() SentryConfig {
	return SentryConfig{
		Port:             defaultPort,
		WorkloadCertTTL:  defaultWorkloadCertTTL,
		AllowedClockSkew: defaultAllowedClockSkew,
	}
}

func getKubernetesConfig(configName string) (SentryConfig, error) {
	defaultConfig := getDefaultConfig()

	kubeConf := utils.GetConfig()
	daprClient, err := scheme.NewForConfig(kubeConf)
	if err != nil {
		return defaultConfig, err
	}

	list, err := daprClient.ConfigurationV1alpha1().Configurations(meta_v1.NamespaceAll).List(meta_v1.ListOptions{})
	if err != nil {
		return defaultConfig, err
	}

	if configName == "" {
		configName = defaultDaprSystemConfigName
	}

	for _, i := range list.Items {
		if i.GetName() == configName {
			spec, _ := json.Marshal(i.Spec)

			var configSpec dapr_config.ConfigurationSpec
			json.Unmarshal(spec, &configSpec)

			conf := dapr_config.Configuration{
				Spec: configSpec,
			}
			return parseConfiguration(defaultConfig, &conf)
		}
	}
	return defaultConfig, errors.New("config CRD not found")
}

func getSelfhostedConfig(configName string) (SentryConfig, error) {
	defaultConfig := getDefaultConfig()

	daprConfig, _, err := dapr_config.LoadStandaloneConfiguration(configName)
	if err != nil {
		return defaultConfig, err
	}

	if daprConfig != nil {
		return parseConfiguration(defaultConfig, daprConfig)
	}
	return defaultConfig, nil
}

func GetDefaultConfig(configName string) (SentryConfig, error) {
	return getKubernetesConfig(configName)
}

func parseConfiguration(conf SentryConfig, daprConfig *dapr_config.Configuration) (SentryConfig, error) {
	//spec:
	//  metric:
	//    enabled: true
	//  mtls:
	//    allowedClockSkew: 15m
	//    enabled: true
	//    workloadCertTTL: 24h
	if daprConfig.Spec.MTLSSpec.WorkloadCertTTL != "" {
		d, err := time.ParseDuration(daprConfig.Spec.MTLSSpec.WorkloadCertTTL)
		if err != nil {
			return conf, errors.Wrap(err, "error parsing WorkloadCertTTL duration")
		}

		conf.WorkloadCertTTL = d
	}

	if daprConfig.Spec.MTLSSpec.AllowedClockSkew != "" {
		d, err := time.ParseDuration(daprConfig.Spec.MTLSSpec.AllowedClockSkew)
		if err != nil {
			return conf, errors.Wrap(err, "error parsing AllowedClockSkew duration")
		}

		conf.AllowedClockSkew = d
	}

	return conf, nil
}
