package daprd_debug

import (
	"fmt"
	nr_kubernetes "github.com/dapr/components-contrib/nameresolution/kubernetes"
	"io/ioutil"
	raw_k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"os"
	"os/exec"
	"path/filepath"
)

func PRE(
	mode,
	daprHTTPPort,
	daprAPIGRPCPort,
	appPort, appID, controlPlaneAddress, appProtocol, placementServiceHostAddr, config,
	sentryAddress, outputLevel,
	metricPort,
	daprInternalGRPCPort *string,
	appMaxConcurrency,
	daprHTTPMaxRequestSize *int,
	metricEnable,
	enableMTLS *bool,
) {
	*metricEnable = true
	*metricPort = "9090"
	*outputLevel = "info"
	*controlPlaneAddress = "dapr-api.dapr-system.svc.cluster.local:80"

	*controlPlaneAddress = "127.0.0.1:6500"
	// kubectl port-forward svc/dapr-api -n dapr-system 6500:80 &
	*appID = "dp-618b5e4aa5ebc3924db86860-executorapp"
	*placementServiceHostAddr = "dapr-placement-server.dapr-system.svc.cluster.local:50005"
	*appProtocol = "http"
	*sentryAddress = "dapr-sentry.dapr-system.svc.cluster.local:80"
	*config = "appconfig" // 注入的时候，就确定了的
	*appMaxConcurrency = -1
	*mode = "kubernetes"
	*daprHTTPPort = "3500"
	*daprAPIGRPCPort = "50001"
	*daprInternalGRPCPort = "50002"
	*appPort = "3001"

	*daprHTTPMaxRequestSize = -1
	*enableMTLS = true
	command := exec.Command("zsh", "-c", "kubectl port-forward svc/dapr-api -n dapr-system 6500:80 &")
	err := command.Run()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	// 以下证书，是从daprd的环境变量中截取的

	crt := `-----BEGIN CERTIFICATE-----
MIIBxTCCAWqgAwIBAgIQc55uyj2aQwZ44JcP0YBp7DAKBggqhkjOPQQDAjAxMRcw
FQYDVQQKEw5kYXByLmlvL3NlbnRyeTEWMBQGA1UEAxMNY2x1c3Rlci5sb2NhbDAe
Fw0yMTA4MjAwOTUyMjJaFw0yMjA4MjAxMDA3MjJaMBgxFjAUBgNVBAMTDWNsdXN0
ZXIubG9jYWwwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAR75g0xjAr4VcZed62s
xzZ2kUH3Bxatbdq2/5+4hJyMyX7iFNfG1RgJSgfJpTiHXbRbd13Q6Mk+XbY8pZgA
V40Eo30wezAOBgNVHQ8BAf8EBAMCAQYwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4E
FgQUN+JjAulZKw/mBAaPgYhvbukY6KkwHwYDVR0jBBgwFoAUT4gnr9tzfi5hRW9A
9aQF0b5p680wGAYDVR0RBBEwD4INY2x1c3Rlci5sb2NhbDAKBggqhkjOPQQDAgNJ
ADBGAiEA1TtSlfgQXmaQ3rEqt+raaG3QUXWKc6bVuvc8oxQGeQQCIQDCGMgoedRX
w+ZOMIjU2uBQ3QZ/ayy273tQHM/beTgDPQ==
-----END CERTIFICATE-----`
	key := `-----BEGIN EC PRIVATE KEY-----
MHcCAQEEILld+Jm1MDXgVq75SKcgh+wVBYQ/UiYd0TLRoH1wV3P1oAoGCCqGSM49
AwEHoUQDQgAEe+YNMYwK+FXGXnetrMc2dpFB9wcWrW3atv+fuIScjMl+4hTXxtUY
CUoHyaU4h120W3dd0OjJPl22PKWYAFeNBA==
-----END EC PRIVATE KEY-----`
	ca := `-----BEGIN CERTIFICATE-----
MIIB3DCCAYKgAwIBAgIRAJrB3Ct76di0AV9xiwz4RYgwCgYIKoZIzj0EAwIwMTEX
MBUGA1UEChMOZGFwci5pby9zZW50cnkxFjAUBgNVBAMTDWNsdXN0ZXIubG9jYWww
HhcNMjEwODIwMDk1MjIyWhcNMjIwODIwMTAwNzIyWjAxMRcwFQYDVQQKEw5kYXBy
LmlvL3NlbnRyeTEWMBQGA1UEAxMNY2x1c3Rlci5sb2NhbDBZMBMGByqGSM49AgEG
CCqGSM49AwEHA0IABEaGZHa2M60u0BvgAoOn4zqUYr3nRGzKKNhT5f9f4SqlI31V
ftvfJBnOQt6dM3YHGifIUPo782N6MzghkKxeapOjezB5MA4GA1UdDwEB/wQEAwIC
BDAdBgNVHSUEFjAUBggrBgEFBQcDAQYIKwYBBQUHAwIwDwYDVR0TAQH/BAUwAwEB
/zAdBgNVHQ4EFgQUT4gnr9tzfi5hRW9A9aQF0b5p680wGAYDVR0RBBEwD4INY2x1
c3Rlci5sb2NhbDAKBggqhkjOPQQDAgNIADBFAiBscw216OcA8jt9tI1LmTywzNVV
zfCt2fhdjXEK2GGEMAIhAKi0GsyI5b2hkrUkIEZm1kTLbeuw0GIguSvW89yUkXbT
-----END CERTIFICATE-----`

	os.Setenv("DAPR_CERT_CHAIN", crt)
	os.Setenv("DAPR_CERT_KEY", key)
	//ca
	os.Setenv("DAPR_TRUST_ANCHORS", ca)

	os.Setenv("NAMESPACE", "mesoid")

	rootCertPath := "/tmp/ca.crt"
	issuerCertPath := "/tmp/issuer.crt"
	issuerKeyPath := "/tmp/issuer.key"

	_ = ioutil.WriteFile(rootCertPath, []byte(ca), 0644)
	_ = ioutil.WriteFile(issuerCertPath, []byte(crt), 0644)
	_ = ioutil.WriteFile(issuerKeyPath, []byte(key), 0644)

	os.Setenv("NAMESPACE", "mesoid")
}

// GetK8s 此处改用加载本地配置文件 ~/.kube/config
func GetK8s() *raw_k8s.Clientset {
	conf, err := rest.InClusterConfig()
	if err != nil {
		// 路径直接写死
		conf, err = clientcmd.BuildConfigFromFlags("", filepath.Join(homedir.HomeDir(), ".kube", "config"))
		if err != nil {
			panic(err)
		}
	}

	kubeClient, _ := raw_k8s.NewForConfig(conf)
	return kubeClient
}

func demo() {
	fmt.Println(nr_kubernetes.NewResolver)
}
