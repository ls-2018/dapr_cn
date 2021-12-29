package daprd_debug

import (
	"bytes"
	"fmt"
	auth "github.com/dapr/dapr/pkg/runtime/security"
	"github.com/pkg/errors"
	"io/ioutil"
	raw_k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
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

	go func() {
		// 开启pprof，监听请求
		ip := "127.0.0.1:6060"
		if err := http.ListenAndServe(ip, nil); err != nil {
			fmt.Printf("start failed on %s\n", ip)
		}
	}()

	fmt.Println(os.Getpid())
	KillProcess(3001)
	go SubCommand([]string{"zsh", "-c", "python3 cmd/daprd/daprd.py"})
	*metricEnable = true
	*metricPort = "9090"
	*outputLevel = "info"
	//*controlPlaneAddress = "dapr-api.dapr-system.svc.cluster.local:80"
	*controlPlaneAddress = "dapr-api.dapr-system.svc.cluster.local:6500" // daprd 注入时指定的值
	*placementServiceHostAddr = "dapr-placement-server.dapr-system.svc.cluster.local:50005"
	//*sentryAddress = "dapr-sentry.dapr-system.svc.cluster.local:80"
	*sentryAddress = "dapr-sentry.dapr-system.svc.cluster.local:10080"

	*appProtocol = "http"
	// kubectl port-forward svc/dapr-sentry -n dapr-system 10080:80 &
	*config = "appconfig" // 注入的时候，就确定了
	*appMaxConcurrency = -1
	//*mode = "kubernetes"
	*daprHTTPPort = "3500"
	*daprAPIGRPCPort = "50003"
	*daprInternalGRPCPort = "50001"
	*appPort = "3001"

	*daprHTTPMaxRequestSize = -1
	*enableMTLS = true
	KillProcess(6500)
	go SubCommand([]string{"zsh", "-c", "kubectl port-forward svc/dapr-api -n dapr-system 6500:80"})
	KillProcess(10080)
	go SubCommand([]string{"zsh", "-c", "kubectl port-forward svc/dapr-sentry -n dapr-system 10080:80"})
	KillProcess(50005)
	go SubCommand([]string{"zsh", "-c", "kubectl port-forward svc/dapr-placement-server -n dapr-system 50005:50005 "})
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
	nameSpace := "mesoid"
	// kubectl -n mesoid exec -it pod/etcd-0 -- cat /var/run/secrets/kubernetes.io/serviceaccount/token

	taskId := "61c2cb20562850d49d47d1c7"
	//
	*appID = "app01"

	KillProcess(50001)
	go SubCommand([]string{"zsh", "-c", "kubectl port-forward svc/dp-" + taskId + "-executorapp-dapr -n " + nameSpace + " 50001:50001"})
	//*appID = "ls-demo"  // 不能包含.
	UpdateHosts(fmt.Sprintf("dp-%s-executorapp-dapr.%s.svc.cluster.local", taskId, nameSpace))

	command := exec.Command("zsh", "-c", fmt.Sprintf("kubectl -n %s get pods|grep worker |grep %s |grep Running|awk 'NR==1'|awk '{print $1}'", nameSpace, taskId))
	podNameBytes, _ := command.CombinedOutput()
	podName := strings.Trim(string(podNameBytes), "\n")

	getToken := fmt.Sprintf("kubectl -n mesoid exec -it pod/%s -c worker-agent -- cat /var/run/secrets/kubernetes.io/serviceaccount/token > /tmp/token", podName)
	go SubCommand([]string{"zsh", "-c", getToken})
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

	os.Setenv("NAMESPACE", nameSpace)
	os.Setenv("SENTRY_LOCAL_IDENTITY", nameSpace+":default") // 用于验证 token 是不是这个sa的,注入的时候指定的
	*auth.GetKubeTknPath() = "/tmp/token"
	KillProcess(45454)
	go SubCommand([]string{"zsh", "-c", "kubectl port-forward svc/dapr-redis-svc -n " + nameSpace + " 45454:45454"})
	time.Sleep(time.Second * 14)
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

func RunCommand(name string, arg ...string) error {
	cmd := exec.Command(name, arg...)
	// 命令的错误输出和标准输出都连接到同一个管道
	stdout, err := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout

	if err != nil {
		return err
	}

	if err = cmd.Start(); err != nil {
		return err
	}
	// 从管道中实时获取输出并打印到终端
	for {
		tmp := make([]byte, 1024)
		_, err := stdout.Read(tmp)
		fmt.Print(string(tmp))
		if err != nil {
			break
		}
	}

	if err = cmd.Wait(); err != nil {
		return err
	}
	return nil
}

func UpdateHosts(domain string) {
	file, err := ioutil.ReadFile("/etc/hosts_bak")
	if err != nil {
		panic(err)
	}
	change := string(file) + "\n127.0.0.1                    " + domain + "\n"
	change += "\n127.0.0.1                    " + "dapr-redis-svc" + "\n"
	err = ioutil.WriteFile("/etc/hosts", []byte(change), 777)
	if err != nil {
		panic(err)
	}
}

func KillProcess(port int) {
	command := exec.Command("zsh", "-c", "kill-port.sh "+strconv.Itoa(port))
	err := command.Run()
	if err != nil {
		panic(err)
	}
}

func SubCommand(opt []string) {
	cmd := exec.Command(opt[0], opt[1:]...)
	stdout, err := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout
	if err != nil {
		panic(err)
	}
	if err = cmd.Start(); err != nil {
		panic(err)
	}
	for {
		tmp := make([]byte, 1024)
		_, err := stdout.Read(tmp)

		fmt.Print(strings.Join(strings.Split(string(tmp), "\n"), "\n[-----------SubCommand-------]"))
		if err != nil {
			break
		}
	}
}

func Home() (string, error) {
	user, err := user.Current()
	if nil == err {
		return user.HomeDir, nil
	}

	// cross compile support

	if "windows" == runtime.GOOS {
		return homeWindows()
	}

	// Unix-like system, so just assume Unix
	return homeUnix()
}

func homeUnix() (string, error) {
	// First prefer the HOME environmental variable
	if home := os.Getenv("HOME"); home != "" {
		return home, nil
	}

	// If that fails, try the shell
	var stdout bytes.Buffer
	cmd := exec.Command("sh", "-c", "eval echo ~$USER")
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return "", err
	}

	result := strings.TrimSpace(stdout.String())
	if result == "" {
		return "", errors.New("blank output when reading home directory")
	}

	return result, nil
}

func homeWindows() (string, error) {
	drive := os.Getenv("HOMEDRIVE")
	path := os.Getenv("HOMEPATH")
	home := drive + path
	if drive == "" || path == "" {
		home = os.Getenv("USERPROFILE")
	}
	if home == "" {
		return "", errors.New("HOMEDRIVE, HOMEPATH, and USERPROFILE are blank")
	}

	return home, nil
}
