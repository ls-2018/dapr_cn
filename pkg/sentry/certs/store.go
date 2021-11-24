package certs

import (
	"context"
	sentry_debug "github.com/dapr/dapr/code_debug/sentry"
	"os"

	"github.com/dapr/dapr/pkg/credentials"
	"github.com/dapr/dapr/pkg/sentry/config"
	"github.com/dapr/dapr/pkg/sentry/kubernetes"
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	defaultSecretNamespace = "default"
)

// StoreCredentials saves the trust bundle in a Kubernetes secret store or locally on disk, depending on the hosting platform.
func StoreCredentials(conf config.SentryConfig, rootCertPem, issuerCertPem, issuerKeyPem []byte) error {
	if config.IsKubernetesHosted() {
		return storeKubernetes(rootCertPem, issuerCertPem, issuerKeyPem)
	}
	return storeSelfhosted(rootCertPem, issuerCertPem, issuerKeyPem, conf.RootCertPath, conf.IssuerCertPath, conf.IssuerKeyPath)
}

func storeKubernetes(rootCertPem, issuerCertPem, issuerCertKey []byte) error {
	kubeClient, err := kubernetes.GetClient()
	if err != nil {
		return err
	}

	namespace := getNamespace()
	secret := &v1.Secret{
		Data: map[string][]byte{
			credentials.RootCertFilename:   rootCertPem,
			credentials.IssuerCertFilename: issuerCertPem,
			credentials.IssuerKeyFilename:  issuerCertKey,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      KubeScrtName,
			Namespace: namespace,
		},
		Type: v1.SecretTypeOpaque,
	}

	// We update and not create because sentry expects a secret to already exist
	_, err = kubeClient.CoreV1().Secrets(namespace).Update(context.TODO(), secret, metav1.UpdateOptions{})
	if err != nil {
		return errors.Wrap(err, "failed saving secret to kubernetes")
	}
	return nil
}

func getNamespace() string {
	namespace := os.Getenv("NAMESPACE")
	if namespace == "" {
		namespace = defaultSecretNamespace
	}
	return namespace
}

// CredentialsExist 检查根证书和用户证书是否存在
func CredentialsExist(conf config.SentryConfig) (bool, error) {
	if config.IsKubernetesHosted() {
		namespace := getNamespace()
		// 此处改用加载本地配置文件 ~/.kube/config
		kubeClient := sentry_debug.GetK8s()

		//  会使用的  /var/run/secrets/kubernetes.io/serviceaccount/token
		//  于本地调试来说不方便
		//kubeClient, err := kubernetes.GetClient()
		//if err != nil {
		//	return false, err
		//}
		s, err := kubeClient.CoreV1().Secrets(namespace).Get(context.TODO(), KubeScrtName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return len(s.Data) > 0, nil
	}
	return false, nil
}

/* #nosec. */
func storeSelfhosted(rootCertPem, issuerCertPem, issuerKeyPem []byte, rootCertPath, issuerCertPath, issuerKeyPath string) error {
	err := os.WriteFile(rootCertPath, rootCertPem, 0644)
	if err != nil {
		return errors.Wrapf(err, "failed saving file to %s", rootCertPath)
	}

	err = os.WriteFile(issuerCertPath, issuerCertPem, 0644)
	if err != nil {
		return errors.Wrapf(err, "failed saving file to %s", issuerCertPath)
	}

	err = os.WriteFile(issuerKeyPath, issuerKeyPem, 0644)
	if err != nil {
		return errors.Wrapf(err, "failed saving file to %s", issuerKeyPath)
	}
	return nil
}
