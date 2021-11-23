package certs

const (
	// KubeScrtName is the name of the kubernetes secret that holds the trust bundle.
	KubeScrtName = "dapr-trust-bundle"
	// TrustAnchorsEnvVar is the environment variable name for the trust anchors in the sidecar.
	//是边车中信任锚的环境变量名。
	TrustAnchorsEnvVar = "DAPR_TRUST_ANCHORS"
	CertChainEnvVar    = "DAPR_CERT_CHAIN"
	CertKeyEnvVar      = "DAPR_CERT_KEY"
)
