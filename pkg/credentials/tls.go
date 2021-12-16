package credentials

import (
	"crypto/tls"
	"crypto/x509"
)

// TLSConfigFromCertAndKey 返回一个tls.config对象从已经验证过的PEM格式的证书、秘钥对
func TLSConfigFromCertAndKey(certPem, keyPem []byte, serverName string, rootCA *x509.CertPool) (*tls.Config, error) {
	//从一对PEM编码的数据中解析出一个公钥/私钥对。
	//在成功返回时，Certificate.Leaf将为nil，因为证书的解析形式没有被保留。
	cert, err := tls.X509KeyPair(certPem, keyPem)
	if err != nil {
		return nil, err
	}

	// nolint:gosec
	config := &tls.Config{
		InsecureSkipVerify: false,
		RootCAs:            rootCA,
		//用来验证返回的证书上的主机名，除非给出InsecureSkipVerify。它也包括在客户端的握手中，以支持虚拟主机，除非它是一个IP地址。
		ServerName:         serverName,
		Certificates:       []tls.Certificate{cert},
	}

	return config, nil
}
