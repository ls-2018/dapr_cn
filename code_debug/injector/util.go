package injector_debug

import (
	"io/ioutil"
	"os"
)

func PRE()  {
	os.Setenv("TLS_CERT_FILE", "/tmp/tls.crt")
	os.Setenv("TLS_KEY_FILE", "/tmp/tls.key")
	os.Setenv("NAMESPACE", "cluster.local")
	os.Setenv("KUBE_CLUSTER_DOMAIN", "dapr")
	os.Setenv("SIDECAR_IMAGE", "docker.io/daprio/daprd:1.3.0")
	os.Setenv("SIDECAR_IMAGE_PULL_POLICY", "IfNotPresent")
	crt := `-----BEGIN CERTIFICATE-----
MIID7TCCAtWgAwIBAgIQb8cx8SlRgjxZOeBRZz0KjTANBgkqhkiG9w0BAQsFADAj
MSEwHwYDVQQDExhkYXByLXNpZGVjYXItaW5qZWN0b3ItY2EwHhcNMjEwODIwMTAw
NjQ3WhcNMzEwODE4MTAwNjQ3WjAgMR4wHAYDVQQDExVkYXByLXNpZGVjYXItaW5q
ZWN0b3IwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCT5QtCySlFwNvd
6RRr8lbE3Gprazae+mNM+XDtzN723X/V/gx0Uf6hh7N7SMAdE6sD+fNcfDpjhuMj
l5CSWCsVBJcFtICiVXj3cCldMVJpX8sGOMlW9I8Q0HinxsWSpWtjBsf6/43xmb/V
dkXYNN8DVgTwnlJ0ok/crRY6zq3Mmb8alglugPJs2OlVZi590Bl2O0mC3vFalpwd
h5O2Gyn/DK/uYSyTEtgQcWNHffmSEbGFBKhqWyiD2MO3s/oRT7q+2Ly3wFc8SbOv
bptqJQnsjnt+koZmFHDrxTbiozEC13UoV7uJqCU14ujUmJ/DR6N8aG8V4rH+1gr3
MY8ADVcbAgMBAAGjggEeMIIBGjAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0lBBYwFAYI
KwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAUngvK
JDtppAlJ54vpJP5QieopzIUwgbkGA1UdEQSBsTCBroIhZGFwci1zaWRlY2FyLWlu
amVjdG9yLmRhcHItc3lzdGVtgiVkYXByLXNpZGVjYXItaW5qZWN0b3IuZGFwci1z
eXN0ZW0uc3Zjgi1kYXByLXNpZGVjYXItaW5qZWN0b3IuZGFwci1zeXN0ZW0uc3Zj
LmNsdXN0ZXKCM2RhcHItc2lkZWNhci1pbmplY3Rvci5kYXByLXN5c3RlbS5zdmMu
Y2x1c3Rlci5sb2NhbDANBgkqhkiG9w0BAQsFAAOCAQEAkkpEPBKXrxbzbZbOOJzE
QK0xIy05zVSR36yS7H1usauZqIMHoZO9fGdnJruj0UOHxa+GXS2eXgVLqzzQkgLT
G7mwCs4zqpFkHhbL6vpBKulpR+mwaN0isivvXMrPoytA6rebvK9RoPYTP7Muvq0K
kT7n1k/qOTRiKBYwmo+hmXIzZx4lo2uK2fpaQWqPStByyqOdmpHoFK7cc/PgOJ7U
p1TVMGS8YUYvtDogO8JSOKvVj/dxCITFgeaIQohGMtANiv/lGW/rOSwBFZLm4YEt
fpfA1orOwSQdHIN/CKN2xR3iwcJ1Xd9JdDdGVd0AAVuTCL8P1YXIDq4steYoUfOH
UQ==
-----END CERTIFICATE-----
`
	key := `-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEAk+ULQskpRcDb3ekUa/JWxNxqa2s2nvpjTPlw7cze9t1/1f4M
dFH+oYeze0jAHROrA/nzXHw6Y4bjI5eQklgrFQSXBbSAolV493ApXTFSaV/LBjjJ
VvSPENB4p8bFkqVrYwbH+v+N8Zm/1XZF2DTfA1YE8J5SdKJP3K0WOs6tzJm/GpYJ
boDybNjpVWYufdAZdjtJgt7xWpacHYeTthsp/wyv7mEskxLYEHFjR335khGxhQSo
alsog9jDt7P6EU+6vti8t8BXPEmzr26baiUJ7I57fpKGZhRw68U24qMxAtd1KFe7
iaglNeLo1Jifw0ejfGhvFeKx/tYK9zGPAA1XGwIDAQABAoIBABzHeF6BqJ9jPSW8
onWzDcF3JtpKXbGoBcTH0Xrk4Apn5eVtb1z/S66BKqL18DR4TFIUxI+duPr/F5oo
d0CTGacYYo8Apftw6H41/EafP2sz8dz/Febu5hwehSwY2P+FYxU9D5M/QY7fHV5b
ipHdD/ylM4bjOCZsI2BM6kN9ToK+qJ7JowIfA65sn0YOiKf15DZmUxnD62AbTYk3
7APDpbtEulH3ibFRxgkf4Sp7FNj6ySBhN5nIa1mDtnL6xQ5W6HaxcreMqAY9Uglt
XzAnlIadq9OkhJGn9TeQWz6lo7HBmwyZ6fXBIKC5qfIwIFlHOS+sFWZQY+fU8cmR
mHRHPrECgYEAwG6lWqvAb3hH/LfosE7LpoQL/gwbzbUtkfD49tymS8y0MBN+fW+K
Zz9HgsTQq2/3Z5Qo3kuzKpfG6+TnIJFo3pgsgR1vaP6ZwRXDqg59jIJn4PuxHmUZ
Q7sIm5ef5hU8pOtofwgkxXp1ZBlJpHELC/AbIxNl/7fblisRjxi2N1MCgYEAxMAC
Oy5jAx39/aTmuZyrRzMXo039/hZzqnjjYRinw9JNydAMbJjqJwIQmS5sMxe7YNJR
zaH1O5wnrxmdp9XHIJxELrxQQbWkDvgvxzuZ6ArRGcrWe18YoJBGrWtk4lszf3oz
QT/9p8Jyn7GSQg2IcPLK+hnba3kLkRInjAAoUBkCgYBIb94y2YBg9agzi489/wtO
LVrOUQSOVOtIiqtNUhYPZolVp6Dv2vMAlqN2qFvcjRNWnEEeHOTe3a910P7oFZSi
ZioqmEf+AAkk0+qJs1azc8tit5we0vPFuTwuRc9dLxVbB3qYhxpAdC6IdbRxAFSm
M8YjWAQHVNCGIMRFV4oQHQKBgHBIR72wfk70GdRb/FRnJZQvg0juJIqSE5ZxAQqT
gIKPjVckBUMgjViPpBtXU+Zgn83kmUyqJgLL3hXen4t9w/NHWt31GDQ3zhXA67te
tzmmmqiGiprDrZFMZRlpFZzcgWo5ufB97WuE29dpBlft0zTqSo4qnX8bCE/VNX5/
RIJZAoGAdEmr5zRRADXmazM/nYbF8HSqjXt5b+77ECM6z7sTrCA9z+WWMWXL5+QH
+B65Ilr98MN/dW89NuJ437QVpTCab81r4II40EFkHyRdFA56y+oKNQbUEUNV3Tiw
8CCgz0FTEDNea0EmRAxtfzpyI1dUg1AYfOWtx0Ej0HPk8ivgX4c=
-----END RSA PRIVATE KEY-----
`
	_ = ioutil.WriteFile("/tmp/tls.crt", []byte(crt), 0644)
	_ = ioutil.WriteFile("/tmp/tls.key", []byte(key), 0644)

}