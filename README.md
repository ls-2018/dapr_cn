<div style="text-align: center"><img src="/img/dapr_logo.svg" height="120px">
 
</div>

[![Go Report Card](https://goreportcard.com/badge/github.com/dapr/dapr)](https://goreportcard.com/report/github.com/dapr/dapr)
[![Docker Pulls](https://img.shields.io/docker/pulls/daprio/daprd)](https://hub.docker.com/r/daprio/dapr)
[![Build Status](https://github.com/dapr/dapr/workflows/dapr/badge.svg?event=push&branch=master)](https://github.com/dapr/dapr/actions?workflow=dapr)
[![Scheduled e2e test](https://github.com/dapr/dapr/workflows/dapr-test/badge.svg?event=schedule)](https://github.com/dapr/dapr/actions?workflow=dapr-test)
[![codecov](https://codecov.io/gh/dapr/dapr/branch/master/graph/badge.svg)](https://codecov.io/gh/dapr/dapr)
[![Discord](https://img.shields.io/discord/778680217417809931)](https://discord.com/channels/778680217417809931/778680217417809934)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![TODOs](https://badgen.net/https/api.tickgit.com/badgen/github.com/dapr/dapr)](https://www.tickgit.com/browse?repo=github.com/dapr/dapr)
[![Follow on Twitter](https://img.shields.io/twitter/follow/daprdev.svg?style=social&logo=twitter)](https://twitter.com/intent/follow?screen_name=daprdev)

Dapr是一种可移植的、无服务器的、事件驱动的运行时，它使开发人员能够轻松地构建运行在云和边缘上的有弹性的、无状态的和有状态的微服务，并拥抱语言和开发人员框架的多样性。


中文注释

## Code of Conduct

Please refer to our [Dapr Community Code of Conduct](https://github.com/dapr/community/blob/master/CODE-OF-CONDUCT.md)


- daprd sidecar负责流量代理、etc
- injector 负责将daprd注入到副本集中


``` 
    -   /usr/local/go/src/crypto/x509/root_unix.go:20
        SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
```