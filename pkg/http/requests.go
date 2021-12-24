// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package http

// OutputBindingRequest is the request object to invoke an output binding.
type OutputBindingRequest struct {
	Metadata  map[string]string `json:"metadata"`
	Data      interface{}       `json:"data"`
	Operation string            `json:"operation"`
}

// BulkGetRequest 是请求对象，用于从状态存储中获取多个键的值的列表。
type BulkGetRequest struct {
	Metadata    map[string]string `json:"metadata"`
	Keys        []string          `json:"keys"`
	Parallelism int               `json:"parallelism"`
}
