// Copyright 2023 The grovemq Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"encoding/json"
	"fmt"
)

// PropertyAgentError is the MQTT v5 User Property carrying the A2A error name.
const PropertyAgentError = "a2a-error"

// MQTT PUBACK/PUBREC reason codes used by the semantic error mapping. MQTT has
// no code per A2A error, so specific semantics travel in a2a-error.
const (
	ReasonSuccess             byte = 0x00
	ReasonNotAuthorized       byte = 0x02
	ReasonImplementationError byte = 0x10
	ReasonNotSupported        byte = 0x11
)

// ErrorName is the stable A2A semantic error name carried in a2a-error.
type ErrorName string

const (
	ErrorInvalidRequest           ErrorName = "InvalidRequestError"
	ErrorInvalidParams            ErrorName = "InvalidParamsError"
	ErrorMethodNotFound           ErrorName = "MethodNotFoundError"
	ErrorTaskNotFound             ErrorName = "TaskNotFoundError"
	ErrorTaskNotCancelable        ErrorName = "TaskNotCancelableError"
	ErrorPushNotSupported         ErrorName = "PushNotificationNotSupportedError"
	ErrorUnsupportedOperation     ErrorName = "UnsupportedOperationError"
	ErrorContentTypeNotSupported  ErrorName = "ContentTypeNotSupportedError"
	ErrorInvalidAgentResponse     ErrorName = "InvalidAgentResponseError"
	ErrorExtensionSupportRequired ErrorName = "ExtensionSupportRequiredError"
	ErrorVersionNotSupported      ErrorName = "VersionNotSupportedError"
	ErrorAgentNotAvailable        ErrorName = "AgentNotAvailableError"
	ErrorUnauthorized             ErrorName = "UnauthorizedError"
	ErrorExtendedCardMissing      ErrorName = "ExtendedAgentCardNotConfiguredError"
	ErrorInternal                 ErrorName = "InternalError"
)

// SemanticError is an A2A error with a stable name, JSON-RPC code and MQTT
// reason-code mapping.
type SemanticError struct {
	Name    ErrorName
	Message string
	Data    interface{}
}

func (e *SemanticError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" {
		return string(e.Name)
	}
	return fmt.Sprintf("%s: %s", e.Name, e.Message)
}

// NewError returns a semantic error with the default message for name.
func NewError(name ErrorName, message string) *SemanticError {
	if message == "" {
		message = string(name)
	}
	return &SemanticError{Name: name, Message: message}
}

// RPCCode maps the error to a JSON-RPC 2.0 code.
func (e ErrorName) RPCCode() int {
	switch e {
	case ErrorInvalidRequest:
		return -32600
	case ErrorInvalidParams:
		return -32602
	case ErrorMethodNotFound:
		return -32601
	case ErrorTaskNotFound:
		return -32001
	case ErrorTaskNotCancelable:
		return -32002
	case ErrorPushNotSupported:
		return -32003
	case ErrorUnsupportedOperation:
		return -32004
	case ErrorContentTypeNotSupported:
		return -32005
	case ErrorInvalidAgentResponse:
		return -32006
	case ErrorExtendedCardMissing:
		return -32007
	case ErrorExtensionSupportRequired:
		return -32008
	case ErrorVersionNotSupported:
		return -32009
	// GroveMQ binding-specific extensions live outside the A2A 1.0 range.
	case ErrorAgentNotAvailable:
		return -32101
	case ErrorUnauthorized:
		return -32102
	default:
		return -32603
	}
}

// MQTTReason maps the error to a PUBACK/PUBREC reason code.
func (e ErrorName) MQTTReason() byte {
	switch e {
	case ErrorUnsupportedOperation, ErrorVersionNotSupported, ErrorMethodNotFound:
		return ReasonNotSupported
	case ErrorAgentNotAvailable, ErrorUnauthorized:
		return ReasonNotAuthorized
	default:
		return ReasonImplementationError
	}
}

// NewSemanticErrorResponse builds a JSON-RPC error response from a semantic
// error, preserving the request ID and stable A2A error code.
func NewSemanticErrorResponse(id json.RawMessage, sem *SemanticError) *Response {
	return NewErrorResponse(id, sem.ToRPC())
}

// ToRPC converts the semantic error into a JSON-RPC error object.
func (e *SemanticError) ToRPC() *Error {
	if e == nil {
		return nil
	}
	return &Error{Code: e.Name.RPCCode(), Message: e.Message, Data: e.Data}
}

// UserProperties returns the MQTT v5 properties needed to carry the A2A error
// name alongside the reason code.
func (e *SemanticError) UserProperties() map[string][]byte {
	if e == nil {
		return nil
	}
	return map[string][]byte{PropertyAgentError: []byte(e.Name)}
}

// TaskNotFound builds the error used when a task ID is unknown.
func TaskNotFound(id string) *SemanticError {
	return NewError(ErrorTaskNotFound, "task not found: "+id)
}

// AgentNotFound builds the error used when a request targets an address with
// no registered agent. The A2A 1.0 semantic name is TaskNotFoundError; the
// message identifies the missing agent for callers.
func AgentNotFound(address string) *SemanticError {
	return NewError(ErrorTaskNotFound, "agent not found: "+address)
}

// TaskNotCancelable builds the error used when a terminal task is cancelled.
func TaskNotCancelable(id string) *SemanticError {
	return NewError(ErrorTaskNotCancelable, "task is not cancelable: "+id)
}
