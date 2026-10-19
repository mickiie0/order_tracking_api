// Package apperror defines the consistent API error format:
// { "code": string, "message": string, "details"?: any }
package apperror

import (
	"fmt"
	"net/http"
)

type Error struct {
	Status  int    `json:"-"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func New(status int, code, msg string, details any) *Error {
	return &Error{Status: status, Code: code, Message: msg, Details: details}
}

func Validation(details any) *Error {
	return New(http.StatusBadRequest, "VALIDATION_ERROR", "request validation failed", details)
}

func InvalidOrderNo(no string) *Error {
	return New(http.StatusBadRequest, "INVALID_ORDER_NO", "order number must match ^ORD-\\d+-\\d+$", map[string]string{"order_no": no})
}

func OrderNotFound(no string) *Error {
	return New(http.StatusNotFound, "ORDER_NOT_FOUND", "order not found", map[string]string{"order_no": no})
}

func InvalidTransition(from, to string) *Error {
	return New(http.StatusConflict, "INVALID_STATE_TRANSITION",
		fmt.Sprintf("cannot transition from %s to %s", from, to), map[string]string{"from": from, "to": to})
}

func ConcurrentModification() *Error {
	return New(http.StatusConflict, "CONCURRENT_MODIFICATION", "order was modified concurrently, please retry", nil)
}

func LeaseLost(no string) *Error {
	return New(http.StatusConflict, "LEASE_LOST", "bot no longer holds the lease for this order", map[string]string{"order_no": no})
}

func Forbidden() *Error {
	return New(http.StatusForbidden, "FORBIDDEN", "order does not belong to this user", nil)
}

func Internal() *Error {
	return New(http.StatusInternalServerError, "INTERNAL_ERROR", "internal server error", nil)
}
