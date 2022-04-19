package handlerlog

import (
	"go.uber.org/zap"
	"net/http"
)

type HandlerWithLogger func(w http.ResponseWriter, r *http.Request, logger *zap.Logger)

func WithLogger(handlerFunc HandlerWithLogger, logger *zap.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		handlerFunc(w, r, logger)
	}
}
