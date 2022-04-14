package handlerlog

import (
	"go.uber.org/zap"
	"net/http"
)

type HandlerWithLoggers func(w http.ResponseWriter, r *http.Request, accessLogger *zap.Logger, handlerLogger *zap.Logger)

func WithLogger(handlerFunc HandlerWithLoggers, accessLogger, handlerLogger *zap.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		handlerFunc(w, r, accessLogger, handlerLogger)
	}
}
