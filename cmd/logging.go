package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"
)

func configureLogging(logLevel string) error {
	handler := &LogHandler{}
	slog.SetDefault(slog.New(handler))
	return handler.level.UnmarshalText([]byte(logLevel))
}

type LogHandler struct {
	level slog.Level
}

func (l *LogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= l.level
}

func (l *LogHandler) Handle(_ context.Context, record slog.Record) error {
	// For now, we do not log attributes and groups. Attributes are the final varargs in the log methods, and are
	// typically used to log key/value pairs, e.g. "name": "Tom", "age": 5. So... don't used the varargs when logging.
	_, _ = fmt.Fprintf(os.Stderr, "%s %s %s\n", record.Time.Format(time.DateTime), record.Level.String(), record.Message)
	return nil
}

func (l *LogHandler) WithAttrs(_ []slog.Attr) slog.Handler {
	return l
}

func (l *LogHandler) WithGroup(_ string) slog.Handler {
	return l
}
