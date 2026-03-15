package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"
)

func configureLogging(logLevel *string) {
	handler := &LogHandler{}
	_ = handler.level.UnmarshalText([]byte(*logLevel))
	slog.SetDefault(slog.New(handler))
}

type LogHandler struct {
	level slog.Level
}

func (l *LogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= l.level
}

func (l *LogHandler) Handle(_ context.Context, record slog.Record) error {
	// FIXME log attributes (and groups?)
	_, _ = fmt.Fprintf(os.Stderr, "%s %s %s\n", record.Time.Format(time.DateTime), record.Level.String(), record.Message)
	return nil
}

func (l *LogHandler) WithAttrs(_ []slog.Attr) slog.Handler {
	return l
}

func (l *LogHandler) WithGroup(_ string) slog.Handler {
	return l
}
