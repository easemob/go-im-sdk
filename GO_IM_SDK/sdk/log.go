package sdk

import "log/slog"

func defaultLogger() *slog.Logger { return slog.Default() }
