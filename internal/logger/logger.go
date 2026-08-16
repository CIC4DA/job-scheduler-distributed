package logger

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

func New(service string) zerolog.Logger {
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}

	// zerolog.Logger is built with a fluent/chained API: .With() starts a context builder, .Str("service", service) attaches a field that'll appear on every log line from this logger, .Logger() finalizes it. You'll call it like log.Info().Str("job_id", id).Msg("processing job") — each call in the chain returns something you keep chaining until .Msg(...), which actually writes the line.
	return zerolog.New(output).With().Timestamp().Str("service", service).Logger()
}