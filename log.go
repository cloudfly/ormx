package ormx

import (
	"os"

	"github.com/rs/zerolog"
)

var log zerolog.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()

func SetLogger(l zerolog.Logger) {
	log = l
}
