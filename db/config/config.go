package config

type Config struct {
	Dir                 string
	MaxLevel            int
	SstSize             int
	SstDataBlockSize    int
	SstFooterSize       int
	SstBlockTrailerSize int
	SstRestartInterval  int
}

func NewConfig(dir string) *Config {
	return &Config{
		Dir:                 dir,
		MaxLevel:            10,
		SstSize:             16 * 1024 * 1024,
		SstDataBlockSize:    16 * 1024 * 1024,
		SstFooterSize:       40,
		SstBlockTrailerSize: 4,
		SstRestartInterval:  16,
	}
}
