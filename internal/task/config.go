package task

type S3TransferConfig struct {
	Endpoint        string `string:"Endpoint" validate:"http_url"`
	ChunkSizeMB     int64  `int64:"ChunkSizeMB" validate:"required"`
	ConcurrentFiles int    `int:"ConcurrentFiles" validate:"required"`
	PoolSize        int    `int:"PoolSize" validate:"required"`
}

type GlobusTransferConfig struct {
	ClientID              string   `yaml:"clientId"`
	ClientSecret          string   `yaml:"clientSecret,omitempty"`
	RedirectURL           string   `yaml:"redirectUrl"`
	Scopes                []string `yaml:"scopes,omitempty"`
	SourceCollection      string   `yaml:"sourceCollection"`
	SourcePrefixPath      string   `yaml:"sourcePrefixPath,omitempty"`
	DestinationCollection string   `yaml:"destinationCollection"`
	DestinationPrefixPath string   `yaml:"destinationPrefixPath,omitempty"`
	RefreshToken          string   `yaml:"refreshToken,omitempty"`
}

type TransferConfig struct {
	Method string               `string:"Method" validate:"oneof=S3 Globus"`
	S3     S3TransferConfig     `mapstructure:"S3" validate:"required_if=Method S3,omitempty"`
	Globus GlobusTransferConfig `mapstructure:"Globus" validate:"required_if=Method Globus,omitempty"`
}
