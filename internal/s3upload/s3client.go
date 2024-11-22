//go:build go1.22

package s3upload

//go:generate go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen -include-tags presignedUrls --config=cfg.yaml  openapi.yaml
