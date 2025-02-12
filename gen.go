package es

//go:generate go tool moq -pkg es_test -rm -out mocks_test.go . ReadWriter Storage
