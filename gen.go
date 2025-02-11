package es

//go:generate go run github.com/matryer/moq@v0.5.1 -pkg es_test -rm -out mocks_test.go . ReadWriter Storage
