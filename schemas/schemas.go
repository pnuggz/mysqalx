package schemas

import (
	"fmt"
)

var (
	schemas = map[string]string{
		"T1": T1,
		"T2": T2,
	}
	errInvalidSchema = fmt.Errorf("invalid schema requested")
)

func GetAllSchema() map[string]string {
	return schemas
}

func GetSchema(n string) (string, error) {
	if schemas[n] == "" {
		return "", errInvalidSchema
	}

	return schemas[n], nil
}
