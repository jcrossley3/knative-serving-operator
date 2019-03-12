package controller

import (
	"github.com/jcrossley3/knative-serving-operator/pkg/controller/install"
)

func init() {
	// AddToManagerFuncs is a list of functions to create controllers and add them to a manager.
	AddToManagerFuncs = append(AddToManagerFuncs, install.Add)
}
