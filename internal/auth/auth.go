package auth

import (
	"fmt"

	"github.com/casbin/casbin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Authorizer struct {
	enforcer *casbin.Enforcer
	insecure bool
}

func New(model, policy string) *Authorizer {
	authorizer := &Authorizer{}
	enforcer := casbin.NewEnforcer(model, policy)

	if model == "" && policy == "" {
		authorizer.insecure = true
	}
	authorizer.enforcer = enforcer
	return authorizer
}

func (a *Authorizer) Authorize(subject, object, action string) error {
	if a.insecure {
		return nil
	}
	if !a.enforcer.Enforce(subject, object, action) {
		msg := fmt.Sprintf(
			"%s not permitted to %s to %s",
			subject,
			action,
			object,
		)

		st := status.New(codes.PermissionDenied, msg)
		return st.Err()
	}

	return nil
}
