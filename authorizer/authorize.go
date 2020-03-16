package authorizer

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb"
	icontext "github.com/influxdata/influxdb/context"
)

// IsAllowed checks to see if an action is authorized by retrieving the authorizer
// off of context and authorizing the action appropriately.
func IsAllowed(ctx context.Context, p influxdb.Permission) error {
	return IsAllowedAll(ctx, []influxdb.Permission{p})
}

// IsAllowedAll checks to see if an action is authorized by ALL permissions.
// Also see IsAllowed.
func IsAllowedAll(ctx context.Context, permissions []influxdb.Permission) error {
	a, err := icontext.GetAuthorizer(ctx)
	if err != nil {
		return err
	}
	for _, p := range permissions {
		if !a.Allowed(p) {
			return &influxdb.Error{
				Code: influxdb.EUnauthorized,
				Msg:  fmt.Sprintf("%s is unauthorized", p),
			}
		}
	}
	return nil
}

// IsAllowedAll checks to see if an action is authorized by ALL permissions.
// Also see IsAllowed.
func IsAllowedAny(ctx context.Context, permissions []influxdb.Permission) error {
	a, err := icontext.GetAuthorizer(ctx)
	if err != nil {
		return err
	}
	for _, p := range permissions {
		if a.Allowed(p) {
			return nil
		}
	}
	return &influxdb.Error{
		Code: influxdb.EUnauthorized,
		Msg:  fmt.Sprintf("none of %v is authorized", permissions),
	}
}

func authorize(ctx context.Context, a influxdb.Action, rt influxdb.ResourceType, rid, oid influxdb.ID) error {
	p, err := influxdb.NewPermissionAtID(rid, a, rt, oid)
	if err != nil {
		return err
	}
	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}
	return nil
}

func AuthorizeRead(ctx context.Context, rt influxdb.ResourceType, rid, oid influxdb.ID) error {
	return authorize(ctx, influxdb.ReadAction, rt, rid, oid)
}

func AuthorizeWrite(ctx context.Context, rt influxdb.ResourceType, rid, oid influxdb.ID) error {
	return authorize(ctx, influxdb.WriteAction, rt, rid, oid)
}

func AuthorizeCreate(ctx context.Context, rt influxdb.ResourceType, oid influxdb.ID) error {
	p, err := influxdb.NewPermission(influxdb.WriteAction, rt, oid)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}
	return nil
}

func authorizeForOrg(ctx context.Context, a influxdb.Action, oid influxdb.ID) error {
	p := influxdb.Permission{Action: a, Resource: influxdb.Resource{Type: influxdb.OrgsResourceType, ID: &oid}}
	if err := p.Valid(); err != nil {
		return err
	}
	if err := IsAllowed(ctx, p); err != nil {
		return err
	}
	return nil
}

func AuthorizeReadOrg(ctx context.Context, oid influxdb.ID) error {
	return authorizeForOrg(ctx, influxdb.ReadAction, oid)
}

func AuthorizeWriteOrg(ctx context.Context, oid influxdb.ID) error {
	return authorizeForOrg(ctx, influxdb.WriteAction, oid)
}

func authorizeForUser(ctx context.Context, a influxdb.Action, uid influxdb.ID) error {
	p := influxdb.Permission{Action: a, Resource: influxdb.Resource{Type: influxdb.UsersResourceType, ID: &uid}}
	if err := p.Valid(); err != nil {
		return err
	}
	if err := IsAllowed(ctx, p); err != nil {
		return err
	}
	return nil
}

func AuthorizeReadUser(ctx context.Context, uid influxdb.ID) error {
	return authorizeForUser(ctx, influxdb.ReadAction, uid)
}

func AuthorizeWriteUser(ctx context.Context, uid influxdb.ID) error {
	return authorizeForUser(ctx, influxdb.WriteAction, uid)
}
