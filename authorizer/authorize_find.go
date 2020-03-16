package authorizer

import (
	"context"

	"github.com/influxdata/influxdb"
)

func authorizeFindAuthorizations(ctx context.Context, rs []*influxdb.Authorization) ([]*influxdb.Authorization, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		err := AuthorizeRead(ctx, influxdb.AuthorizationsResourceType, r.ID, r.OrgID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}
		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

func authorizeFindBuckets(ctx context.Context, rs []*influxdb.Bucket) ([]*influxdb.Bucket, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		err := AuthorizeRead(ctx, influxdb.BucketsResourceType, r.ID, r.OrgID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}
		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

func authorizeFindDashboards(ctx context.Context, rs []*influxdb.Dashboard) ([]*influxdb.Dashboard, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		err := AuthorizeRead(ctx, influxdb.DashboardsResourceType, r.ID, r.OrganizationID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}
		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

func authorizeFindOrganizations(ctx context.Context, rs []*influxdb.Organization) ([]*influxdb.Organization, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		err := AuthorizeReadOrg(ctx, r.ID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}
		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

func authorizeFindSources(ctx context.Context, rs []*influxdb.Source) ([]*influxdb.Source, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		err := AuthorizeRead(ctx, influxdb.SourcesResourceType, r.ID, r.OrganizationID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}
		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

func authorizeFindTasks(ctx context.Context, rs []*influxdb.Task) ([]*influxdb.Task, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		err := AuthorizeRead(ctx, influxdb.TasksResourceType, r.ID, r.OrganizationID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}
		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

func authorizeFindTelegrafs(ctx context.Context, rs []*influxdb.TelegrafConfig) ([]*influxdb.TelegrafConfig, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		err := AuthorizeRead(ctx, influxdb.TelegrafsResourceType, r.ID, r.OrgID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}
		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

func authorizeFindUsers(ctx context.Context, rs []*influxdb.User) ([]*influxdb.User, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		err := AuthorizeReadUser(ctx, r.ID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}
		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

func authorizeFindVariables(ctx context.Context, rs []*influxdb.Variable) ([]*influxdb.Variable, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		err := AuthorizeRead(ctx, influxdb.VariablesResourceType, r.ID, r.OrganizationID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}
		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

func authorizeFindScrapers(ctx context.Context, rs []*influxdb.ScraperTarget) ([]*influxdb.ScraperTarget, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		err := AuthorizeRead(ctx, influxdb.ScraperResourceType, r.ID, r.OrgID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}
		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

func authorizeFindLabels(ctx context.Context, rs []*influxdb.Label) ([]*influxdb.Label, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		err := AuthorizeRead(ctx, influxdb.LabelsResourceType, r.ID, r.OrgID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}
		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

func authorizeFindNotificationRules(ctx context.Context, rs []influxdb.NotificationRule) ([]influxdb.NotificationRule, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		err := AuthorizeRead(ctx, influxdb.NotificationRuleResourceType, r.GetID(), r.GetOrgID())
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}
		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

func authorizeFindNotificationEndpoints(ctx context.Context, rs []influxdb.NotificationEndpoint) ([]influxdb.NotificationEndpoint, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		err := AuthorizeRead(ctx, influxdb.NotificationEndpointResourceType, r.GetID(), r.GetOrgID())
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}
		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

func authorizeFindChecks(ctx context.Context, rs []influxdb.Check) ([]influxdb.Check, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		err := AuthorizeRead(ctx, influxdb.ChecksResourceType, r.GetID(), r.GetOrgID())
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}
		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}
