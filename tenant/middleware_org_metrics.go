package tenant

import (
	"context"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/metric"
	"github.com/influxdata/influxdb/kit/prom"
)

type orgMetrics struct {
	// RED metrics
	rec *metric.REDClient

	orgService influxdb.OrganizationService
}

var _ influxdb.OrganizationService = (*orgMetrics)(nil)

// NewOrgMetrics returns a metrics service middleware for the Organization Service.
func NewOrgMetrics(reg *prom.Registry) influxdb.OrgMiddleware {
	return func(orgService influxdb.OrganizationService) influxdb.OrganizationService {
		return &orgMetrics{
			rec:        metric.New(reg, "org"),
			orgService: orgService,
		}
	}
}

// Returns a single organization by ID.
func (m *orgMetrics) FindOrganizationByID(ctx context.Context, id influxdb.ID) (*influxdb.Organization, error) {
	rec := m.rec.Record("find_org_by_id")
	org, err := m.orgService.FindOrganizationByID(ctx, id)
	return org, rec(err)
}

// Returns the first organization that matches filter.
func (m *orgMetrics) FindOrganization(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
	rec := m.rec.Record("find_org")
	org, err := m.orgService.FindOrganization(ctx, filter)
	return org, rec(err)
}

// Returns a list of organizations that match filter and the total count of matching organizations.
// Additional options provide pagination & sorting.
func (m *orgMetrics) FindOrganizations(ctx context.Context, filter influxdb.OrganizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Organization, int, error) {
	rec := m.rec.Record("find_orgs")
	orgs, n, err := m.orgService.FindOrganizations(ctx, filter, opt...)
	return orgs, n, rec(err)
}

// Creates a new organization and sets b.ID with the new identifier.
func (m *orgMetrics) CreateOrganization(ctx context.Context, b *influxdb.Organization) error {
	rec := m.rec.Record("create_org")
	err := m.orgService.CreateOrganization(ctx, b)
	return rec(err)
}

// Updates a single organization with changeset.
// Returns the new organization state after update.
func (m *orgMetrics) UpdateOrganization(ctx context.Context, id influxdb.ID, upd influxdb.OrganizationUpdate) (*influxdb.Organization, error) {
	rec := m.rec.Record("update_org")
	updatedOrg, err := m.orgService.UpdateOrganization(ctx, id, upd)
	return updatedOrg, rec(err)
}

// Removes a organization by ID.
func (m *orgMetrics) DeleteOrganization(ctx context.Context, id influxdb.ID) error {
	rec := m.rec.Record("delete_org")
	err := m.orgService.DeleteOrganization(ctx, id)
	return rec(err)
}
