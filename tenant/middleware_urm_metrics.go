package tenant

import (
	"context"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/metric"
	"github.com/influxdata/influxdb/kit/prom"
)

type urmMetrics struct {
	// RED metrics
	rec *metric.REDClient

	urmService influxdb.UserResourceMappingService
}

var _ influxdb.UserResourceMappingService = (*urmMetrics)(nil)

// NewUrmMetrics returns a metrics service middleware for the URM Service.
func NewUrmMetrics(reg *prom.Registry) influxdb.UserResourceMappingMiddleware {
	return func(urmService influxdb.UserResourceMappingService) influxdb.UserResourceMappingService {
		return &urmMetrics{
			rec:        metric.New(reg, "urm"),
			urmService: urmService,
		}
	}
}

// FindUserResourceMappings returns a list of UserResourceMappings that match filter and the total count of matching mappings.
func (m *urmMetrics) FindUserResourceMappings(ctx context.Context, filter influxdb.UserResourceMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.UserResourceMapping, int, error) {
	rec := m.rec.Record("find_urms")
	urms, n, err := m.urmService.FindUserResourceMappings(ctx, filter, opt...)
	return urms, n, rec(err)
}

// CreateUserResourceMapping creates a user resource mapping.
func (m *urmMetrics) CreateUserResourceMapping(ctx context.Context, urm *influxdb.UserResourceMapping) error {
	rec := m.rec.Record("create_urm")
	err := m.urmService.CreateUserResourceMapping(ctx, urm)
	return rec(err)
}

// DeleteUserResourceMapping deletes a user resource mapping.
func (m *urmMetrics) DeleteUserResourceMapping(ctx context.Context, resourceID, userID influxdb.ID) error {
	rec := m.rec.Record("delete_urm")
	err := m.urmService.DeleteUserResourceMapping(ctx, resourceID, userID)
	return rec(err)
}
