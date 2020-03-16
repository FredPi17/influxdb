package tenant

import (
	"context"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/metric"
	"github.com/influxdata/influxdb/kit/prom"
)

type bucketMetrics struct {
	// RED metrics
	rec *metric.REDClient

	bucketService influxdb.BucketService
}

var _ influxdb.BucketService = (*bucketMetrics)(nil)

// NewBucketMetrics returns a metrics service middleware for the Bucket Service.
func NewBucketMetrics(reg *prom.Registry) influxdb.BucketMiddleware {
	return func(bucketService influxdb.BucketService) influxdb.BucketService {
		return &bucketMetrics{
			rec:           metric.New(reg, "bucket"),
			bucketService: bucketService,
		}
	}
}

// Returns a single bucket by ID.
func (m *bucketMetrics) FindBucketByID(ctx context.Context, id influxdb.ID) (*influxdb.Bucket, error) {
	rec := m.rec.Record("find_bucket_by_id")
	bucket, err := m.bucketService.FindBucketByID(ctx, id)
	return bucket, rec(err)
}

// Returns the first bucket that matches filter.
func (m *bucketMetrics) FindBucket(ctx context.Context, filter influxdb.BucketFilter) (*influxdb.Bucket, error) {
	rec := m.rec.Record("find_bucket")
	bucket, err := m.bucketService.FindBucket(ctx, filter)
	return bucket, rec(err)
}

// FindBuckets returns a list of buckets that match filter and the total count of matching buckets.
func (m *bucketMetrics) FindBuckets(ctx context.Context, filter influxdb.BucketFilter, opt ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
	rec := m.rec.Record("find_buckets")
	buckets, n, err := m.bucketService.FindBuckets(ctx, filter, opt...)
	return buckets, n, rec(err)
}

// Creates a new bucket and sets b.ID with the new identifier.
func (m *bucketMetrics) CreateBucket(ctx context.Context, b *influxdb.Bucket) error {
	rec := m.rec.Record("create_bucket")
	err := m.bucketService.CreateBucket(ctx, b)
	return rec(err)
}

// Updates a single bucket with changeset and returns the new bucket state after update.
func (m *bucketMetrics) UpdateBucket(ctx context.Context, id influxdb.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
	rec := m.rec.Record("update_bucket")
	updatedBucket, err := m.bucketService.UpdateBucket(ctx, id, upd)
	return updatedBucket, rec(err)
}

// Removes a bucket by ID.
func (m *bucketMetrics) DeleteBucket(ctx context.Context, id influxdb.ID) error {
	rec := m.rec.Record("delete_bucket")
	err := m.bucketService.DeleteBucket(ctx, id)
	return rec(err)
}

// FindBucketByName finds a Bucket given its name and Organization ID
func (m *bucketMetrics) FindBucketByName(ctx context.Context, orgID influxdb.ID, name string) (*influxdb.Bucket, error) {
	rec := m.rec.Record("find_bucket_by_name")
	bucket, err := m.bucketService.FindBucketByName(ctx, orgID, name)
	return bucket, rec(err)
}
