// Copyright 2019 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"context"
	"database/sql"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
)

const perfReplicationConnStatusQuery = `
	SELECT
		CHANNEL_NAME,
		GROUP_NAME,
		LAST_QUEUED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP,
		LAST_QUEUED_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP,
		LAST_QUEUED_TRANSACTION_START_QUEUE_TIMESTAMP,
		LAST_QUEUED_TRANSACTION_END_QUEUE_TIMESTAMP,
		QUEUEING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP,
		QUEUEING_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP,
		QUEUEING_TRANSACTION_START_QUEUE_TIMESTAMP
    FROM performance_schema.replication_connection_status
	`

// Metric descriptors.
var (
	performanceSchemaReplicationConnStatusLastQueuedTransactionOriginalCommitTimestampDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "last_queued_transaction_original_commit_timestamp_seconds"),
		"A timestamp that shows when the last transaction queued in the relay log was committed on the original source",
		[]string{"channel_name", "group_name"}, nil,
	)

	performanceSchemaReplicationConnStatusLastQueuedTransactionImmediateCommitTimestampDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "last_queued_transaction_immediate_commit_timestamp_seconds"),
		"A timestamp that shows when the last transaction queued in the relay log was committed on the immediate source",
		[]string{"channel_name", "group_name"}, nil,
	)

	performanceSchemaReplicationConnStatusLastQueuedTransactionStartQueueTimestampDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "last_queued_transaction_start_queue_timestamp_seconds"),
		"A timestamp that shows when the last transaction was placed in the relay log queue by this I/O thread",
		[]string{"channel_name", "group_name"}, nil,
	)

	performanceSchemaReplicationConnStatusLastQueuedTransactionEndQueueTimestampDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "last_queued_transaction_end_queue_timestamp_seconds"),
		"A timestamp that shows when the last transaction was queued to the relay log files",
		[]string{"channel_name", "group_name"}, nil,
	)

	performanceSchemaReplicationConnStatusQueueingTransactionOriginalCommitTimestampDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "queueing_transaction_original_commit_timestamp_seconds"),
		"A timestamp that shows when the currently queueing transaction was committed on the original source",
		[]string{"channel_name", "group_name"}, nil,
	)

	performanceSchemaReplicationConnStatusQueueingTransactionImmediateCommitTimestampDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "queueing_transaction_immediate_commit_timestamp_seconds"),
		"A timestamp that shows when the currently queueing transaction was committed on the immediate source",
		[]string{"channel_name", "group_name"}, nil,
	)

	performanceSchemaReplicationConnStatusQueueingTransactionStartQueueTimestampDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "queueing_transaction_start_queue_timestamp_seconds"),
		"A timestamp that shows when the first event of the currently queueing transaction was written to the relay log by this I/O thread",
		[]string{"channel_name", "group_name"}, nil,
	)
)

type ScrapePerfReplicationConnectionStatus struct{}

// Name of the Scraper. Should be unique.
func (ScrapePerfReplicationConnectionStatus) Name() string {
	return performanceSchema + ".replication_connection_status"
}

// Help describes the role of the Scraper.
func (ScrapePerfReplicationConnectionStatus) Help() string {
	return "Collect metrics from performance_schema.replication_connection_status"
}

// Version of MySQL from which scraper is available.
func (ScrapePerfReplicationConnectionStatus) Version() float64 {
	return 8.0
}

func (ScrapePerfReplicationConnectionStatus) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric, logger log.Logger) error {
	perfReplicationConnStatusRows, err := db.QueryContext(ctx, perfReplicationConnStatusQuery)
	if err != nil {
		if mysqlErr, ok := err.(*mysqldriver.MySQLError); ok { // Now the error number is accessible directly
			// Check for error 1054: Unknown column
			if mysqlErr.Number == 1054 {
				level.Debug(logger).Log("msg", "information_schema.replication_connection_status is not available before MySQL 8.0")
				return nil
			}
		}
		return err
	}
	defer perfReplicationConnStatusRows.Close()

	var (
		channelName, groupName string
	)

	var (
		lastQueuedTransactionOriginalCommitTimestamp, lastQueuedTransactionImmediateCommitTimestamp string
		lastQueuedTransactionStartQueueTimestamp, lastQueuedTransactionEndQueueTimestamp            string
		queueingTransactionOriginalCommitTimestamp, queueingTransactionImmediateCommitTimestamp     string
		queueingTransactionStartQueueTimestamp                                                      string
	)

	for perfReplicationConnStatusRows.Next() {
		if err := perfReplicationConnStatusRows.Scan(&channelName, &groupName,
			&lastQueuedTransactionOriginalCommitTimestamp, &lastQueuedTransactionImmediateCommitTimestamp,
			&lastQueuedTransactionStartQueueTimestamp, &lastQueuedTransactionEndQueueTimestamp,
			&queueingTransactionOriginalCommitTimestamp, &queueingTransactionImmediateCommitTimestamp,
			&queueingTransactionStartQueueTimestamp,
		); err != nil {
			return err
		}

		ch <- prometheus.MustNewConstMetric(
			performanceSchemaReplicationConnStatusLastQueuedTransactionOriginalCommitTimestampDesc,
			prometheus.GaugeValue, timeStringToUnixMicro(lastQueuedTransactionOriginalCommitTimestamp), channelName, groupName,
		)

		ch <- prometheus.MustNewConstMetric(
			performanceSchemaReplicationConnStatusLastQueuedTransactionImmediateCommitTimestampDesc,
			prometheus.GaugeValue, timeStringToUnixMicro(lastQueuedTransactionImmediateCommitTimestamp), channelName, groupName,
		)

		ch <- prometheus.MustNewConstMetric(
			performanceSchemaReplicationConnStatusLastQueuedTransactionStartQueueTimestampDesc,
			prometheus.GaugeValue, timeStringToUnixMicro(lastQueuedTransactionStartQueueTimestamp), channelName, groupName,
		)

		ch <- prometheus.MustNewConstMetric(
			performanceSchemaReplicationConnStatusLastQueuedTransactionEndQueueTimestampDesc,
			prometheus.GaugeValue, timeStringToUnixMicro(lastQueuedTransactionEndQueueTimestamp), channelName, groupName,
		)

		ch <- prometheus.MustNewConstMetric(
			performanceSchemaReplicationConnStatusQueueingTransactionOriginalCommitTimestampDesc,
			prometheus.GaugeValue, timeStringToUnixMicro(queueingTransactionOriginalCommitTimestamp), channelName, groupName,
		)

		ch <- prometheus.MustNewConstMetric(
			performanceSchemaReplicationConnStatusQueueingTransactionImmediateCommitTimestampDesc,
			prometheus.GaugeValue, timeStringToUnixMicro(queueingTransactionImmediateCommitTimestamp), channelName, groupName,
		)

		ch <- prometheus.MustNewConstMetric(
			performanceSchemaReplicationConnStatusQueueingTransactionStartQueueTimestampDesc,
			prometheus.GaugeValue, timeStringToUnixMicro(queueingTransactionStartQueueTimestamp), channelName, groupName,
		)
	}

	return nil
}

func timeStringToUnixMicro(value string) float64 {
	t, err := time.Parse(timeLayout, value)
	if err != nil {
		t = time.Time{}
	}

	if t.IsZero() {
		return 0
	}

	return float64(t.UnixNano()) / 1e9
}

var _ Scraper = ScrapePerfReplicationConnectionStatus{}
