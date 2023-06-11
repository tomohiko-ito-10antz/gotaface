package clear_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	spanner_test "github.com/Jumpaku/gotaface/spanner/test"

	"cloud.google.com/go/spanner"
	spanner_adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/Jumpaku/gotaface/spanner/dml/clear"
)

var (
	testSpannerProject  string
	testSpannerInstance string
)
var skipTest bool

func TestMain(m *testing.M) {
	initialize()
	os.Exit(m.Run())
}

func initialize() {
	if testSpannerProject = os.Getenv(spanner_test.EnvTestSpannerProject); testSpannerProject == "" {
		skipTest = true
		return
	}
	if testSpannerInstance = os.Getenv(spanner_test.EnvTestSpannerInstance); testSpannerInstance == "" {
		skipTest = true
		return
	}
}

func TestClearer_Clear(t *testing.T) {
	if skipTest {
		t.Skipf(`environment variables %s and %s are required`, spanner_test.EnvTestSpannerProject, spanner_test.EnvTestSpannerInstance)
	}

	spannerAdminClient, spannerClient, tearDown, err := spanner_test.Setup(testSpannerProject, testSpannerInstance, fmt.Sprintf(`dml_insert_%d`, time.Now().Unix()))
	if err != nil {
		t.Fatalf(`fail to set up spanner: %v`, err)
	}
	defer tearDown()

	ctx := context.Background()
	ddl := &spanner_adminpb.UpdateDatabaseDdlRequest{
		Database: spannerClient.DatabaseName(),
		Statements: []string{
			`
CREATE TABLE t (
	id1 INT64,
	id2 INT64,
	col_integer INT64,
	col_string STRING(MAX),
	col_float FLOAT64,
	col_bytes BYTES(16),
	col_bool BOOL,
	col_date DATE,
	col_timestamp TIMESTAMP
) PRIMARY KEY (id1, id2);
INSERT INTO t (id1, id2, col_integer, col_string, col_float, col_bytes, col_bool, col_date, col_timestamp)
VALUES
	(1, 2, 4, 'abc', 1.25, b'124abc', TRUE,  '2023-06-11T01:23:45Z', '2023-06-11T01:23:45Z'),
	(2, 2, 3, 'def', 1.50, b'223def', FALSE, '2023-06-11T01:23:45Z', '2023-06-11T01:23:45Z'),
	(1, 1, 2, 'ghi', 1.75, b'112ghi', TRUE,  '2023-06-11T01:23:45Z', '2023-06-11T01:23:45Z'),
	(2, 1, 1, 'jkl', 2.00, b'211jkl', FALSE, '2023-06-11T01:23:45Z', '2023-06-11T01:23:45Z');
`,
		},
	}
	op, err := spannerAdminClient.UpdateDatabaseDdl(ctx, ddl)
	if err != nil {
		tearDown()
		t.Fatalf(`fail to wait create tables: %v`, err)
	}
	if err := op.Wait(ctx); err != nil {
		tearDown()
		t.Fatalf(`fail to wait create tables: %v`, err)
	}

	_, err = spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		sut := clear.NewClearer(tx)
		err = sut.Clear(ctx, `t`)
		if err != nil {
			return fmt.Errorf("fail to clear table: %w", err)
		}

		itr := tx.Query(ctx, spanner.Statement{SQL: `SELECT * FROM t`})
		if itr.RowCount != 0 {
			return fmt.Errorf("rows are remaining")
		}
		return nil
	})
	if err != nil {
		tearDown()
		t.Errorf(`fail to clear table: %v`, err)
	}
}
