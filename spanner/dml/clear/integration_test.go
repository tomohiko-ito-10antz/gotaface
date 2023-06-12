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

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestClearer_Clear(t *testing.T) {
	spannerAdminClient, spannerClient, tearDown := spanner_test.Setup(t, fmt.Sprintf(`dml_insert_%d`, time.Now().Unix()))
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
	col_timestamp TIMESTAMP
) PRIMARY KEY (id1, id2)
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
		_, err := tx.Update(ctx, spanner.NewStatement(`
INSERT INTO t (id1, id2, col_integer, col_string, col_float, col_bytes, col_bool, col_timestamp)
VALUES
	(1, 2, 4, 'abc', 1.25, b'124abc', TRUE,  '2023-06-11T01:23:45Z'),
	(2, 2, 3, 'def', 1.50, b'223def', FALSE, '2023-06-11T01:23:45Z'),
	(1, 1, 2, 'ghi', 1.75, b'112ghi', TRUE,  '2023-06-11T01:23:45Z'),
	(2, 1, 1, 'jkl', 2.00, b'211jkl', FALSE, '2023-06-11T01:23:45Z')
`))
		if err != nil {
			return fmt.Errorf(`fail to insert rows: %w`, err)
		}
		return nil
	})
	if err != nil {
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
