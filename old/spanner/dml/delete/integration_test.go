package delete_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Jumpaku/gotaface/spanner/dml/delete"
	spanner_test "github.com/Jumpaku/gotaface/spanner/test"

	"cloud.google.com/go/spanner"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestDeleter_Clear(t *testing.T) {
	adminClient, client, tearDown := spanner_test.Setup(t, fmt.Sprintf(`dml_clear_%d`, time.Now().UnixNano()))
	defer tearDown()

	spanner_test.InitDDL(t, adminClient, client.DatabaseName(), []string{`
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
`})

	spanner_test.InitDML(t, client, []spanner.Statement{spanner.NewStatement(`
INSERT INTO t (id1, id2, col_integer, col_string, col_float, col_bytes, col_bool, col_timestamp)
VALUES
	(1, 2, 4, 'abc', 1.25, b'124abc', TRUE,  '2023-06-11T01:23:45Z'),
	(2, 2, 3, 'def', 1.50, b'223def', FALSE, '2023-06-11T01:23:45Z'),
	(1, 1, 2, 'ghi', 1.75, b'112ghi', TRUE,  '2023-06-11T01:23:45Z'),
	(2, 1, 1, 'jkl', 2.00, b'211jkl', FALSE, '2023-06-11T01:23:45Z')
`)})

	sut := delete.NewDeleter(client)

	err := sut.Delete(context.Background(), `t`)
	if err != nil {
		t.Errorf(`fail to delete table: %v`, err)
	}

	tx := client.ReadOnlyTransaction()
	defer tx.Close()

	rows := spanner_test.ListRows[struct{}](t, tx, `t`)
	if len(rows) > 0 {
		t.Errorf("rows are remaining")
	}
}
