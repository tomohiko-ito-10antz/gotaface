package test

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/spanner"

	spanner_admin "cloud.google.com/go/spanner/admin/database/apiv1"
	spanner_adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

func SkipIfNoEnv(t *testing.T) {
	t.Helper()

	e := GetEnvSpanner()
	if e.Project == "" || e.Instance == "" {
		t.Skipf(`environment variables %s and %s are required`, EnvTestSpannerProject, EnvTestSpannerInstance)
	}
}
func Setup(t *testing.T, database string) (*spanner_admin.DatabaseAdminClient, *spanner.Client, func()) {
	t.Helper()

	SkipIfNoEnv(t)

	env := GetEnvSpanner()
	ctx := context.Background()
	adminClient, err := spanner_admin.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatalf(`fail to create spanner admin client: %v`, err)
	}

	parent := fmt.Sprintf(`projects/%s/instances/%s`, env.Project, env.Instance)
	op, err := adminClient.CreateDatabase(ctx, &spanner_adminpb.CreateDatabaseRequest{
		Parent:          parent,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", database),
	})
	if err != nil {
		adminClient.Close()
		t.Fatalf(`fail to create spanner database in %s: %v`, parent, err)
	}
	if _, err := op.Wait(ctx); err != nil {
		adminClient.Close()
		t.Fatalf(`fail to wait create spanner database: %v`, err)
	}

	dataSource := fmt.Sprintf(`%s/databases/%s`, parent, database)
	client, err := spanner.NewClient(ctx, dataSource)
	if err != nil {
		adminClient.Close()
		t.Fatalf(`fail to create spanner client with %s: %v`, dataSource, err)
	}

	tearDown := func() {
		client.Close()
		adminClient.DropDatabase(ctx, &spanner_adminpb.DropDatabaseRequest{Database: dataSource})
		adminClient.Close()
	}

	return adminClient, client, tearDown
}

func InitDDL(t *testing.T, adminClient *spanner_admin.DatabaseAdminClient, database string, stmt []string) {
	t.Helper()

	ctx := context.Background()
	ddl := &spanner_adminpb.UpdateDatabaseDdlRequest{
		Database:   database,
		Statements: stmt,
	}

	op, err := adminClient.UpdateDatabaseDdl(ctx, ddl)
	if err != nil {
		t.Fatalf(`fail to execute ddl: %v`, err)
	}
	if err := op.Wait(ctx); err != nil {
		t.Fatalf(`fail to wait create tables: %v`, err)
	}
}

func InitDML(t *testing.T, client *spanner.Client, stmt []spanner.Statement) {
	_, err := client.ReadWriteTransaction(context.Background(), func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		for _, stmt := range stmt {
			_, err := tx.Update(ctx, stmt)
			if err != nil {
				return fmt.Errorf(`fail to insert rows: %w`, err)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf(`fail to wait create tables: %v`, err)
	}
}
