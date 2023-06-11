package test

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"

	spanner_admin "cloud.google.com/go/spanner/admin/database/apiv1"
	spanner_adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

func Setup(project, instance, database string) (*spanner_admin.DatabaseAdminClient, *spanner.Client, func(), error) {
	ctx := context.Background()
	adminClient, err := spanner_admin.NewDatabaseAdminClient(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf(`fail to create spanner admin client: %v`, err)
	}

	parent := fmt.Sprintf(`projects/%s/instances/%s`, project, instance)
	op, err := adminClient.CreateDatabase(ctx, &spanner_adminpb.CreateDatabaseRequest{
		Parent:          parent,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", database),
	})
	if err != nil {
		adminClient.Close()
		return nil, nil, nil, fmt.Errorf(`fail to create spanner database in %s: %v`, parent, err)
	}
	if _, err := op.Wait(ctx); err != nil {
		adminClient.Close()
		return nil, nil, nil, fmt.Errorf(`fail to wait create spanner database: %v`, err)
	}

	dataSource := fmt.Sprintf(`%s/databases/%s`, parent, database)
	client, err := spanner.NewClient(ctx, dataSource)
	if err != nil {
		adminClient.Close()
		return nil, nil, nil, fmt.Errorf(`fail to create spanner client with %s: %v`, dataSource, err)
	}

	tearDown := func() {
		client.Close()
		adminClient.DropDatabase(ctx, &spanner_adminpb.DropDatabaseRequest{Database: dataSource})
		adminClient.Close()
	}

	return adminClient, client, tearDown, nil
}
