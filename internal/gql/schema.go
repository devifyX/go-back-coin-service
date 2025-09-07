package gql

import (
	"github.com/graphql-go/graphql"
)

// NewSchema builds and returns the GraphQL schema wired to the provided resolvers.
func NewSchema(r *Resolvers) (graphql.Schema, error) {
	accountType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Account",
		Fields: graphql.Fields{
			"id":               &graphql.Field{Type: graphql.NewNonNull(graphql.ID)},
			"coins":            &graphql.Field{Type: graphql.NewNonNull(graphql.Int)},
			"lastRechargeDate": &graphql.Field{Type: graphql.DateTime},
			"lastUsageDate":    &graphql.Field{Type: graphql.DateTime},
		},
	})

	transferResultType := graphql.NewObject(graphql.ObjectConfig{
		Name: "TransferResult",
		Fields: graphql.Fields{
			"from": &graphql.Field{Type: accountType},
			"to":   &graphql.Field{Type: accountType},
		},
	})

	// ----- Query Root -----
	query := graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			"getUser": {
				Type: accountType,
				Args: graphql.FieldConfigArgument{
					"id": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
				},
				Resolve: r.GetUser(),
			},
			"listUsers": {
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(accountType))),
				Args: graphql.FieldConfigArgument{
					"limit":  &graphql.ArgumentConfig{Type: graphql.Int},
					"offset": &graphql.ArgumentConfig{Type: graphql.Int},
				},
				Resolve: r.ListUsers(),
			},
			"getBalance": {
				Type: graphql.NewNonNull(graphql.Int),
				Args: graphql.FieldConfigArgument{
					"id": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
				},
				Resolve: r.GetBalance(),
			},
			"getUsersByCoinsRange": {
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(accountType))),
				Args: graphql.FieldConfigArgument{
					"min": &graphql.ArgumentConfig{Type: graphql.Int},
					"max": &graphql.ArgumentConfig{Type: graphql.Int},
				},
				Resolve: r.GetUsersByCoinsRange(),
			},
			"getRecentRecharges": {
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(accountType))),
				Args: graphql.FieldConfigArgument{
					"since": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.DateTime)},
				},
				Resolve: r.GetRecentRecharges(),
			},
			"getInactiveSince": {
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(accountType))),
				Args: graphql.FieldConfigArgument{
					"before": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.DateTime)},
				},
				Resolve: r.GetInactiveSince(),
			},
			"countUsers": {
				Type:    graphql.NewNonNull(graphql.Int),
				Resolve: r.CountUsers(),
			},
			"totalCoins": {
				Type:    graphql.NewNonNull(graphql.Int),
				Resolve: r.TotalCoins(),
			},
			"existsUser": {
				Type: graphql.NewNonNull(graphql.Boolean),
				Args: graphql.FieldConfigArgument{
					"id": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
				},
				Resolve: r.ExistsUser(),
			},
		},
	})

	// ----- Mutation Root -----
	mutation := graphql.NewObject(graphql.ObjectConfig{
		Name: "Mutation",
		Fields: graphql.Fields{
			"createUser": {
				Type: accountType,
				Args: graphql.FieldConfigArgument{
					"id":    &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
					"coins": &graphql.ArgumentConfig{Type: graphql.Int},
				},
				Resolve: r.CreateUser(),
			},
			"rechargeCoins": {
				Type: accountType,
				Args: graphql.FieldConfigArgument{
					"id":     &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
					"amount": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.Int)},
				},
				Resolve: r.RechargeCoins(),
			},
			"batchRecharge": {
				Type: graphql.NewNonNull(graphql.Int), // rows affected
				Args: graphql.FieldConfigArgument{
					"ids":    &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(graphql.ID)))},
					"amount": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.Int)},
				},
				Resolve: r.BatchRecharge(),
			},
			"useCoins": {
				Type: accountType,
				Args: graphql.FieldConfigArgument{
					"id":     &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
					"amount": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.Int)},
				},
				Resolve: r.UseCoins(),
			},
			"transferCoins": {
				Type: transferResultType,
				Args: graphql.FieldConfigArgument{
					"fromId": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
					"toId":   &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
					"amount": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.Int)},
				},
				Resolve: r.TransferCoins(),
			},
			"setCoins": {
				Type: accountType,
				Args: graphql.FieldConfigArgument{
					"id":    &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
					"coins": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.Int)},
				},
				Resolve: r.SetCoins(),
			},
			"touchUsage": {
				Type: accountType,
				Args: graphql.FieldConfigArgument{
					"id": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
				},
				Resolve: r.TouchUsage(),
			},
			"deleteUser": {
				Type: graphql.NewNonNull(graphql.Boolean),
				Args: graphql.FieldConfigArgument{
					"id": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
				},
				Resolve: r.DeleteUser(),
			},
		},
	})

	return graphql.NewSchema(graphql.SchemaConfig{
		Query:    query,
		Mutation: mutation,
	})
}
