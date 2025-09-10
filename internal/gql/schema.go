package gql

import (
	"github.com/graphql-go/graphql"
)

// NewSchema builds the GraphQL schema using the provided resolvers.
func NewSchema(r *Resolvers) (graphql.Schema, error) {
	// ----- Types -----
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
			// getUser(id: ID!): Account
			"getUser": &graphql.Field{
				Type: accountType,
				Args: graphql.FieldConfigArgument{
					"id": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
				},
				Resolve: r.GetUser(),
			},

			// listUsers(limit: Int, offset: Int): [Account!]!
			"listUsers": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(accountType))),
				Args: graphql.FieldConfigArgument{
					"limit":  &graphql.ArgumentConfig{Type: graphql.Int},
					"offset": &graphql.ArgumentConfig{Type: graphql.Int},
				},
				Resolve: r.ListUsers(),
			},

			// getBalance(id: ID!): Int!
			"getBalance": &graphql.Field{
				Type: graphql.NewNonNull(graphql.Int),
				Args: graphql.FieldConfigArgument{
					"id": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
				},
				Resolve: r.GetBalance(),
			},

			// getUsersByCoinsRange(min: Int, max: Int): [Account!]!
			"getUsersByCoinsRange": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(accountType))),
				Args: graphql.FieldConfigArgument{
					"min": &graphql.ArgumentConfig{Type: graphql.Int},
					"max": &graphql.ArgumentConfig{Type: graphql.Int},
				},
				Resolve: r.GetUsersByCoinsRange(),
			},

			// getRecentRecharges(since: DateTime!): [Account!]!
			"getRecentRecharges": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(accountType))),
				Args: graphql.FieldConfigArgument{
					"since": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.DateTime)},
				},
				Resolve: r.GetRecentRecharges(),
			},

			// getInactiveSince(before: DateTime!): [Account!]!
			"getInactiveSince": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(accountType))),
				Args: graphql.FieldConfigArgument{
					"before": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.DateTime)},
				},
				Resolve: r.GetInactiveSince(),
			},

			// countUsers: Int!
			"countUsers": &graphql.Field{
				Type:    graphql.NewNonNull(graphql.Int),
				Resolve: r.CountUsers(),
			},

			// totalCoins: Int!
			"totalCoins": &graphql.Field{
				Type:    graphql.NewNonNull(graphql.Int),
				Resolve: r.TotalCoins(),
			},

			// existsUser(id: ID!): Boolean!
			"existsUser": &graphql.Field{
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
			// createUser(id: ID!, coins: Int): Account
			"createUser": &graphql.Field{
				Type: accountType,
				Args: graphql.FieldConfigArgument{
					"id":    &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
					"coins": &graphql.ArgumentConfig{Type: graphql.Int},
				},
				Resolve: r.CreateUser(),
			},

			// rechargeCoins(id: ID!, amount: Int!, userId: ID!, dataId: String): Account
			"rechargeCoins": &graphql.Field{
				Type: accountType,
				Args: graphql.FieldConfigArgument{
					"id":     &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
					"amount": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.Int)},
					"userId": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
					"dataId": &graphql.ArgumentConfig{Type: graphql.String},
				},
				Resolve: r.RechargeCoins(),
			},

			// batchRecharge(ids: [ID!]!, amount: Int!, userId: ID!, dataId: String): Int!
			"batchRecharge": &graphql.Field{
				Type: graphql.NewNonNull(graphql.Int),
				Args: graphql.FieldConfigArgument{
					"ids": &graphql.ArgumentConfig{
						Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(graphql.ID))),
					},
					"amount": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.Int)},
					"userId": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
					"dataId": &graphql.ArgumentConfig{Type: graphql.String},
				},
				Resolve: r.BatchRecharge(),
			},

			// useCoins(id: ID!, amount: Int!, userId: ID!, dataId: String): Account
			"useCoins": &graphql.Field{
				Type: accountType,
				Args: graphql.FieldConfigArgument{
					"id":     &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
					"amount": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.Int)},
					"userId": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
					"dataId": &graphql.ArgumentConfig{Type: graphql.String},
				},
				Resolve: r.UseCoins(),
			},

			// transferCoins(fromId: ID!, toId: ID!, amount: Int!, userId: ID!, dataId: String): TransferResult
			"transferCoins": &graphql.Field{
				Type: transferResultType,
				Args: graphql.FieldConfigArgument{
					"fromId": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
					"toId":   &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
					"amount": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.Int)},
					"userId": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
					"dataId": &graphql.ArgumentConfig{Type: graphql.String},
				},
				Resolve: r.TransferCoins(),
			},

			// setCoins(id: ID!, coins: Int!, userId: ID!, dataId: String): Account
			"setCoins": &graphql.Field{
				Type: accountType,
				Args: graphql.FieldConfigArgument{
					"id":     &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
					"coins":  &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.Int)},
					"userId": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
					"dataId": &graphql.ArgumentConfig{Type: graphql.String},
				},
				Resolve: r.SetCoins(),
			},

			// touchUsage(id: ID!): Account
			"touchUsage": &graphql.Field{
				Type: accountType,
				Args: graphql.FieldConfigArgument{
					"id": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
				},
				Resolve: r.TouchUsage(),
			},

			// deleteUser(id: ID!): Boolean!
			"deleteUser": &graphql.Field{
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
