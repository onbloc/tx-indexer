package graph

import (
	"context"
	embed "embed"
	"io/fs"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/go-chi/chi/v5"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/gnolang/tx-indexer/events"
	"github.com/gnolang/tx-indexer/serve/graph/model"
	"github.com/gnolang/tx-indexer/storage"
)

//go:embed examples/*.gql
var examples embed.FS

func Setup(s storage.Storage, manager *events.Manager, m *chi.Mux) *chi.Mux {
	srv := newGraphQueryServer(NewExecutableSchema(
		Config{
			Resolvers: NewResolver(s, manager),
			Directives: DirectiveRoot{
				Filterable: func(
					ctx context.Context,
					_ interface{},
					next graphql.Resolver,
					_ []model.FilterableExtra,
				) (interface{}, error) {
					return next(ctx)
				},
			},
		},
	))

	srv.AddTransport(&transport.Websocket{})

	srv.AroundOperations(func(ctx context.Context, next graphql.OperationHandler) graphql.ResponseHandler {
		oc := graphql.GetOperationContext(ctx)
		if oc.Operation != nil && oc.Operation.Operation == ast.Query {
			if includesIntrospection(oc.Operation.SelectionSet) {
				return func(ctx context.Context) *graphql.Response {
					return &graphql.Response{
						Errors: gqlerror.List{
							&gqlerror.Error{
								Message: "GraphQL introspection is disabled",
							},
						},
					}
				}
			}
		}

		return next(ctx)
	})

	es, err := examplesToSlice()
	if err != nil {
		panic(err)
	}

	m.Handle("/graphql", HandlerWithDefaultTabs("Gno Indexer: GraphQL playground", "/graphql/query", es))
	m.Handle("/graphql/query", srv)

	return m
}

func examplesToSlice() ([]string, error) {
	var out []string

	err := fs.WalkDir(examples, ".", func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}

		content, err := examples.ReadFile(path)
		if err != nil {
			return err
		}

		out = append(out, string(content))

		return nil
	})

	return out, err
}

func newGraphQueryServer(es graphql.ExecutableSchema) *handler.Server {
	srv := handler.New(es)

	srv.AddTransport(transport.Websocket{
		KeepAlivePingInterval: 10 * time.Second,
	})
	srv.AddTransport(transport.Options{})
	srv.AddTransport(transport.GET{})
	srv.AddTransport(transport.POST{})
	srv.AddTransport(transport.MultipartForm{})

	srv.SetQueryCache(lru.New[*ast.QueryDocument](1000))

	srv.Use(extension.AutomaticPersistedQuery{
		Cache: lru.New[string](100),
	})

	return srv
}

func includesIntrospection(selectionSet ast.SelectionSet) bool {
	for _, selection := range selectionSet {
		switch sel := selection.(type) {
		case *ast.Field:
			if sel.Name == "__schema" || sel.Name == "__typename" {
				return true
			}

			if includesIntrospection(sel.SelectionSet) {
				return true
			}
		case *ast.FragmentSpread:
			if sel.Definition != nil && includesIntrospection(sel.Definition.SelectionSet) {
				return true
			}
		case *ast.InlineFragment:
			if includesIntrospection(sel.SelectionSet) {
				return true
			}
		}
	}

	return false
}
