package testpg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// Template owns an immutable migrated database for this test process.
type Template struct {
	cloneGate   chan struct{}
	admin       *sql.DB
	maintenance string
	name        string
	config      *pgx.ConnConfig
}

// NewTemplate prepares a database with a private schema for cloning.
// prepare must close all connections it opens before returning.
func NewTemplate(ctx context.Context, prepare func(context.Context, *pgx.ConnConfig) error) (_ *Template, result error) {
	dsn, err := BaseDSNContext(ctx)
	if err != nil {
		return nil, err
	}
	base, err := parseTestConfig(dsn)
	if err != nil {
		return nil, err
	}
	name, err := newResourceName('t')
	if err != nil {
		return nil, err
	}
	schema, err := newResourceName('s')
	if err != nil {
		return nil, err
	}
	admin := stdlib.OpenDB(*base)
	admin.SetMaxOpenConns(4)
	statement, err := templateStatement(ctx, admin, name)
	if err != nil {
		return nil, errors.Join(err, admin.Close())
	}
	drop := registerResource(name, func(ctx context.Context) error {
		if err := dropDatabase(ctx, admin, base.Database, name); err != nil {
			return err
		}
		return admin.Close()
	})
	defer func() {
		if result != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
			defer cancel()
			result = errors.Join(result, drop(cleanupCtx))
		}
	}()
	if _, err := admin.ExecContext(ctx, statement); err != nil {
		return nil, fmt.Errorf("create PostgreSQL test template: %w", err)
	}
	config := base.Copy()
	config.Database = name
	config.RuntimeParams["search_path"] = schema
	initializer := stdlib.OpenDB(*config)
	_, err = initializer.ExecContext(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize())
	if err := errors.Join(err, initializer.Close()); err != nil {
		return nil, fmt.Errorf("create PostgreSQL template schema: %w", err)
	}
	if err := prepare(ctx, config.Copy()); err != nil {
		return nil, fmt.Errorf("prepare PostgreSQL test template: %w", err)
	}
	if _, err := admin.ExecContext(ctx, "ALTER DATABASE "+pgx.Identifier{name}.Sanitize()+" ALLOW_CONNECTIONS false"); err != nil {
		return nil, err
	}
	return &Template{admin: admin, maintenance: base.Database, name: name, config: config, cloneGate: make(chan struct{}, 2)}, nil
}

// Clone returns a fresh database with real independent transactions and catalogs.
// Close all connections to the clone before invoking its cleanup function.
func (template *Template) Clone(ctx context.Context) (*pgx.ConnConfig, func(context.Context) error, error) {
	name, err := newResourceName('c')
	if err != nil {
		return nil, nil, err
	}
	statement := "CREATE DATABASE " + pgx.Identifier{name}.Sanitize() + " TEMPLATE " +
		pgx.Identifier{template.name}.Sanitize() + " STRATEGY WAL_LOG ALLOW_CONNECTIONS true"
	drop := registerResource(name, func(ctx context.Context) error {
		return dropDatabase(ctx, template.admin, template.maintenance, name)
	})
	createErr := func() error {
		select {
		case template.cloneGate <- struct{}{}:
			defer func() { <-template.cloneGate }()
		case <-ctx.Done():
			return ctx.Err()
		}
		_, err := template.admin.ExecContext(ctx, statement)
		return err
	}()
	if err := createErr; err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		return nil, nil, errors.Join(fmt.Errorf("clone PostgreSQL test template: %w", err), drop(cleanupCtx))
	}
	config := template.config.Copy()
	config.Database = name
	return config, drop, nil
}

func templateStatement(ctx context.Context, admin *sql.DB, name string) (string, error) {
	var encoding, provider, collate, ctype, locale, rules string
	if err := admin.QueryRowContext(ctx, `SELECT pg_encoding_to_char(encoding), datlocprovider::text,
		datcollate, datctype, COALESCE(datlocale, ''), COALESCE(daticurules, '')
		FROM pg_database WHERE datname = current_database()`).Scan(&encoding, &provider, &collate, &ctype, &locale, &rules); err != nil {
		return "", err
	}
	statement := "CREATE DATABASE " + pgx.Identifier{name}.Sanitize() + " TEMPLATE template0 STRATEGY WAL_LOG ENCODING " + sqlLiteral(encoding)
	switch provider {
	case "c":
		statement += " LOCALE_PROVIDER libc LC_COLLATE " + sqlLiteral(collate) + " LC_CTYPE " + sqlLiteral(ctype)
	case "b":
		statement += " LOCALE_PROVIDER builtin BUILTIN_LOCALE " + sqlLiteral(locale)
	case "i":
		statement += " LOCALE_PROVIDER icu ICU_LOCALE " + sqlLiteral(locale) + " ICU_RULES " + sqlLiteral(rules) +
			" LC_COLLATE " + sqlLiteral(collate) + " LC_CTYPE " + sqlLiteral(ctype)
	default:
		return "", fmt.Errorf("unsupported PostgreSQL locale provider %q", provider)
	}
	return statement, nil
}

func sqlLiteral(value string) string {
	return "E'" + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), "'", "''") + "'"
}
