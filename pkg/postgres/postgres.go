package postgres

import (
	_ "embed"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

var (
	ErrNotFoundAggregateID        = errors.New("not found")
	ErrPostgresqlConfigNoHost     = errors.New("no postgres host config")
	ErrPostgresqlConfigNoPort     = errors.New("no postgres host port config")
	ErrPostgresqlConfigNoUser     = errors.New("no postgres username")
	ErrPostgresqlConfigNoPassword = errors.New("no postgres password")
	ErrPostgresqlConfigNoDBName   = errors.New("no postgres db name config")
	ErrPostgresqlNoEventAppended  = errors.New("no event was appended")
)

type PostgresConnctionInfo struct {
	Host   string
	Port   int
	User   string
	Pass   string
	DBname string
}

type PostgresConnctionInfoBuilder struct {
	info PostgresConnctionInfo
	errs []error
}

func (builder *PostgresConnctionInfoBuilder) WithHost() *PostgresConnctionInfoBuilder {
	pgHost := os.Getenv("PG_HOST")
	if pgHost != "" {
		builder.info.Host = pgHost
	} else {
		builder.errs = append(builder.errs, ErrPostgresqlConfigNoHost)
	}
	pgPort := os.Getenv("PG_PORT")
	if pgPort != "" {
		pgPortInt, err := strconv.Atoi(pgPort)
		if err != nil {
			builder.errs = append(builder.errs, err)
		}
		builder.info.Port = pgPortInt
	} else {
		builder.errs = append(builder.errs, ErrPostgresqlConfigNoPort)
	}
	return builder
}

func (builder *PostgresConnctionInfoBuilder) WithUserAndPass() *PostgresConnctionInfoBuilder {
	pgUser := os.Getenv("PG_USER")
	if pgUser != "" {
		builder.info.User = pgUser
	} else {
		builder.errs = append(builder.errs, ErrPostgresqlConfigNoUser)
	}
	pgPass := os.Getenv("PG_PASS")
	if pgPass != "" {
		builder.info.Pass = pgPass
	} else {
		builder.errs = append(builder.errs, ErrPostgresqlConfigNoPassword)
	}
	return builder
}

func (builder *PostgresConnctionInfoBuilder) WithDBName() *PostgresConnctionInfoBuilder {
	pgDBName := os.Getenv("PG_DBNAME")
	if pgDBName != "" {
		builder.info.DBname = pgDBName
	} else {
		builder.errs = append(builder.errs, ErrPostgresqlConfigNoDBName)
	}
	return builder
}

func (builder *PostgresConnctionInfoBuilder) Build() (PostgresConnctionInfo, error) {
	if len(builder.errs) > 0 {
		return PostgresConnctionInfo{}, builder.errs[0]
	}
	return builder.info, nil
}

func NewPostgresqlRepository() (*sqlx.DB, error) {
	pcib := PostgresConnctionInfoBuilder{}

	connectionInfo, err := pcib.WithHost().WithUserAndPass().WithDBName().Build()
	if err != nil {
		return nil, err
	}
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		connectionInfo.Host,
		connectionInfo.Port,
		connectionInfo.User,
		connectionInfo.Pass,
		connectionInfo.DBname,
	)

	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}
