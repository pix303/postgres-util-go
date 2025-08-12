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
	NotFoundAggregateID             = errors.New("not found")
	PostgresqlConfigNoHostError     = errors.New("no postgres host config")
	PostgresqlConfigNoPortError     = errors.New("no postgres host port config")
	PostgresqlConfigNoUserError     = errors.New("no postgres username")
	PostgresqlConfigNoPasswordError = errors.New("no postgres password")
	PostgresqlConfigNoDBNameError   = errors.New("no postgres db name config")
	PostgresqlNoEventAppendedError  = errors.New("no event was appended")
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

func (this *PostgresConnctionInfoBuilder) WithHost() *PostgresConnctionInfoBuilder {
	pgHost := os.Getenv("PG_HOST")
	if pgHost != "" {
		this.info.Host = pgHost
	} else {
		this.errs = append(this.errs, PostgresqlConfigNoHostError)
	}
	pgPort := os.Getenv("PG_PORT")
	if pgPort != "" {
		pgPortInt, err := strconv.Atoi(pgPort)
		if err != nil {
			this.errs = append(this.errs, err)
		}
		this.info.Port = pgPortInt
	} else {
		this.errs = append(this.errs, PostgresqlConfigNoPortError)
	}
	return this
}

func (this *PostgresConnctionInfoBuilder) WithUserAndPass() *PostgresConnctionInfoBuilder {
	pgUser := os.Getenv("PG_USER")
	if pgUser != "" {
		this.info.User = pgUser
	} else {
		this.errs = append(this.errs, PostgresqlConfigNoUserError)
	}
	pgPass := os.Getenv("PG_PASS")
	if pgPass != "" {
		this.info.Pass = pgPass
	} else {
		this.errs = append(this.errs, PostgresqlConfigNoPasswordError)
	}
	return this
}

func (this *PostgresConnctionInfoBuilder) WithDBName() *PostgresConnctionInfoBuilder {
	pgDBName := os.Getenv("PG_DBNAME")
	if pgDBName != "" {
		this.info.DBname = pgDBName
	} else {
		this.errs = append(this.errs, PostgresqlConfigNoDBNameError)
	}
	return this
}

func (this *PostgresConnctionInfoBuilder) Build() (PostgresConnctionInfo, error) {
	if len(this.errs) > 0 {
		return PostgresConnctionInfo{}, this.errs[0]
	}
	return this.info, nil
}

func NewPostgresqlRepository() (*PostgresRepository, error) {
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

	pr := PostgresRepository{
		DB: db,
	}

	return &pr, nil
}

type PostgresRepository struct {
	DB *sqlx.DB
}
