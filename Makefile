# Include environment files in order of preference.
# .env.local overrides .env if both exist.
-include .env
-include .env.local

# Workspace Paths
STORAGE_POSTGRES_DIR := crates/storage-postgres
STORAGE_MYSQL_DIR    := crates/storage-mysql
STORAGE_SQLITE_DIR   := crates/storage-sqlite

# Default DB Connection Parameters (overridable via environment or .env)
MYSQL_USER        ?= root
MYSQL_PASSWORD    ?= root
MYSQL_HOST        ?= 127.0.0.1
MYSQL_PORT        ?= 3306
MYSQL_DB          ?= miranda

POSTGRES_USER     ?= postgres
POSTGRES_PASSWORD ?= postgres
POSTGRES_HOST     ?= 127.0.0.1
POSTGRES_PORT     ?= 5432
POSTGRES_DB       ?= miranda

SQLITE_FILE       ?= miranda.sqlite

# Dynamic Database URLs (respects exported DATABASE_URL_* variables if present)
DATABASE_URL_MYSQL    ?= mysql://$(MYSQL_USER):$(MYSQL_PASSWORD)@$(MYSQL_HOST):$(MYSQL_PORT)/$(MYSQL_DB)
DATABASE_URL_POSTGRES ?= postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(POSTGRES_HOST):$(POSTGRES_PORT)/$(POSTGRES_DB)
DATABASE_URL_SQLITE   ?= sqlite://$(CURDIR)/$(STORAGE_SQLITE_DIR)/$(SQLITE_FILE)?mode=rwc

define enter_dir
if [ "$$(basename "$$PWD")" != "$(notdir $(1))" ]; then cd $(1); fi
endef

.PHONY: db-migrate-mysql db-migrate-postgres db-migrate-sqlite db-migrate-all \
        sqlx-prepare-mysql sqlx-prepare-postgres sqlx-prepare-sqlite sqlx-prepare-all \
        db-clean

db-migrate-mysql:
	@$(call enter_dir,$(STORAGE_MYSQL_DIR)) && DATABASE_URL="$(DATABASE_URL_MYSQL)" sqlx database create
	@$(call enter_dir,$(STORAGE_MYSQL_DIR)) && DATABASE_URL="$(DATABASE_URL_MYSQL)" sqlx migrate run --source ./migrations

db-migrate-postgres:
	@$(call enter_dir,$(STORAGE_POSTGRES_DIR)) && DATABASE_URL="$(DATABASE_URL_POSTGRES)" sqlx database create
	@$(call enter_dir,$(STORAGE_POSTGRES_DIR)) && DATABASE_URL="$(DATABASE_URL_POSTGRES)" sqlx migrate run --source ./migrations

db-migrate-sqlite:
	@$(call enter_dir,$(STORAGE_SQLITE_DIR)) && DATABASE_URL="$(DATABASE_URL_SQLITE)" sqlx database create
	@$(call enter_dir,$(STORAGE_SQLITE_DIR)) && DATABASE_URL="$(DATABASE_URL_SQLITE)" sqlx migrate run --source ./migrations

db-migrate-all: db-migrate-mysql db-migrate-postgres db-migrate-sqlite

sqlx-prepare-mysql: db-migrate-mysql
	@$(call enter_dir,$(STORAGE_MYSQL_DIR)) && DATABASE_URL="$(DATABASE_URL_MYSQL)" cargo sqlx prepare

sqlx-prepare-postgres: db-migrate-postgres
	@$(call enter_dir,$(STORAGE_POSTGRES_DIR)) && DATABASE_URL="$(DATABASE_URL_POSTGRES)" cargo sqlx prepare

sqlx-prepare-sqlite: db-migrate-sqlite
	@$(call enter_dir,$(STORAGE_SQLITE_DIR)) && DATABASE_URL="$(DATABASE_URL_SQLITE)" cargo sqlx prepare

sqlx-prepare-all: sqlx-prepare-mysql sqlx-prepare-postgres sqlx-prepare-sqlite

db-clean:
	rm -f $(STORAGE_SQLITE_DIR)/$(SQLITE_FILE) $(STORAGE_SQLITE_DIR)/$(SQLITE_FILE)-shm $(STORAGE_SQLITE_DIR)/$(SQLITE_FILE)-wal
