"""Settings for DB-related tasks."""

from __future__ import annotations

import os
from dataclasses import dataclass, replace


@dataclass(frozen=True)
class DBConnSettings:
    """Postgres connection settings."""

    host: str = os.environ.get("PGNEAREST_DB_HOST", "localhost")
    name: str = os.environ.get("PGNEAREST_DB_NAME", "postgres")
    password: str = os.environ.get("PGNEAREST_DB_PASSWORD", "postgres")
    port: int = int(os.environ.get("PGNEAREST_DB_PORT", "5432"))
    user: str = os.environ.get("PGNEAREST_DB_USER", "postgres")

    def with_overrides(self, **kwargs) -> DBConnSettings:
        """Return a new instance with the given fields overridden."""
        return replace(self, **kwargs)

    @property
    def conn_string(self) -> str:
        """Generate PostgreSQL connection string."""
        return (
            f"postgresql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.name}"
        )


@dataclass
class DBConfigSetting:
    """Postgres configuration settings."""

    name: str
    value: str | int

    def __post_init__(self):
        if isinstance(self.value, str):
            self.value = f"'{self.value}'"


init_db_settings: list[DBConfigSetting] = [
    DBConfigSetting(name="jit", value="off"),
    DBConfigSetting(name="maintenance_work_mem", value="1GB"),
    DBConfigSetting(name="work_mem", value="1GB"),
    DBConfigSetting(name="lock_timeout", value=0),
    DBConfigSetting(name="statement_timeout", value=0),
    DBConfigSetting(name="transaction_timeout", value=0),
]

MAX_WORKERS: int = 8
