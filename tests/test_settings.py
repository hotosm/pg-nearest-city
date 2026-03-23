"""Tests for pg_nearest_city.db.settings."""

from pg_nearest_city.db.settings import DBConfigSetting, DBConnSettings


class TestDBConnSettings:
    def test_default_conn_string(self):
        s = DBConnSettings()
        cs = s.conn_string
        assert "postgresql://" in cs
        assert s.host in cs
        assert str(s.port) in cs

    def test_with_overrides(self):
        s = DBConnSettings()
        s2 = s.with_overrides(host="remotehost", port=9999)
        assert s2.host == "remotehost"
        assert s2.port == 9999
        # original unchanged
        assert s.host != "remotehost"

    def test_frozen(self):
        s = DBConnSettings()
        import dataclasses

        with __import__("pytest").raises(dataclasses.FrozenInstanceError):
            s.host = "x"  # type: ignore[invalid-assignment]


class TestDBConfigSetting:
    def test_string_value_quoted(self):
        s = DBConfigSetting(name="jit", value="off")
        assert s.value == "'off'"

    def test_int_value_unchanged(self):
        s = DBConfigSetting(name="lock_timeout", value=0)
        assert s.value == 0
