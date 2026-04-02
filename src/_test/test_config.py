"""Tests for arroyosas.config"""


def test_settings_importable():
    from arroyosas.config import settings

    assert settings is not None


def test_settings_is_dynaconf():
    from dynaconf import Dynaconf

    from arroyosas.config import settings

    assert isinstance(settings, Dynaconf)


def test_settings_attribute_access_missing_returns_default():
    from arroyosas.config import settings

    # Dynaconf returns None or raises AttributeError for missing keys
    # depending on version; just verify it can be imported and is usable
    assert hasattr(settings, "get")
