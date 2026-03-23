from pg_nearest_city.datasets.sources import GEONAMES_URLCONFIG, OVERPASS_URLCONFIG
from pg_nearest_city.datasets.types import (
    OverpassItemType,
    OverpassQL,
    OverpassQueryBuilder,
    OverpassType,
)
from pg_nearest_city.datasets.url_config import URLConfig

op = OverpassQL(
    item_type=[OverpassItemType.DEFAULT_INPUT, OverpassItemType.RECURSE_DOWN],
    queries=[OverpassQueryBuilder(OverpassType.REL).name("foobar").build()],
)


def test_urlconfig_parsing_no_suffix_passes():
    conf = OVERPASS_URLCONFIG
    assert conf.filename == "interpreter"
    assert conf.slug == "interpreter"


def test_urlconfig_parsing_single_suffix_passes():
    conf = GEONAMES_URLCONFIG
    assert conf.filename == "cities500.zip"
    assert conf.slug == "cities500.zip"


def test_urlconfig_parsing_two_suffixes_passes():
    conf = URLConfig(url="https://x/y/some_slug.tar.gz")
    assert conf.filename == "some_slug.tar.gz"
    assert conf.slug == "some_slug.tar.gz"


def test_urlconfig_parsing_three_suffixes_passes():
    conf = URLConfig(url="https://x/y/some_slug.final.tar.gz")
    assert conf.filename == "some_slug.final.tar.gz"
    assert conf.slug == "some_slug.final.tar.gz"


def test_urlconfig_parsing_duplicate_suffixes_passes():
    conf = URLConfig(url="https://x/y/some_slug.tar.tar.gz")
    assert conf.filename == "some_slug.tar.tar.gz"
    assert conf.slug == "some_slug.tar.tar.gz"


def test_urlconfig_parsing_suffix_override_passes():
    conf = OVERPASS_URLCONFIG.with_overrides(target_filetype=op.filetype)
    assert conf.filename == "interpreter.osm"
    assert conf.slug == "interpreter"


def test_urlconfig_parsing_filename_override_passes():
    conf = OVERPASS_URLCONFIG.with_overrides(target_filename="foobar")
    assert conf.filename == "foobar"
    assert conf.slug == "interpreter"


def test_urlconfig_parsing_multiple_overrides_passes():
    conf = GEONAMES_URLCONFIG.with_overrides(target_filename="foobar")
    assert conf.filename == "foobar.zip"
    assert conf.slug == "cities500.zip"


def test_stem_with_dot_passes():
    conf = URLConfig(url="https://x/y/report.final.csv")
    assert conf.filename == "report.final.csv"
    assert conf.slug == "report.final.csv"


def test_stem_with_multiple_dots_passes():
    conf = URLConfig(url="https://x/y/report.final.v2.tar.gz")
    assert conf.filename == "report.final.v2.tar.gz"
    assert conf.slug == "report.final.v2.tar.gz"


def test_slug_with_dots_in_stem_is_supported():
    conf = URLConfig(url="https://x/y/report.final.v2.csv")
    assert conf.filename == "report.final.v2.csv"
    assert conf.slug == "report.final.v2.csv"


def test_trailing_slash_does_not_break_slug():
    conf = URLConfig(url="https://x/y/report.tar.gz/")
    assert conf.filename == "report.tar.gz"
    assert conf.slug == "report.tar.gz"
