from pg_nearest_city.scripts.generate_border_city_center_points import (
    build_point_rows,
)


def test_build_point_rows_deduplicates_exact_city_centers():
    rows = [
        {
            "pair": "AE/OM",
            "country_a": "AE",
            "country_b": "OM",
            "country_name_a": "United Arab Emirates",
            "country_name_b": "Oman",
            "city_a": "Murbah",
            "city_b": "Madha",
            "lat_a": "25.27623",
            "lon_a": "56.36256",
            "lat_b": "25.28345",
            "lon_b": "56.33280",
            "distance_m": "3102.3",
            "seam_length_m": "567311.7",
        },
        {
            "pair": "AE/OM",
            "country_a": "AE",
            "country_b": "OM",
            "country_name_a": "United Arab Emirates",
            "country_name_b": "Oman",
            "city_a": "Murbah",
            "city_b": "Madha 2",
            "lat_a": "25.27623",
            "lon_a": "56.36256",
            "lat_b": "25.28357",
            "lon_b": "56.33196",
            "distance_m": "3187.5",
            "seam_length_m": "567311.7",
        },
    ]

    point_rows = build_point_rows(rows)

    assert len(point_rows) == 3
    assert point_rows[0]["city"] == "Murbah"
    assert point_rows[0]["source_pair_count"] == 2
    assert point_rows[0]["point_id"] == "border-city-center-00001"
    assert point_rows[1]["city"] == "Madha"
    assert point_rows[2]["city"] == "Madha 2"


def test_build_point_rows_can_keep_duplicates():
    rows = [
        {
            "pair": "AL/GR",
            "country_a": "AL",
            "country_b": "GR",
            "country_name_a": "Albania",
            "country_name_b": "Greece",
            "city_a": "Konispol",
            "city_b": "Sagiada",
            "lat_a": "39.65889",
            "lon_a": "20.18139",
            "lat_b": "39.62333",
            "lon_b": "20.19433",
            "distance_m": "4101.4",
            "seam_length_m": "218983.1",
        },
        {
            "pair": "AL/GR",
            "country_a": "AL",
            "country_b": "GR",
            "country_name_a": "Albania",
            "country_name_b": "Greece",
            "city_a": "Konispol",
            "city_b": "Sagiada",
            "lat_a": "39.65889",
            "lon_a": "20.18139",
            "lat_b": "39.62333",
            "lon_b": "20.19433",
            "distance_m": "4101.4",
            "seam_length_m": "218983.1",
        },
    ]

    point_rows = build_point_rows(rows, keep_duplicates=True)

    assert len(point_rows) == 4
    assert point_rows[0]["source_pair_count"] == 1
    assert point_rows[2]["city"] == "Konispol"
