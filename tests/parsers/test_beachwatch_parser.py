from opal_ocean.ingest.parsers import UnknownStatusError, parse_beachwatch_status


def test_parse_beachwatch_status_normalises_case():
    assert parse_beachwatch_status(" very GOOD ") == "Very good"


def test_parse_beachwatch_status_unknown_value():
    try:
        parse_beachwatch_status("mystery")
    except UnknownStatusError as exc:
        assert "Unknown Beachwatch status" in str(exc)
    else:
        raise AssertionError("Expected UnknownStatusError")
