from __future__ import annotations

from popgetter.assets.uk.england_wales_census import retrieve_table_description


def test_retrieve_table_description():
    example_url = "https://www.nomisweb.co.uk/datasets/c2021ts009"
    expected_desction_snipet = "estimates that classify usual residents in England and Wales by sex and single year of age"
    actual_description = retrieve_table_description(example_url)

    assert (
        expected_desction_snipet in actual_description
    ), "The description of the table did not included the expected text"
