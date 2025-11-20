from datetime import datetime, date

import pytest
from pyspark import Row

from lib.ConfigLoader import get_config
from lib.Utils import get_spark_session
from lib import DataLoader

@pytest.fixture(scope='session')
def spark():
    return get_spark_session("LOCAL")

@pytest.fixture(scope='session')
def expected_party_rows():
    return [
        Row(
            load_date=date(2022, 8, 2),
            account_id='6982391060',
            party_id='9823462810',
            relation_type='F-N',
            relation_start_date=datetime(2019, 7, 29, 6, 21, 32)
        ),
        Row(
            load_date=date(2022, 8, 2),
            account_id='6982391061',
            party_id='9823462811',
            relation_type='F-N',
            relation_start_date=datetime(2018, 8, 31, 5, 27, 22)
        ),
        Row(
            load_date=date(2022, 8, 2),
            account_id='6982391062',
            party_id='9823462812',
            relation_type='F-N',
            relation_start_date=datetime(2018, 8, 25, 15, 50, 29)
        ),
        Row(
            load_date=date(2022, 8, 2),
            account_id='6982391063',
            party_id='9823462813',
            relation_type='F-N',
            relation_start_date=datetime(2018, 5, 11, 7, 18, 23)
        ),
        Row(
            load_date=date(2022, 8, 2),
            account_id='6982391064',
            party_id='9823462814',
            relation_type='F-N',
            relation_start_date=datetime(2019, 6, 6, 14, 18, 12)
        ),
        Row(
            load_date=date(2022, 8, 2),
            account_id='6982391065',
            party_id='9823462815',
            relation_type='F-N',
            relation_start_date=datetime(2017, 5, 14, 2, 17, 32)
        ),
        Row(
            load_date=date(2022, 8, 2),
            account_id='6982391066',
            party_id='9823462816',
            relation_type='F-N',
            relation_start_date=datetime(2019, 5, 15, 10, 39, 29)
        ),
        Row(
            load_date=date(2022, 8, 2),
            account_id='6982391062',
            party_id='9823462817',
            relation_type='F-S',
            relation_start_date=datetime(2018, 8, 18, 4, 32, 17)
        ),
        Row(
            load_date=date(2022, 8, 2),
            account_id='6982391067',
            party_id='9823462818',
            relation_type='F-N',
            relation_start_date=datetime(2017, 11, 27, 1, 12)
        ),
        Row(
            load_date=date(2022, 8, 2),
            account_id='6982391067',
            party_id='9823462820',
            relation_type='F-S',
            relation_start_date=datetime(2017, 11, 20, 14, 18, 5)
        ),
        Row(
            load_date=date(2022, 8, 2),
            account_id='6982391067',
            party_id='9823462821',
            relation_type='F-S',
            relation_start_date=datetime(2018, 7, 19, 18, 56, 57)
        )
    ]

def test_blank_test(spark):
    print(spark.version)
    assert spark.version == "4.0.1"

def test_get_config():
    conf_local = get_config("LOCAL")
    conf_qa = get_config("QA")
    assert conf_local["kafka.topic"] == "sbdl_kafka_cloud"
    assert conf_qa["hive.database"] == "sbdl_db_qa"

def test_read_accounts(spark):
    accounts_df = DataLoader.read_accounts(spark, "LOCAL", False, None)
    assert accounts_df.count() == 9

def test_read_parties(spark, expected_party_rows):
    actual_party_rows = DataLoader.read_parties(spark, "LOCAL", False, None).collect()
    assert expected_party_rows == actual_party_rows
