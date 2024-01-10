import pathlib
from os import path
from unittest.mock import Mock, call

import pandas as pd
import pytest
from elena.adapters.config.local_config_manager import LocalConfigManager
from elena.domain.model.bot_status import BotStatus
from elena.domain.model.order import Order, OrderSide, OrderType
from elena.domain.model.trading_pair import TradingPair
from elena.domain.ports.storage_manager import StorageError
from elena.shared.dynamic_loading import get_instance
from pandas import DataFrame

bot_status = BotStatus(
    bot_id="test_bot_id",
    timestamp=1703944135288,
    active_orders=[
        Order(
            id="3448098",
            exchange_id="binance",
            bot_id="Exchange_Test_Ops_BTC_USDT",
            strategy_id="ExchangeBasicOperationsTest-1",
            pair=TradingPair(base="BTC", quote="USDT"),
            timestamp=1702485175960,
            type=OrderType.stop_loss_limit,
            side=OrderSide.sell,
            price=31972.77,
            amount=0.00945,
            cost=0.0,
            average=None,
            filled=0.0,
            remaining=0.00945,
            status="canceled",
            fee=None,
            trigger_price=33655.54,
            stop_price=33655.54,
            take_profit_price=None,
            stop_loss_price=None,
        )
    ],
    archived_orders=[
        Order(
            id="3448099",
            exchange_id="binance",
            bot_id="Exchange_Test_Ops_BTC_USDT",
            strategy_id="ExchangeBasicOperationsTest-1",
            pair=TradingPair(base="BTC", quote="USDT"),
            timestamp=1702485175960,
            type=OrderType.stop_loss_limit,
            side=OrderSide.sell,
            price=31972.77,
            amount=0.00945,
            cost=0.0,
            average=None,
            filled=0.0,
            remaining=0.00945,
            status="canceled",
            fee=None,
            trigger_price=33655.54,
            stop_price=33655.54,
            take_profit_price=None,
            stop_loss_price=None,
        )
    ],
    active_trades=[],
    closed_trades=[],
)

data_frame = DataFrame(
    {
        "Name": {
            0: "Person_1",
            1: "Person_2",
            2: "Person_3",
            3: "Person_4",
            4: "Person_5",
            5: "Person_6",
            6: "Person_7",
            7: "Person_8",
            8: "Person_9",
            9: "Person_10",
        },
        "Age": {
            0: 56,
            1: 46,
            2: 32,
            3: 25,
            4: 38,
            5: 56,
            6: 36,
            7: 40,
            8: 28,
            9: 28,
        },
        "City": {
            0: "City_A",
            1: "City_B",
            2: "City_C",
            3: "City_A",
            4: "City_B",
            5: "City_C",
            6: "City_A",
            7: "City_B",
            8: "City_C",
            9: "City_A",
        },
    }
)


@pytest.fixture
def logger():
    return Mock()


@pytest.fixture
def storage_manager(logger):
    test_home_dir = path.join(pathlib.Path(__file__).parent.parent.parent, "test_home")
    config_manager = LocalConfigManager()
    config_manager.init(url=test_home_dir)
    config = config_manager.get_config()

    sut = get_instance(config["StorageManager"]["class"])
    sut.init(config, logger)
    return sut


def test_save_and_load_bot_status(logger, storage_manager):
    storage_manager.save_bot_status(bot_status)

    actual = storage_manager.load_bot_status("test_bot_id")

    assert bot_status == actual
    assert logger.mock_calls == [
        call.info('Started S3 storage manager, working with bucket %s', 'elena.dev.p3.b0a5dc0528d6'),
        call.debug('Saving %s %s to storage: %s', 'BotStatus', 'test_bot_id', 'BotStatus/test_bot_id.json'),
        call.debug('Loading %s %s from storage: %s', 'BotStatus', 'test_bot_id', 'BotStatus/test_bot_id.json'),
    ]


def test_load_un_existing_bot_status(storage_manager):
    with pytest.raises(StorageError) as excinfo:
        storage_manager.load_bot_status("fake_test_bot_id")

    assert excinfo.value.args[0] == ("Error loading BotStatus fake_test_bot_id: "
                                     "An error occurred (NoSuchKey) when calling the GetObject operation: "
                                     "The specified key does not exist.")


def test_save_and_delete_bot_status(storage_manager):
    storage_manager.save_bot_status(bot_status)
    storage_manager.delete_bot_status("test_bot_id")

    with pytest.raises(StorageError) as excinfo:
        storage_manager.load_bot_status("test_bot_id")

    assert excinfo.value.args[0] == ("Error loading BotStatus test_bot_id: "
                                     "An error occurred (NoSuchKey) when calling the GetObject operation: "
                                     "The specified key does not exist.")


def test_save_and_load_data_frame(logger, storage_manager):
    storage_manager.save_data_frame("test_df_id", data_frame)

    actual = storage_manager.load_data_frame("test_df_id")

    pd.testing.assert_frame_equal(actual, data_frame)
    assert logger.mock_calls == [
        call.info('Started S3 storage manager, working with bucket %s', 'elena.dev.p3.b0a5dc0528d6'),
        call.debug('Loading %s %s from storage: %s', 'DataFrame', 'test_df_id', 'DataFrame/test_df_id.json'),
        call.debug('Saving %s %s to storage: %s', 'DataFrame', 'test_df_id', 'DataFrame/test_df_id.json'),
        call.debug('Loading %s %s from storage: %s', 'DataFrame', 'test_df_id', 'DataFrame/test_df_id.json'),
    ]


def test_load_un_existing_data_frame(storage_manager):
    with pytest.raises(StorageError) as excinfo:
        storage_manager.load_data_frame("fake_test_df_id")

    assert excinfo.value.args[0] == ("Error loading DataFrame fake_test_df_id: "
                                     "An error occurred (NoSuchKey) when calling the GetObject operation: "
                                     "The specified key does not exist.")


def test_save_and_delete_data_frame(storage_manager):
    storage_manager.save_data_frame("test_df_id", data_frame)
    storage_manager.delete_data_frame("test_df_id")

    with pytest.raises(StorageError) as excinfo:
        storage_manager.load_data_frame("test_df_id")

    assert excinfo.value.args[0] == ("Error loading DataFrame test_df_id: "
                                     "An error occurred (NoSuchKey) when calling the GetObject operation: "
                                     "The specified key does not exist.")
