import json
import pathlib
from os import path
from unittest.mock import Mock, call, patch

import pytest
from elena.adapters.config.local_config_manager import LocalConfigManager
from elena.domain.model.bot_budget import BotBudget
from elena.domain.model.bot_status import BotStatus
from elena.domain.model.order import Order, OrderSide, OrderType
from elena.domain.model.trading_pair import TradingPair
from elena.domain.ports.storage_manager import StorageError
from elena.shared.dynamic_loading import get_instance

bot_status = BotStatus(
    bot_id="test_bot_id",
    timestamp=1703944135288,
    budget=BotBudget(
        set_limit=1000.0,
        current_limit=999.0,
        used=77.0,
        pct_reinvest_profit=10.0,
    ),
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


def test_append_metric(storage_manager):
    try:
        storage_manager._delete_file('Metric/test_append_metric_bot/240119.jsonl')
    except StorageError:
        pass

    with patch("elena.adapters.storage_manager.file_storage_manager.time") as mocked_datetime:
        mocked_datetime.time.return_value = 1705685253
        mocked_datetime.strftime.return_value = "240119"
        storage_manager.append_metric(
            bot_id="test_append_metric_bot",
            metric_name="OrderCancelled",
            metric_type="counter",
            value=1,
            tags=["tag1:abc", "tag2:def"],
        )
        storage_manager.append_metric(
            bot_id="test_append_metric_bot",
            metric_name="OrderCancelled",
            metric_type="gauge",
            value=11,
            tags=["tag1:abc", "tag2:def"],
        )

    actual = storage_manager._load_file('Metric/test_append_metric_bot/240119.jsonl')

    lines = actual.splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0]) == {
        "timestamp": 1705685253000,
        "bot_id": "test_append_metric_bot",
        "metric_name": "OrderCancelled",
        "metric_type": "counter",
        "value": 1,
        "tags": "tag1:abc#tag2:def",
    }
    assert json.loads(lines[1]) == {
        "timestamp": 1705685253000,
        "bot_id": "test_append_metric_bot",
        "metric_name": "OrderCancelled",
        "metric_type": "gauge",
        "value": 11,
        "tags": "tag1:abc#tag2:def",
    }
