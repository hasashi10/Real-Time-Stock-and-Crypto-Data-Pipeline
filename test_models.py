from datetime import datetime
import pytest
from pydantic import ValidationError
from consumer import TradeData, setup_database
from unittest.mock import MagicMock
def test_trade_data_happy_path():
    payload = {
        "symbol": "BTC/USD",
        "price": 65000.50,
        "timestamp": datetime.now(),
        "direction": "up",
        "percentage": 1.2
    }
    trade = TradeData(**payload)
    assert trade.symbol == "BTC/USD"
    assert trade.price == 65000.50

def test_trade_data_catches_negative_price():
    bad_payload = {
        "symbol": "ETH/USD",
        "price": -50.00,
        "timestamp": datetime.now(),
        "direction": "sell",
        "percentage": -0.5
    }
    with pytest.raises(ValidationError) as error_info:
        TradeData(**bad_payload)

    assert "suspicious price detected" in str(error_info.value)
def test_setup_database_executes_queries():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    setup_database(mock_conn)
    assert mock_cursor.execute.call_count == 4
    assert mock_conn.commit.call_count == 2