-- Create database schema for storing trading signals and market data
CREATE TABLE IF NOT EXISTS trading_signals (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    signal_timestamp TIMESTAMP NOT NULL,
    open_price DECIMAL(10,2) NOT NULL,
    high_price DECIMAL(10,2) NOT NULL,
    low_price DECIMAL(10,2) NOT NULL,
    close_price DECIMAL(10,2) NOT NULL,
    volume BIGINT NOT NULL,
    signal VARCHAR(10) NOT NULL CHECK (signal IN ('BUY', 'SELL', 'HOLD')),
    signal_strength VARCHAR(20) NOT NULL,
    volume_ratio DECIMAL(5,2),
    price_change_pct DECIMAL(5,2),
    stop_loss_price DECIMAL(10,2),
    take_profit_price DECIMAL(10,2),
    risk_reward_ratio DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for efficient querying
CREATE INDEX idx_trading_signals_symbol_timestamp ON trading_signals(symbol, timestamp);
CREATE INDEX idx_trading_signals_signal ON trading_signals(signal);
CREATE INDEX idx_trading_signals_created_at ON trading_signals(created_at);

-- Create table for raw stock data
CREATE TABLE IF NOT EXISTS stock_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open_price DECIMAL(10,2) NOT NULL,
    high_price DECIMAL(10,2) NOT NULL,
    low_price DECIMAL(10,2) NOT NULL,
    close_price DECIMAL(10,2) NOT NULL,
    volume BIGINT NOT NULL,
    fetch_time TIMESTAMP NOT NULL,
    processing_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_stock_data_symbol_timestamp ON stock_data(symbol, timestamp);
