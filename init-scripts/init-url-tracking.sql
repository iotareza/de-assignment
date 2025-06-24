-- Initialize URL tracking table for pre-extract filter
-- This table tracks URLs that have been processed for each stock to avoid duplicates

CREATE TABLE IF NOT EXISTS url_tracking (
    id SERIAL PRIMARY KEY,
    stock_name VARCHAR(100) NOT NULL,
    url TEXT NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_name, url)
);

-- Create index on stock_name and url for faster lookups
CREATE INDEX IF NOT EXISTS idx_url_tracking_stock_url ON url_tracking(stock_name, url);

-- Create index on processed_at for potential cleanup operations
CREATE INDEX IF NOT EXISTS idx_url_tracking_processed_at ON url_tracking(processed_at);

-- Create index for stock name lookups
CREATE INDEX IF NOT EXISTS idx_url_tracking_stock_name ON url_tracking(stock_name);

-- Add comment to table
COMMENT ON TABLE url_tracking IS 'Tracks URLs that have been processed for each stock to avoid duplicate extraction';
COMMENT ON COLUMN url_tracking.stock_name IS 'Name of the stock (e.g., HDFC, Tata Motors)';
COMMENT ON COLUMN url_tracking.url IS 'The URL that was processed';
COMMENT ON COLUMN url_tracking.processed_at IS 'Timestamp when the URL was marked as processed';

-- Insert some sample data for testing (optional)
-- INSERT INTO url_tracking (stock_name, url) VALUES 
-- ('HDFC', 'https://example.com/article1'),
-- ('Tata Motors', 'https://example.com/article2')
-- ON CONFLICT (stock_name, url) DO NOTHING; 