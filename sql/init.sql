CREATE TABLE IF NOT EXISTS events(
    event_id UUID PRIMARY KEY,
    user_id  INT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('view','add_to_cart','purchase')),
    product_id INT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_time ON events (event_time);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
