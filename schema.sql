CREATE TABLE IF NOT EXISTS users (
 id BIGINT PRIMARY KEY,
 username TEXT,
 first_name TEXT,
 last_name TEXT,
 language TEXT NOT NULL DEFAULT 'en',
 photo_url TEXT,
 ton_address TEXT,
 usdt_address TEXT,
 card_requisite TEXT,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS balances (
 user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
 currency TEXT NOT NULL,
 balance NUMERIC(30,8) NOT NULL DEFAULT 0 CHECK(balance >= 0),
 PRIMARY KEY(user_id,currency)
);

CREATE TABLE IF NOT EXISTS deals (
 id BIGSERIAL PRIMARY KEY,
 tag TEXT UNIQUE NOT NULL,
 deal_type TEXT NOT NULL CHECK(deal_type IN ('sell','buy')),
 creator_id BIGINT REFERENCES users(id),
 user_id BIGINT REFERENCES users(id),
 seller_id BIGINT REFERENCES users(id),
 buyer_id BIGINT REFERENCES users(id),
 seller_username TEXT,
 seller_first_name TEXT,
 buyer_username TEXT,
 buyer_first_name TEXT,
 user_username TEXT,
 user_first_name TEXT,
 amount NUMERIC(30,8) NOT NULL CHECK(amount > 0),
 currency TEXT NOT NULL,
 fee NUMERIC(30,8) NOT NULL DEFAULT 0,
 description TEXT NOT NULL DEFAULT '',
 status TEXT NOT NULL DEFAULT 'wait_payment' CHECK(status IN ('wait_payment','paid','sent','completed','cancelled','disputed')),
 escrow_status TEXT NOT NULL DEFAULT 'none' CHECK(escrow_status IN ('none','locked','released','cancelled_locked')),
 escrow_amount NUMERIC(30,8) NOT NULL DEFAULT 0,
 escrow_currency TEXT,
 escrow_account TEXT,
 payment_currency TEXT,
 payment_amount NUMERIC(30,8) NOT NULL DEFAULT 0,
 payment_fee NUMERIC(30,8) NOT NULL DEFAULT 0,
 dispute_id BIGINT,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 joined_at TIMESTAMPTZ,
 paid_at TIMESTAMPTZ,
 sent_at TIMESTAMPTZ,
 completed_at TIMESTAMPTZ,
 cancelled_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS deals_seller_idx ON deals(seller_id);
CREATE INDEX IF NOT EXISTS deals_buyer_idx ON deals(buyer_id);
CREATE INDEX IF NOT EXISTS deals_creator_idx ON deals(creator_id);
CREATE INDEX IF NOT EXISTS deals_status_idx ON deals(status);
CREATE INDEX IF NOT EXISTS deals_created_idx ON deals(created_at DESC);

CREATE TABLE IF NOT EXISTS transactions (
 id BIGSERIAL PRIMARY KEY,
 user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 type TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'completed',
 currency TEXT NOT NULL,
 amount NUMERIC(30,8) NOT NULL,
 description TEXT,
 deal_tag TEXT,
 metadata JSONB NOT NULL DEFAULT '{}',
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS transactions_user_idx ON transactions(user_id,created_at DESC);
CREATE INDEX IF NOT EXISTS transactions_created_idx ON transactions(created_at DESC);

CREATE TABLE IF NOT EXISTS deposit_requests (
 id BIGSERIAL PRIMARY KEY,
 user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 currency TEXT NOT NULL,
 amount NUMERIC(30,8) NOT NULL,
 comment TEXT,
 tx_hash TEXT,
 network TEXT,
 from_address TEXT,
 to_address TEXT,
 status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','processing','completed','rejected')),
 raw_data JSONB NOT NULL DEFAULT '{}',
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 processed_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS deposit_tx_hash_unique ON deposit_requests(currency,tx_hash) WHERE tx_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS deposits_status_idx ON deposit_requests(status,created_at);

CREATE TABLE IF NOT EXISTS withdrawal_requests (
 id BIGSERIAL PRIMARY KEY,
 user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 currency TEXT NOT NULL,
 amount NUMERIC(30,8) NOT NULL,
 method TEXT,
 destination TEXT,
 network TEXT,
 status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','processing','completed','rejected','cancelled')),
 tx_hash TEXT,
 raw_data JSONB NOT NULL DEFAULT '{}',
 client_request_id TEXT,
 comment TEXT,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 processed_at TIMESTAMPTZ,
 cancelled_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS withdrawals_status_idx ON withdrawal_requests(status,created_at);
CREATE UNIQUE INDEX IF NOT EXISTS withdrawal_tx_hash_unique ON withdrawal_requests(tx_hash) WHERE tx_hash IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS withdrawal_client_request_unique ON withdrawal_requests(user_id,client_request_id) WHERE client_request_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS user_requisites (
 user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 requisite_key TEXT NOT NULL,
 requisite_value TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(user_id,requisite_key)
);

CREATE TABLE IF NOT EXISTS admins (
 user_id BIGINT PRIMARY KEY,
 username TEXT,
 added_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS whitelist (
 user_id BIGINT PRIMARY KEY,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS banned_users (
 user_id BIGINT PRIMARY KEY,
 reason TEXT,
 banned_by BIGINT,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS escrow_accounts (
 phone TEXT PRIMARY KEY,
 first_name TEXT,
 username TEXT,
 active BOOLEAN NOT NULL DEFAULT FALSE,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS settings (
 key TEXT PRIMARY KEY,
 value JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_logs (
 id BIGSERIAL PRIMARY KEY,
 user_id BIGINT,
 action TEXT NOT NULL,
 entity_type TEXT,
 entity_id TEXT,
 metadata JSONB NOT NULL DEFAULT '{}',
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS audit_logs_created_idx ON audit_logs(created_at DESC);

CREATE TABLE IF NOT EXISTS broadcasts (
 id BIGSERIAL PRIMARY KEY,
 text TEXT NOT NULL,
 target TEXT NOT NULL,
 parse_mode TEXT,
 sent INT NOT NULL DEFAULT 0,
 failed INT NOT NULL DEFAULT 0,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS notifications (
 id BIGSERIAL PRIMARY KEY,
 user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 type TEXT NOT NULL,
 title TEXT,
 body TEXT NOT NULL,
 data JSONB NOT NULL DEFAULT '{}',
 telegram_sent BOOLEAN NOT NULL DEFAULT FALSE,
 read_at TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS notifications_user_idx ON notifications(user_id,created_at DESC);

CREATE TABLE IF NOT EXISTS disputes (
 id BIGSERIAL PRIMARY KEY,
 deal_id BIGINT NOT NULL UNIQUE REFERENCES deals(id) ON DELETE CASCADE,
 opened_by BIGINT NOT NULL REFERENCES users(id),
 reason TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','investigating','resolved','closed')),
 resolution TEXT,
 resolved_by BIGINT,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 resolved_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS disputes_status_idx ON disputes(status,created_at DESC);

CREATE TABLE IF NOT EXISTS dispute_messages (
 id BIGSERIAL PRIMARY KEY,
 dispute_id BIGINT NOT NULL REFERENCES disputes(id) ON DELETE CASCADE,
 user_id BIGINT NOT NULL REFERENCES users(id),
 message TEXT NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS dispute_messages_idx ON dispute_messages(dispute_id,created_at);

CREATE TABLE IF NOT EXISTS telegram_updates (
 update_id BIGINT PRIMARY KEY,
 received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS processed_chain_transactions (
 currency TEXT NOT NULL,
 network TEXT NOT NULL,
 tx_hash TEXT NOT NULL,
 user_id BIGINT REFERENCES users(id),
 amount NUMERIC(30,8) NOT NULL,
 raw_data JSONB NOT NULL DEFAULT '{}',
 processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 PRIMARY KEY(currency,network,tx_hash)
);


CREATE TABLE IF NOT EXISTS ledger_entries (
 id BIGSERIAL PRIMARY KEY,
 user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 currency TEXT NOT NULL,
 amount NUMERIC(30,8) NOT NULL,
 entry_type TEXT NOT NULL,
 description TEXT,
 reference_type TEXT,
 reference_id TEXT,
 metadata JSONB NOT NULL DEFAULT '{}',
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ledger_user_idx ON ledger_entries(user_id,created_at DESC);

CREATE TABLE IF NOT EXISTS idempotency_keys (
 key TEXT PRIMARY KEY,
 user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
 endpoint TEXT NOT NULL,
 response JSONB,
 status_code INT,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idempotency_user_idx ON idempotency_keys(user_id,created_at DESC);
