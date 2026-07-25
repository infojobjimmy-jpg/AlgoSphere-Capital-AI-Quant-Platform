CREATE TABLE IF NOT EXISTS social_profiles (
    member_id TEXT PRIMARY KEY,
    invite_code TEXT UNIQUE NOT NULL,
    display_name TEXT NOT NULL DEFAULT 'Explorer',
    avatar TEXT NOT NULL DEFAULT '🧭',
    bio TEXT NOT NULL DEFAULT '',
    city TEXT NOT NULL DEFAULT '',
    country TEXT NOT NULL DEFAULT '',
    intent TEXT NOT NULL DEFAULT 'community',
    discoverable BOOLEAN NOT NULL DEFAULT FALSE,
    points INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS social_connections (
    owner_id TEXT NOT NULL REFERENCES social_profiles(member_id) ON DELETE CASCADE,
    contact_id TEXT NOT NULL REFERENCES social_profiles(member_id) ON DELETE CASCADE,
    label TEXT NOT NULL DEFAULT 'friends',
    status TEXT NOT NULL DEFAULT 'accepted',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (owner_id, contact_id)
);

CREATE TABLE IF NOT EXISTS social_presence (
    member_id TEXT PRIMARY KEY REFERENCES social_profiles(member_id) ON DELETE CASCADE,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS social_messages (
    id BIGSERIAL PRIMARY KEY,
    member_id TEXT NOT NULL REFERENCES social_profiles(member_id) ON DELETE CASCADE,
    room TEXT NOT NULL DEFAULT 'global',
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS social_messages_room_id_idx ON social_messages(room, id DESC);
CREATE INDEX IF NOT EXISTS social_profiles_discoverable_idx ON social_profiles(discoverable, updated_at DESC);
