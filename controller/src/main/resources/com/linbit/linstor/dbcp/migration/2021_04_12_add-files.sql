-- Create tables for Files
CREATE TABLE FILES (
    UUID CHARACTER(36) NOT NULL,
    PATH VARCHAR(1024) PRIMARY KEY, -- linux supports 4k path-len, mariadb 3k indexable column, but max url length is 2k :)
    FLAGS BIGINT NOT NULL,
    CONTENT BLOB NOT NULL,
    CONTENT_CHECKSUM VARCHAR(64) NOT NULL  -- sha256, returns 32 bytes but stored as hex version, thus 64 chars
);
