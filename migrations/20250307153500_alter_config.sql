-- SQLite does not support modifying check constraints in place.
-- To allow the new cluster_size and block_size values, the table has to be recreated;
CREATE TABLE new_config
(
    vbd_id       BLOB    NOT NULL CHECK (TYPEOF(vbd_id) == 'blob' AND
                                         LENGTH(vbd_id) == 16),
    cluster_size INTEGER NOT NULL CHECK (cluster_size IN (64, 128, 256)),
    block_size   INTEGER NOT NULL CHECK (block_size IN (16384, 65536, 262144, 1048576)),
    content_hash TEXT    NOT NULL CHECK (content_hash IN ('TENT', 'BLAKE3', 'XXH3_128')),
    meta_hash    TEXT    NOT NULL CHECK (content_hash IN ('TENT', 'BLAKE3', 'XXH3_128'))
);

INSERT INTO new_config
SELECT *
FROM config;

DROP TABLE config;
ALTER TABLE new_config
    RENAME TO config;

-- The previous triggers where dropped with the table and have to be recreated

CREATE TRIGGER prevent_multiple_configs
    BEFORE INSERT
    ON config
    WHEN (SELECT COUNT(*)
          FROM config) >= 1
BEGIN
    SELECT RAISE(ABORT, 'Only one config entry allowed');
END;

CREATE TRIGGER prevent_config_updates
    BEFORE UPDATE
    ON config
BEGIN
    SELECT RAISE(ABORT, 'Config is immutable');
END;

CREATE TRIGGER prevent_config_deletes
    BEFORE DELETE
    ON config
BEGIN
    SELECT RAISE(ABORT, 'Config cannot be deleted');
END;