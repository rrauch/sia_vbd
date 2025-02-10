-- config is supposed to be immutable
-- Also, there can only be a single config at any time
CREATE TABLE config
(
    vbd_id       BLOB    NOT NULL CHECK (TYPEOF(vbd_id) == 'blob' AND
                                         LENGTH(vbd_id) == 16),
    cluster_size INTEGER NOT NULL CHECK (cluster_size == 256),
    block_size   INTEGER NOT NULL CHECK (block_size IN (16384, 65536, 262144)),
    content_hash TEXT    NOT NULL CHECK (content_hash IN ('TENT', 'BLAKE3', 'XXH3_128')),
    meta_hash    TEXT    NOT NULL CHECK (content_hash IN ('TENT', 'BLAKE3', 'XXH3_128'))
);

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

-- Keeps track of all known blocks, chunks & indices
-- Entries will be auto-deleted once they aren't referenced anymore
CREATE TABLE known_content
(
    content_type TEXT    NOT NULL CHECK (content_type IN ('B', 'C', 'I')),
    block_id     BLOB UNIQUE CHECK (block_id IS NULL OR (TYPEOF(block_id) == 'blob' AND
                                                         LENGTH(block_id) >= 16 AND
                                                         LENGTH(block_id) <= 32)),
    cluster_id   BLOB UNIQUE CHECK (cluster_id IS NULL OR (TYPEOF(cluster_id) == 'blob' AND
                                                           LENGTH(cluster_id) >= 16 AND
                                                           LENGTH(cluster_id) <= 32)),
    index_id     BLOB UNIQUE CHECK (index_id IS NULL OR (TYPEOF(index_id) == 'blob' AND
                                                         LENGTH(index_id) >= 16 AND
                                                         LENGTH(index_id) <= 32)),
    used         INTEGER NOT NULL CHECK (used >= 0),
    chunk_avail  INTEGER NOT NULL CHECK (chunk_avail >= 0),
    wal_avail    INTEGER NOT NULL CHECK (wal_avail >= 0),

    CHECK (
        (content_type = 'B' AND block_id IS NOT NULL AND cluster_id IS NULL AND index_id IS NULL) OR
        (content_type = 'C' AND cluster_id IS NOT NULL AND block_id IS NULL AND index_id IS NULL) OR
        (content_type = 'I' AND index_id IS NOT NULL AND block_id IS NULL AND cluster_id IS NULL)
        )
);

CREATE INDEX idx_known_content_content_type ON known_content (content_type);
CREATE INDEX idx_known_content_block_id ON known_content (block_id);
CREATE INDEX idx_known_content_cluster_id ON known_content (cluster_id);
CREATE INDEX idx_known_content_index_id ON known_content (index_id);

-- Prevent Id changes
CREATE TRIGGER prevent_known_blocks_id_update
    BEFORE UPDATE
    ON known_content
    FOR EACH ROW
    WHEN NEW.block_id != OLD.block_id
BEGIN
    SELECT RAISE(ABORT, 'Updates to block_id columns are not allowed.');
END;

CREATE TRIGGER prevent_known_cluster_id_update
    BEFORE UPDATE
    ON known_content
    FOR EACH ROW
    WHEN NEW.cluster_id != OLD.cluster_id
BEGIN
    SELECT RAISE(ABORT, 'Updates to cluster_id columns are not allowed.');
END;

CREATE TRIGGER prevent_known_indices_id_update
    BEFORE UPDATE
    ON known_content
    FOR EACH ROW
    WHEN NEW.index_id != OLD.index_id
BEGIN
    SELECT RAISE(ABORT, 'Updates to index_id columns are not allowed.');
END;

-- GC
CREATE TRIGGER delete_obsolete_known_content
    AFTER UPDATE
    ON known_content
    WHEN NEW.used = 0 AND NEW.chunk_avail = 0 AND NEW.wal_avail = 0
BEGIN
    DELETE FROM known_content WHERE ROWID = NEW.ROWID;
END;

CREATE TRIGGER adjust_wal_critical_after_insert
    AFTER INSERT
    ON known_content
    FOR EACH ROW
    WHEN (NEW.used > 0 AND NEW.chunk_avail = 0 AND NEW.wal_avail > 0)
BEGIN
    UPDATE wal_files
    SET critical = critical + 1
    WHERE id IN (SELECT DISTINCT ac.wal_id
                 FROM available_content ac
                 WHERE (
                           (ac.block_id = NEW.block_id) OR
                           (ac.cluster_id = NEW.cluster_id) OR
                           (ac.index_id = NEW.index_id)
                           ));
END;

CREATE TRIGGER adjust_wal_critical_after_update_activated
    AFTER UPDATE
    ON known_content
    FOR EACH ROW
    WHEN (
        NEW.used > 0 AND
        NEW.chunk_avail = 0 AND
        NEW.wal_avail > 0 AND
        (OLD.used IS NULL OR OLD.used = 0 OR OLD.chunk_avail > 0)
        )
BEGIN
    UPDATE wal_files
    SET critical = critical + 1
    WHERE id IN (SELECT DISTINCT ac.wal_id
                 FROM available_content ac
                 WHERE (
                           (ac.block_id = NEW.block_id) OR
                           (ac.cluster_id = NEW.cluster_id) OR
                           (ac.index_id = NEW.index_id)
                           ));
END;

CREATE TRIGGER adjust_wal_critical_after_update_deactivated
    AFTER UPDATE
    ON known_content
    FOR EACH ROW
    WHEN (
        (NEW.used = 0 OR
         NEW.chunk_avail > 0) AND
        NEW.wal_avail > 0 AND
        (OLD.used > 0 AND OLD.chunk_avail = 0)
        )
BEGIN
    UPDATE wal_files
    SET critical = critical - 1
    WHERE id IN (SELECT DISTINCT ac.wal_id
                 FROM available_content ac
                 WHERE (
                           (ac.block_id = NEW.block_id) OR
                           (ac.cluster_id = NEW.cluster_id) OR
                           (ac.index_id = NEW.index_id)
                           ));
END;


CREATE TABLE cluster_content
(
    cluster_id  BLOB    NOT NULL CHECK (TYPEOF(cluster_id) == 'blob' AND
                                        LENGTH(cluster_id) >= 16 AND
                                        LENGTH(cluster_id) <= 32),
    block_index INTEGER NOT NULL CHECK (block_index >= 0),
    block_id    BLOB    NOT NULL CHECK (TYPEOF(block_id) == 'blob' AND
                                        LENGTH(block_id) >= 16 AND
                                        LENGTH(block_id) <= 32),

    FOREIGN KEY (cluster_id) REFERENCES known_content (cluster_id) ON DELETE CASCADE,
    FOREIGN KEY (block_id) REFERENCES known_content (block_id),

    UNIQUE (cluster_id, block_index)
);

-- Prevent Updates
CREATE TRIGGER prevent_cluster_content_update
    BEFORE UPDATE
    ON cluster_content
    FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'Updates to cluster_content are not allowed.');
END;

-- Reference Counting
CREATE TRIGGER increment_used_counters_before_cluster_content_block_insert
    BEFORE INSERT
    ON cluster_content
BEGIN
    -- Upsert known blocks
    INSERT INTO known_content (content_type, block_id, used, chunk_avail, wal_avail)
    VALUES ('B', NEW.block_id, 1, 0, 0)
    ON CONFLICT(block_id) DO UPDATE SET used = used + 1;
END;

CREATE TRIGGER decrement_used_counters_after_cluster_content_delete
    AFTER DELETE
    ON cluster_content
BEGIN
    UPDATE known_content
    SET used = used - 1
    WHERE block_id = OLD.block_id;
END;


CREATE TABLE index_content
(
    index_id      BLOB    NOT NULL CHECK (TYPEOF(index_id) == 'blob' AND
                                          LENGTH(index_id) >= 16 AND
                                          LENGTH(index_id) <= 32),
    cluster_index INTEGER NOT NULL CHECK (cluster_index >= 0),
    cluster_id    BLOB    NOT NULL CHECK (TYPEOF(cluster_id) == 'blob' AND
                                          LENGTH(cluster_id) >= 16 AND
                                          LENGTH(cluster_id) <= 32),

    FOREIGN KEY (index_id) REFERENCES known_content (index_id) ON DELETE CASCADE,
    FOREIGN KEY (cluster_id) REFERENCES known_content (cluster_id),

    UNIQUE (index_id, cluster_index)
);

-- Prevent Updates
CREATE TRIGGER prevent_index_content_update
    BEFORE UPDATE
    ON index_content
    FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'Updates to index_content are not allowed.');
END;

-- Reference Counting
CREATE TRIGGER increment_used_counters_before_index_content_cluster_insert
    BEFORE INSERT
    ON index_content
BEGIN
    -- Upsert known clusters
    INSERT INTO known_content (content_type, cluster_id, used, chunk_avail, wal_avail)
    VALUES ('C', NEW.cluster_id, 1, 0, 0)
    ON CONFLICT(cluster_id) DO UPDATE SET used = used + 1;
END;

CREATE TRIGGER decrement_used_counters_after_index_content_delete
    AFTER DELETE
    ON index_content
BEGIN
    UPDATE known_content
    SET used = used - 1
    WHERE cluster_id = OLD.cluster_id;
END;

CREATE TABLE commits
(
    name                TEXT    NOT NULL COLLATE NOCASE CHECK (LENGTH(name) >= 1 AND
                                                               LENGTH(name) <= 255),
    type                TEXT    NOT NULL CHECK (type IN ('B', 'T', 'LB')),
    commit_id           BLOB    NOT NULL CHECK (TYPEOF(commit_id) == 'blob' AND
                                                LENGTH(commit_id) >= 16 AND
                                                LENGTH(commit_id) <= 32),
    preceding_commit_id BLOB    NOT NULL CHECK (TYPEOF(preceding_commit_id) == 'blob' AND
                                                LENGTH(preceding_commit_id) >= 16 AND
                                                LENGTH(preceding_commit_id) <= 32),
    index_id            BLOB    NOT NULL CHECK (TYPEOF(index_id) == 'blob' AND
                                                LENGTH(index_id) >= 16 AND
                                                LENGTH(index_id) <= 32),
    committed           INTEGER NOT NULL,
    num_clusters        INTEGER NOT NULL CHECK (num_clusters > 0),

    PRIMARY KEY (name, type),
    FOREIGN KEY (index_id) REFERENCES known_content (index_id)
);

-- Reference Counting
CREATE TRIGGER increment_used_counters_before_commit_insert
    BEFORE INSERT
    ON commits
BEGIN
    -- Upsert known indices
    INSERT INTO known_content (content_type, index_id, used, chunk_avail, wal_avail)
    VALUES ('I', NEW.index_id, 1, 0, 0)
    ON CONFLICT(index_id) DO UPDATE SET used = used + 1;
END;

CREATE TRIGGER increment_used_counters_before_commit_update
    BEFORE UPDATE
    ON commits
    WHEN NEW.index_id != OLD.index_id
BEGIN
    -- Upsert new index_id
    INSERT INTO known_content (content_type, index_id, used, chunk_avail, wal_avail)
    VALUES ('I', NEW.index_id, 1, 0, 0)
    ON CONFLICT(index_id) DO UPDATE SET used = used + 1;
END;

CREATE TRIGGER decrement_used_counters_after_commit_update
    AFTER UPDATE
    ON commits
    WHEN NEW.index_id != OLD.index_id
BEGIN
    -- Decrement old index_id
    UPDATE known_content
    SET used = used - 1
    WHERE index_id = OLD.index_id;
END;

CREATE TRIGGER decrement_used_counters_after_commit_delete
    AFTER DELETE
    ON commits
BEGIN
    UPDATE known_content
    SET used = used - 1
    WHERE index_id = OLD.index_id;
END;

CREATE TABLE wal_files
(
    id       BLOB    NOT NULL PRIMARY KEY CHECK (TYPEOF(id) == 'blob' AND
                                                 LENGTH(id) == 16),
    etag     BLOB    NOT NULL CHECK (TYPEOF(etag) == 'blob' AND
                                     LENGTH(etag) >= 8),
    created  INTEGER NOT NULL,
    active   BOOLEAN NOT NULL CHECK (active IN (0, 1)),
    entries  INTEGER NOT NULL DEFAULT 0 CHECK (entries >= 0),
    critical INTEGER NOT NULL DEFAULT 0 CHECK (critical >= 0)
);

-- Prevent Id changes
CREATE TRIGGER prevent_wal_files_id_update
    BEFORE UPDATE
    ON wal_files
    FOR EACH ROW
    WHEN NEW.id != OLD.id
BEGIN
    SELECT RAISE(ABORT, 'Updates to id columns are not allowed.');
END;

CREATE TRIGGER ensure_only_one_wal_file_active_before_update
    BEFORE UPDATE
    ON wal_files
    FOR EACH ROW
    WHEN NEW.active != OLD.active
        AND NEW.active = 1
BEGIN
    UPDATE wal_files SET active = 0 WHERE id != NEW.id;
END;

CREATE TRIGGER ensure_only_one_wal_file_active_before_insert
    BEFORE INSERT
    ON wal_files
    FOR EACH ROW
    WHEN NEW.active = 1
BEGIN
    UPDATE wal_files SET active = 0 WHERE id != NEW.id;
END;

CREATE TABLE chunks
(
    id   BLOB NOT NULL PRIMARY KEY CHECK (TYPEOF(id) == 'blob' AND
                                          LENGTH(id) == 16),
    etag BLOB NOT NULL CHECK (TYPEOF(etag) == 'blob' AND
                              LENGTH(etag) >= 8)
);

-- Prevent Id changes
CREATE TRIGGER prevent_chunks_id_update
    BEFORE UPDATE
    ON chunks
    FOR EACH ROW
    WHEN NEW.id != OLD.id
BEGIN
    SELECT RAISE(ABORT, 'Updates to id columns are not allowed.');
END;

CREATE TABLE available_content
(
    source_type  TEXT    NOT NULL CHECK (source_type IN ('W', 'C')),
    wal_id       BLOB CHECK (wal_id IS NULL OR (TYPEOF(wal_id) == 'blob' AND
                                                LENGTH(wal_id) == 16)),
    chunk_id     BLOB CHECK (chunk_id IS NULL OR (TYPEOF(chunk_id) == 'blob' AND
                                                  LENGTH(chunk_id) == 16)),
    offset       INTEGER NOT NULL CHECK (offset > 0),
    content_type TEXT    NOT NULL CHECK (content_type IN ('B', 'C', 'I')),

    block_id     BLOB,
    cluster_id   BLOB,
    index_id     BLOB,

    FOREIGN KEY (wal_id) REFERENCES wal_files (id) ON DELETE CASCADE,
    FOREIGN KEY (chunk_id) REFERENCES chunks (id) ON DELETE CASCADE,
    FOREIGN KEY (block_id) REFERENCES known_content (block_id),
    FOREIGN KEY (cluster_id) REFERENCES known_content (cluster_id),
    FOREIGN KEY (index_id) REFERENCES known_content (index_id),

    UNIQUE (wal_id, offset),
    UNIQUE (chunk_id, offset),

    CHECK (
        (source_type = 'W' AND wal_id IS NOT NULL AND chunk_id IS NULL) OR
        (source_type = 'C' AND wal_id IS NULL AND chunk_id IS NOT NULL)
        ),

    CHECK (
        (content_type = 'B' AND block_id IS NOT NULL AND cluster_id IS NULL AND index_id IS NULL) OR
        (content_type = 'C' AND cluster_id IS NOT NULL AND block_id IS NULL AND index_id IS NULL) OR
        (content_type = 'I' AND index_id IS NOT NULL AND block_id IS NULL AND cluster_id IS NULL)
        )
);

CREATE INDEX idx_available_content_source_type ON available_content (source_type);
CREATE INDEX idx_available_content_content_type ON available_content (content_type);
CREATE INDEX idx_available_content_block_id ON available_content (block_id);
CREATE INDEX idx_available_content_cluster_id ON available_content (cluster_id);
CREATE INDEX idx_available_content_index_id ON available_content (index_id);

-- Prevent Updates
CREATE TRIGGER prevent_available_content_update
    BEFORE UPDATE
    ON available_content
    FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'Updates to content are not allowed.');
END;

-- Reference Counting
CREATE TRIGGER increment_chunk_avail_counter_before_available_content_block_insert
    BEFORE INSERT
    ON available_content
    WHEN NEW.chunk_id IS NOT NULL
        AND NEW.block_id IS NOT NULL
BEGIN
    -- Upsert known blocks
    INSERT INTO known_content (content_type, block_id, used, chunk_avail, wal_avail)
    VALUES ('B', NEW.block_id, 0, 1, 0)
    ON CONFLICT(block_id) DO UPDATE SET chunk_avail = chunk_avail + 1;
END;

CREATE TRIGGER increment_chunk_avail_counter_before_available_content_cluster_insert
    BEFORE INSERT
    ON available_content
    WHEN NEW.chunk_id IS NOT NULL
        AND NEW.cluster_id IS NOT NULL
BEGIN
    -- Upsert known clusters
    INSERT INTO known_content (content_type, cluster_id, used, chunk_avail, wal_avail)
    VALUES ('C', NEW.cluster_id, 0, 1, 0)
    ON CONFLICT(cluster_id) DO UPDATE SET chunk_avail = chunk_avail + 1;
END;

CREATE TRIGGER increment_chunk_avail_counter_before_available_content_index_insert
    BEFORE INSERT
    ON available_content
    WHEN NEW.chunk_id IS NOT NULL
        AND NEW.index_id IS NOT NULL
BEGIN
    -- Upsert known indices
    INSERT INTO known_content (content_type, index_id, used, chunk_avail, wal_avail)
    VALUES ('I', NEW.index_id, 0, 1, 0)
    ON CONFLICT(index_id) DO UPDATE SET chunk_avail = chunk_avail + 1;
END;

CREATE TRIGGER increment_wal_avail_counter_before_available_content_block_insert
    BEFORE INSERT
    ON available_content
    WHEN NEW.wal_id IS NOT NULL
        AND NEW.block_id IS NOT NULL
BEGIN
    -- Upsert known blocks
    INSERT INTO known_content (content_type, block_id, used, chunk_avail, wal_avail)
    VALUES ('B', NEW.block_id, 0, 0, 1)
    ON CONFLICT(block_id) DO UPDATE SET wal_avail = wal_avail + 1;

    UPDATE wal_files
    SET entries = entries + 1
    WHERE id = NEW.wal_id;
END;

CREATE TRIGGER increment_wal_avail_counter_before_available_content_cluster_insert
    BEFORE INSERT
    ON available_content
    WHEN NEW.wal_id IS NOT NULL
        AND NEW.cluster_id IS NOT NULL
BEGIN
    -- Upsert known clusters
    INSERT INTO known_content (content_type, cluster_id, used, chunk_avail, wal_avail)
    VALUES ('C', NEW.cluster_id, 0, 0, 1)
    ON CONFLICT(cluster_id) DO UPDATE SET wal_avail = wal_avail + 1;

    UPDATE wal_files
    SET entries = entries + 1
    WHERE id = NEW.wal_id;
END;

CREATE TRIGGER increment_wal_avail_counter_before_available_content_index_insert
    BEFORE INSERT
    ON available_content
    WHEN NEW.wal_id IS NOT NULL
        AND NEW.index_id IS NOT NULL
BEGIN
    -- Upsert known indices
    INSERT INTO known_content (content_type, index_id, used, chunk_avail, wal_avail)
    VALUES ('I', NEW.index_id, 0, 0, 1)
    ON CONFLICT(index_id) DO UPDATE SET wal_avail = wal_avail + 1;

    UPDATE wal_files
    SET entries = entries + 1
    WHERE id = NEW.wal_id;
END;

CREATE TRIGGER decrement_chunk_avail_counters_after_available_content_delete
    AFTER DELETE
    ON available_content
    WHEN OLD.chunk_id IS NOT NULL
BEGIN
    UPDATE known_content
    SET chunk_avail = known_content.chunk_avail - 1
    WHERE OLD.block_id IS NOT NULL
      AND block_id = OLD.block_id;

    UPDATE known_content
    SET chunk_avail = known_content.chunk_avail - 1
    WHERE OLD.cluster_id IS NOT NULL
      AND cluster_id = OLD.cluster_id;

    UPDATE known_content
    SET chunk_avail = known_content.chunk_avail - 1
    WHERE OLD.index_id IS NOT NULL
      AND index_id = OLD.index_id;
END;

CREATE TRIGGER decrement_wal_avail_counters_after_available_content_delete
    AFTER DELETE
    ON available_content
    WHEN OLD.wal_id IS NOT NULL
BEGIN
    UPDATE wal_files
    SET entries = entries - 1
    WHERE id = OLD.wal_id;

    UPDATE known_content
    SET wal_avail = known_content.wal_avail - 1
    WHERE OLD.block_id IS NOT NULL
      AND block_id = OLD.block_id;

    UPDATE known_content
    SET wal_avail = known_content.wal_avail - 1
    WHERE OLD.cluster_id IS NOT NULL
      AND cluster_id = OLD.cluster_id;

    UPDATE known_content
    SET wal_avail = known_content.wal_avail - 1
    WHERE OLD.index_id IS NOT NULL
      AND index_id = OLD.index_id;
END;

-- keeps track of the most recent commit per wal_id / branch
CREATE TABLE wal_commits
(
    wal_id              BLOB    NOT NULL CHECK (TYPEOF(wal_id) == 'blob' AND
                                                LENGTH(wal_id) == 16),
    branch              TEXT    NOT NULL COLLATE NOCASE CHECK (LENGTH(branch) >= 1 AND
                                                               LENGTH(branch) <= 255),
    commit_id           BLOB    NOT NULL CHECK (TYPEOF(commit_id) == 'blob' AND
                                                LENGTH(commit_id) >= 16 AND
                                                LENGTH(commit_id) <= 32),
    preceding_commit_id BLOB    NOT NULL CHECK (TYPEOF(preceding_commit_id) == 'blob' AND
                                                LENGTH(preceding_commit_id) >= 16 AND
                                                LENGTH(preceding_commit_id) <= 32),
    index_id            BLOB    NOT NULL CHECK (TYPEOF(index_id) == 'blob' AND
                                                LENGTH(index_id) >= 16 AND
                                                LENGTH(index_id) <= 32),
    committed           INTEGER NOT NULL,
    num_clusters        INTEGER NOT NULL CHECK (num_clusters > 0),

    UNIQUE (wal_id, branch),

    FOREIGN KEY (wal_id) REFERENCES wal_files (id) ON DELETE CASCADE
);

-- Prevent Updates
CREATE TRIGGER prevent_wal_commits_update
    BEFORE UPDATE
    ON wal_commits
    FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'Updates to wal_commits are not allowed.');
END;

-- Only keep the most recent entry for a given wal_id / branch combination
CREATE TRIGGER delete_older_wal_commit_entries
    BEFORE INSERT
    ON wal_commits
    FOR EACH ROW
    WHEN EXISTS (SELECT 1
                 FROM wal_commits
                 WHERE wal_id = NEW.wal_id
                   AND branch = NEW.branch
                   AND NEW.committed > committed)
BEGIN
    DELETE
    FROM wal_commits
    WHERE wal_id = NEW.wal_id
      AND branch = NEW.branch;
END;
