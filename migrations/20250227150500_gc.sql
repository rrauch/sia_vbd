ALTER TABLE chunk_files
    ADD COLUMN entries INTEGER NOT NULL DEFAULT 0 CHECK (entries >= 0);
ALTER TABLE chunk_files
    ADD COLUMN critical INTEGER NOT NULL DEFAULT 0 CHECK (critical >= 0);

UPDATE chunk_files
SET entries = (SELECT COUNT(*)
               FROM available_content
               WHERE available_content.chunk_id = chunk_files.chunk_id);

UPDATE chunk_files
SET critical = (SELECT COUNT(*)
                FROM available_content ac
                         JOIN known_content kc
                              ON (ac.block_id = kc.block_id
                                  OR ac.cluster_id = kc.cluster_id
                                  OR ac.snapshot_id = kc.snapshot_id)
                WHERE ac.chunk_id = chunk_files.chunk_id
                  AND kc.used > 0
                  AND kc.wal_avail = 0
                  AND kc.chunk_avail <= 1);

CREATE TRIGGER adjust_chunk_critical_after_insert
    AFTER INSERT
    ON known_content
    FOR EACH ROW
    WHEN (NEW.used > 0 AND NEW.chunk_avail = 1 AND NEW.wal_avail = 0)
BEGIN
    UPDATE chunk_files
    SET critical = critical + 1
    WHERE chunk_id IN (SELECT DISTINCT ac.chunk_id
                       FROM available_content ac
                       WHERE (
                                 (ac.block_id = NEW.block_id) OR
                                 (ac.cluster_id = NEW.cluster_id) OR
                                 (ac.snapshot_id = NEW.snapshot_id)
                                 ));
END;

CREATE TRIGGER adjust_chunk_critical_after_update_activated
    AFTER UPDATE
    ON known_content
    FOR EACH ROW
    WHEN (
        NEW.used > 0 AND
        NEW.chunk_avail = 1 AND
        NEW.wal_avail = 0 AND
        (OLD.used IS NULL OR OLD.used = 0 OR OLD.wal_avail > 0)
        )
BEGIN
    UPDATE chunk_files
    SET critical = critical + 1
    WHERE chunk_id IN (SELECT DISTINCT ac.chunk_id
                       FROM available_content ac
                       WHERE (
                                 (ac.block_id = NEW.block_id) OR
                                 (ac.cluster_id = NEW.cluster_id) OR
                                 (ac.snapshot_id = NEW.snapshot_id)
                                 ));
END;

CREATE TRIGGER adjust_chunk_critical_after_update_deactivated
    AFTER UPDATE
    ON known_content
    FOR EACH ROW
    WHEN (
        (NEW.used = 0 OR
         NEW.wal_avail > 0 OR
         NEW.chunk_avail != 1) AND
        OLD.chunk_avail = 1 AND
        OLD.used > 0 AND
        OLD.wal_avail = 0
        )
BEGIN
    UPDATE chunk_files
    SET critical = critical - 1
    WHERE chunk_id IN (SELECT DISTINCT ac.chunk_id
                       FROM available_content ac
                       WHERE (
                                 (ac.block_id = NEW.block_id) OR
                                 (ac.cluster_id = NEW.cluster_id) OR
                                 (ac.snapshot_id = NEW.snapshot_id)
                                 ))
      AND critical > 0;
END;

-- Reference Counting
DROP TRIGGER increment_chunk_avail_counter_before_available_content_block_insert;
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

    UPDATE chunk_files
    SET entries = entries + 1
    WHERE chunk_id = NEW.chunk_id;
END;

DROP TRIGGER increment_chunk_avail_counter_before_available_content_cluster_insert;
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

    UPDATE chunk_files
    SET entries = entries + 1
    WHERE chunk_id = NEW.chunk_id;
END;

DROP TRIGGER increment_chunk_avail_counter_before_available_content_snapshot_insert;
CREATE TRIGGER increment_chunk_avail_counter_before_available_content_snapshot_insert
    BEFORE INSERT
    ON available_content
    WHEN NEW.chunk_id IS NOT NULL
        AND NEW.snapshot_id IS NOT NULL
BEGIN
    -- Upsert known snapshots
    INSERT INTO known_content (content_type, snapshot_id, used, chunk_avail, wal_avail)
    VALUES ('S', NEW.snapshot_id, 0, 1, 0)
    ON CONFLICT(snapshot_id) DO UPDATE SET chunk_avail = chunk_avail + 1;

    UPDATE chunk_files
    SET entries = entries + 1
    WHERE chunk_id = NEW.chunk_id;
END;

DROP TRIGGER decrement_chunk_avail_counters_after_available_content_delete;
CREATE TRIGGER decrement_chunk_avail_counters_after_available_content_delete
    AFTER DELETE
    ON available_content
    WHEN OLD.chunk_id IS NOT NULL
BEGIN
    UPDATE chunk_files
    SET entries = entries - 1
    WHERE chunk_id = OLD.chunk_id;

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
    WHERE OLD.snapshot_id IS NOT NULL
      AND snapshot_id = OLD.snapshot_id;
END;

ALTER TABLE manifests
    ADD COLUMN prunable INTEGER NOT NULL DEFAULT 0 CHECK (prunable >= 0);

UPDATE manifests
SET prunable = (SELECT COUNT(*)
                FROM manifest_content mc
                         JOIN known_chunks kc ON mc.chunk_id = kc.id
                WHERE mc.manifest_id = manifests.id
                  AND kc.available = 0);

CREATE TRIGGER adjust_manifest_prunable_after_insert
    AFTER INSERT
    ON known_chunks
    FOR EACH ROW
    WHEN NEW.available = 0
BEGIN
    UPDATE manifests
    SET prunable = prunable + 1
    WHERE id IN (SELECT manifest_id
                 FROM manifest_content
                 WHERE chunk_id = NEW.id);
END;

CREATE TRIGGER adjust_manifest_prunable_after_update_available
    AFTER UPDATE
    ON known_chunks
    FOR EACH ROW
    WHEN NEW.available > 0 AND OLD.available = 0
BEGIN
    UPDATE manifests
    SET prunable = prunable - 1
    WHERE id IN (SELECT manifest_id
                 FROM manifest_content
                 WHERE chunk_id = NEW.id)
      AND prunable > 0;
END;

CREATE TRIGGER adjust_manifest_prunable_after_update_unavailable
    AFTER UPDATE
    ON known_chunks
    FOR EACH ROW
    WHEN NEW.available = 0 AND OLD.available > 0
BEGIN
    UPDATE manifests
    SET prunable = prunable + 1
    WHERE id IN (SELECT manifest_id
                 FROM manifest_content
                 WHERE chunk_id = NEW.id);
END;
