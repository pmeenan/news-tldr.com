<?php

declare(strict_types=1);

final class SyncHttpError extends RuntimeException
{
    public function __construct(
        public readonly int $status,
        public readonly string $errorCode,
        string $message,
    ) {
        parent::__construct($message);
    }
}

final class SyncConfig
{
    /** @param list<string> $allowedOrigins */
    public function __construct(
        public readonly string $databasePath,
        public readonly array $allowedOrigins,
        public readonly int $maxBodyBytes,
        public readonly int $maxStateBytes,
        public readonly int $maxDatabaseBytes,
        public readonly int $maxReads,
        public readonly int $readRetentionSeconds,
        public readonly int $groupRetentionSeconds,
        public readonly int $maxActiveGroups,
        public readonly int $maxDailyGroupCreations,
    ) {
    }

    public static function fromEnvironment(): self
    {
        $databasePath = sync_setting('SYNC_DB_PATH', '/var/lib/news-tldr-sync/sync.sqlite');
        if (!str_starts_with($databasePath, '/')) {
            throw new RuntimeException('SYNC_DB_PATH must be absolute');
        }

        $origins = array_values(array_filter(array_map(
            static fn (string $value): string => trim($value),
            explode(',', sync_setting(
                'SYNC_ALLOWED_ORIGINS',
                'https://news-tldr.com,https://www.news-tldr.com',
            )),
        )));
        if ($origins === []) {
            throw new RuntimeException('at least one sync origin must be configured');
        }

        return new self(
            databasePath: $databasePath,
            allowedOrigins: $origins,
            maxBodyBytes: sync_positive_int('SYNC_MAX_BODY_BYTES', 262_144, 1_024, 1_048_576),
            maxStateBytes: sync_positive_int('SYNC_MAX_STATE_BYTES', 262_144, 4_096, 1_048_576),
            maxDatabaseBytes: sync_positive_int(
                'SYNC_MAX_DATABASE_BYTES',
                256 * 1024 * 1024,
                1024 * 1024,
                5_000_000_000,
            ),
            maxReads: sync_positive_int('SYNC_MAX_READS', 2_000, 1, 10_000),
            readRetentionSeconds: sync_positive_int(
                'SYNC_READ_RETENTION_SECONDS',
                3 * 24 * 60 * 60,
                60,
                31 * 24 * 60 * 60,
            ),
            groupRetentionSeconds: sync_positive_int(
                'SYNC_GROUP_RETENTION_SECONDS',
                180 * 24 * 60 * 60,
                24 * 60 * 60,
                5 * 365 * 24 * 60 * 60,
            ),
            maxActiveGroups: sync_positive_int('SYNC_MAX_ACTIVE_GROUPS', 2_000, 1, 1_000_000),
            maxDailyGroupCreations: sync_positive_int(
                'SYNC_MAX_DAILY_GROUP_CREATIONS',
                100,
                1,
                100_000,
            ),
        );
    }
}

final class SyncStore
{
    private PDO $database;

    public function __construct(private readonly SyncConfig $config)
    {
        $directory = dirname($config->databasePath);
        if (!is_dir($directory) || !is_writable($directory)) {
            throw new RuntimeException('sync database directory is unavailable');
        }

        $databaseWasMissing = !file_exists($config->databasePath);
        $this->database = new PDO('sqlite:' . $config->databasePath, options: [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_TIMEOUT => 5,
            PDO::ATTR_EMULATE_PREPARES => false,
        ]);
        $this->database->exec('PRAGMA busy_timeout = 5000');
        $this->database->exec('PRAGMA journal_mode = WAL');
        $this->database->exec('PRAGMA synchronous = NORMAL');
        $this->database->exec('PRAGMA wal_autocheckpoint = 1000');
        $this->database->exec('PRAGMA journal_size_limit = 16777216');
        $this->database->exec('PRAGMA foreign_keys = ON');
        $this->database->exec('PRAGMA trusted_schema = OFF');
        $pageSize = (int) $this->database->query('PRAGMA page_size')->fetchColumn();
        $maximumPages = max(1, intdiv($config->maxDatabaseBytes, max(1, $pageSize)));
        $this->database->exec('PRAGMA max_page_count = ' . $maximumPages);
        $this->migrate();
        if ($databaseWasMissing) {
            @chmod($config->databasePath, 0640);
        }
    }

    /** @param array<string, mixed> $clientState
     *  @return array<string, mixed>
     */
    public function createGroup(array $clientState): array
    {
        $nowSeconds = time();
        $nowMilliseconds = $nowSeconds * 1000;
        $state = $this->normalizeState($clientState, $nowMilliseconds);
        $token = sync_base64url_encode(random_bytes(32));
        $tokenHash = hash('sha256', $token);
        $day = gmdate('Y-m-d', $nowSeconds);

        $this->beginImmediate();
        try {
            $this->deleteExpiredGroups($nowSeconds);

            $activeGroups = (int) $this->database->query(
                'SELECT COUNT(*) FROM sync_groups',
            )->fetchColumn();
            if ($activeGroups >= $this->config->maxActiveGroups) {
                throw new SyncHttpError(503, 'group_capacity_reached', 'Sync capacity is temporarily full.');
            }

            $counter = $this->database->prepare(
                'SELECT groups_created FROM sync_daily_counters WHERE day = :day',
            );
            $counter->execute(['day' => $day]);
            $createdToday = (int) ($counter->fetchColumn() ?: 0);
            if ($createdToday >= $this->config->maxDailyGroupCreations) {
                throw new SyncHttpError(
                    429,
                    'daily_group_limit_reached',
                    'The daily sync-group creation limit has been reached.',
                );
            }

            $insertGroup = $this->database->prepare(
                'INSERT INTO sync_groups '
                . '(token_hash, reads_json, revision, created_at, updated_at, expires_at) '
                . 'VALUES (:token_hash, :reads_json, 1, :created_at, :updated_at, :expires_at)',
            );
            $insertGroup->execute([
                'token_hash' => $tokenHash,
                'reads_json' => $this->encodeState($state),
                'created_at' => $nowSeconds,
                'updated_at' => $nowSeconds,
                'expires_at' => $nowSeconds + $this->config->groupRetentionSeconds,
            ]);

            $incrementCounter = $this->database->prepare(
                'INSERT INTO sync_daily_counters (day, groups_created, updated_at) '
                . 'VALUES (:day, 1, :updated_at) '
                . 'ON CONFLICT(day) DO UPDATE SET '
                . 'groups_created = groups_created + 1, updated_at = excluded.updated_at',
            );
            $incrementCounter->execute(['day' => $day, 'updated_at' => $nowSeconds]);
            $this->commit();
        } catch (Throwable $error) {
            $this->rollback();
            throw $error;
        }

        return array_merge([
            'token' => $token,
            'revision' => 1,
            'server_time' => $nowMilliseconds,
        ], $this->publicState($state));
    }

    /** @param array<string, mixed> $clientState
     *  @return array<string, mixed>
     */
    public function mergeGroup(string $token, array $clientState, ?int $knownRevision = null): array
    {
        $nowSeconds = time();
        $nowMilliseconds = $nowSeconds * 1000;
        $tokenHash = hash('sha256', $token);
        $clientState = $this->normalizeState($clientState, $nowMilliseconds);

        $this->beginImmediate();
        try {
            $this->deleteExpiredGroups($nowSeconds);
            $select = $this->database->prepare(
                'SELECT reads_json, revision FROM sync_groups WHERE token_hash = :token_hash',
            );
            $select->execute(['token_hash' => $tokenHash]);
            $group = $select->fetch();
            if (!is_array($group)) {
                throw new SyncHttpError(404, 'sync_group_not_found', 'The sync link is invalid or expired.');
            }

            $storedEncoded = (string) $group['reads_json'];
            $serverState = $this->decodeState($storedEncoded, $nowMilliseconds);
            $serverRevision = (int) $group['revision'];
            foreach ($clientState['reads'] as $storyId => $timestamp) {
                $serverState['reads'][$storyId] = max(
                    $serverState['reads'][$storyId] ?? 0,
                    $timestamp,
                );
                if (isset($clientState['read_orders'][$storyId])) {
                    $serverState['read_orders'][$storyId] = $clientState['read_orders'][$storyId];
                }
            }
            if ($clientState['read_before'] !== null
                && ($serverState['read_before'] === null
                    || strcmp($clientState['read_before'], $serverState['read_before']) > 0)) {
                $serverState['read_before'] = $clientState['read_before'];
            }
            $mergedState = $this->normalizeState($serverState, $nowMilliseconds, rejectOversized: false);
            $stateChanged = $this->encodeState($mergedState) !== $storedEncoded;
            $revision = $serverRevision + ($stateChanged ? 1 : 0);

            if ($stateChanged) {
                $update = $this->database->prepare(
                    'UPDATE sync_groups SET reads_json = :reads_json, revision = :revision, '
                    . 'updated_at = :updated_at, expires_at = :expires_at WHERE token_hash = :token_hash',
                );
                $update->execute([
                    'reads_json' => $this->encodeState($mergedState),
                    'revision' => $revision,
                    'updated_at' => $nowSeconds,
                    'expires_at' => $nowSeconds + $this->config->groupRetentionSeconds,
                    'token_hash' => $tokenHash,
                ]);
            } else {
                $touch = $this->database->prepare(
                    'UPDATE sync_groups SET updated_at = :updated_at, expires_at = :expires_at '
                    . 'WHERE token_hash = :token_hash',
                );
                $touch->execute([
                    'updated_at' => $nowSeconds,
                    'expires_at' => $nowSeconds + $this->config->groupRetentionSeconds,
                    'token_hash' => $tokenHash,
                ]);
            }
            $this->commit();
        } catch (Throwable $error) {
            $this->rollback();
            throw $error;
        }

        $response = [
            'state_version' => 2,
            'revision' => $revision,
            'server_time' => $nowMilliseconds,
        ];
        if ($knownRevision === $serverRevision) {
            $response['unchanged'] = !$stateChanged;
            return $response;
        }
        return array_merge($response, $this->publicState($mergedState));
    }

    public function deleteGroup(string $token): bool
    {
        $delete = $this->database->prepare('DELETE FROM sync_groups WHERE token_hash = :token_hash');
        $delete->execute(['token_hash' => hash('sha256', $token)]);
        return $delete->rowCount() > 0;
    }

    /** @return array{groups_deleted: int, groups_pruned: int, reads_deleted: int, counters_deleted: int} */
    public function cleanup(): array
    {
        $nowSeconds = time();
        $nowMilliseconds = $nowSeconds * 1000;
        $groupsDeleted = $this->deleteExpiredGroups($nowSeconds);
        $counterCutoff = gmdate('Y-m-d', $nowSeconds - 8 * 24 * 60 * 60);
        $deleteCounters = $this->database->prepare(
            'DELETE FROM sync_daily_counters WHERE day < :cutoff',
        );
        $deleteCounters->execute(['cutoff' => $counterCutoff]);

        $groupsPruned = 0;
        $readsDeleted = 0;
        $select = $this->database->prepare(
            'SELECT token_hash, reads_json, revision FROM sync_groups '
            . 'WHERE token_hash > :cursor ORDER BY token_hash LIMIT 100',
        );
        $update = $this->database->prepare(
            'UPDATE sync_groups SET reads_json = :reads_json, revision = :revision '
            . 'WHERE token_hash = :token_hash',
        );
        $cursor = '';
        do {
            $select->execute(['cursor' => $cursor]);
            $groups = $select->fetchAll();
            foreach ($groups as $group) {
                $cursor = (string) $group['token_hash'];
                $storedEncoded = (string) $group['reads_json'];
                $storedState = $this->decodeStateUnpruned($storedEncoded);
                $storedReadCount = $this->rawStateReadCount($storedState);
                $currentState = $this->normalizeState(
                    $storedState,
                    $nowMilliseconds,
                    rejectOversized: false,
                );
                if ($storedEncoded === $this->encodeState($currentState)) {
                    continue;
                }
                $update->execute([
                    'reads_json' => $this->encodeState($currentState),
                    'revision' => (int) $group['revision'] + 1,
                    'token_hash' => $cursor,
                ]);
                $groupsPruned++;
                $readsDeleted += max(0, $storedReadCount - count($currentState['reads']));
            }
        } while (count($groups) === 100);

        return [
            'groups_deleted' => $groupsDeleted,
            'groups_pruned' => $groupsPruned,
            'reads_deleted' => $readsDeleted,
            'counters_deleted' => $deleteCounters->rowCount(),
        ];
    }

    private function migrate(): void
    {
        $this->database->exec(
            'CREATE TABLE IF NOT EXISTS sync_groups ('
            . 'token_hash TEXT PRIMARY KEY CHECK(length(token_hash) = 64), '
            . 'reads_json TEXT NOT NULL, '
            . 'revision INTEGER NOT NULL CHECK(revision >= 1), '
            . 'created_at INTEGER NOT NULL, '
            . 'updated_at INTEGER NOT NULL, '
            . 'expires_at INTEGER NOT NULL'
            . ')',
        );
        $this->database->exec(
            'CREATE INDEX IF NOT EXISTS sync_groups_expires_at_idx ON sync_groups(expires_at)',
        );
        $this->database->exec(
            'CREATE TABLE IF NOT EXISTS sync_daily_counters ('
            . 'day TEXT PRIMARY KEY, '
            . 'groups_created INTEGER NOT NULL CHECK(groups_created >= 0), '
            . 'updated_at INTEGER NOT NULL'
            . ')',
        );
    }

    /** @param array<string, mixed> $state
     *  @return array{reads: array<string, int>, read_orders: array<string, string>, read_before: ?string}
     */
    private function normalizeState(
        array $state,
        int $nowMilliseconds,
        bool $rejectOversized = true,
    ): array
    {
        $reads = $state['reads'] ?? [];
        if (!is_array($reads) || (array_is_list($reads) && $reads !== [])) {
            throw new SyncHttpError(400, 'invalid_reads', 'The reads field must be a JSON object.');
        }
        $rawOrders = $state['read_orders'] ?? [];
        if (!is_array($rawOrders) || (array_is_list($rawOrders) && $rawOrders !== [])) {
            throw new SyncHttpError(400, 'invalid_read_orders', 'The read-order field must be a JSON object.');
        }
        $orderedReads = $state['ordered_reads'] ?? [];
        if (!is_array($orderedReads) || !array_is_list($orderedReads)) {
            throw new SyncHttpError(400, 'invalid_ordered_reads', 'The ordered reads field must be a JSON list.');
        }
        foreach ($orderedReads as $row) {
            if (!is_array($row) || !array_is_list($row) || count($row) !== 3
                || !is_string($row[0])
                || preg_match('/\A[a-zA-Z0-9._-]{1,128}\z/D', $row[0]) !== 1
                || (!is_int($row[1]) && !is_float($row[1]))
                || !is_int($row[2])) {
                throw new SyncHttpError(400, 'invalid_ordered_read', 'An ordered read entry is invalid.');
            }
            $storyId = $row[0];
            $reads[$storyId] = max((int) ($reads[$storyId] ?? 0), (int) $row[1]);
            $rawOrders[$storyId] = sprintf('%013d:%s', $row[2], $storyId);
        }
        if ($rejectOversized && count($reads) > $this->config->maxReads) {
            throw new SyncHttpError(413, 'too_many_reads', 'The read-history list is too large.');
        }

        $minimumTimestamp = $nowMilliseconds - $this->config->readRetentionSeconds * 1000;
        $maximumTimestamp = $nowMilliseconds + 5 * 60 * 1000;
        $normalized = [];
        foreach ($reads as $storyId => $timestamp) {
            if (!is_string($storyId) || preg_match('/\A[a-zA-Z0-9._-]{1,128}\z/D', $storyId) !== 1) {
                throw new SyncHttpError(400, 'invalid_story_id', 'The read-history list contains an invalid story ID.');
            }
            if (!is_int($timestamp) && !is_float($timestamp)) {
                throw new SyncHttpError(400, 'invalid_read_timestamp', 'A read timestamp is invalid.');
            }
            $timestamp = (int) $timestamp;
            if ($timestamp < $minimumTimestamp) {
                continue;
            }
            $normalized[$storyId] = min($timestamp, $maximumTimestamp, $nowMilliseconds);
        }
        if (count($normalized) > $this->config->maxReads) {
            arsort($normalized, SORT_NUMERIC);
            $normalized = array_slice($normalized, 0, $this->config->maxReads, preserve_keys: true);
        }
        ksort($normalized, SORT_STRING);

        $orders = [];
        foreach ($rawOrders as $storyId => $order) {
            if (!isset($normalized[$storyId])) {
                continue;
            }
            $normalizedOrder = $this->normalizeStoryOrder($order, $storyId, $nowMilliseconds);
            if ($normalizedOrder !== null) {
                $orders[$storyId] = $normalizedOrder;
            }
        }
        ksort($orders, SORT_STRING);

        $readBefore = $state['read_before'] ?? null;
        if ($readBefore !== null) {
            $readBefore = $this->normalizeStoryOrder($readBefore, null, $nowMilliseconds);
        }
        if ($readBefore !== null) {
            foreach ($orders as $storyId => $order) {
                if (strcmp($order, $readBefore) <= 0) {
                    unset($normalized[$storyId], $orders[$storyId]);
                }
            }
        }
        return ['reads' => $normalized, 'read_orders' => $orders, 'read_before' => $readBefore];
    }

    /** @return array{reads: array<string, int>, read_orders: array<string, string>, read_before: ?string} */
    private function decodeState(string $encoded, int $nowMilliseconds): array
    {
        return $this->normalizeState(
            $this->decodeStateUnpruned($encoded),
            $nowMilliseconds,
            rejectOversized: false,
        );
    }

    /** @return array{reads: array<mixed, mixed>, read_orders: array<mixed, mixed>, ordered_reads: array<mixed>, read_before: mixed} */
    private function decodeStateUnpruned(string $encoded): array
    {
        try {
            $decoded = json_decode($encoded, true, 16, JSON_THROW_ON_ERROR);
        } catch (JsonException $error) {
            throw new RuntimeException('stored sync state is invalid', previous: $error);
        }
        if (!is_array($decoded) || (array_is_list($decoded) && $decoded !== [])) {
            throw new RuntimeException('stored sync state has an invalid shape');
        }
        if (($decoded['state_version'] ?? null) === 2) {
            return [
                'reads' => $decoded['reads'] ?? [],
                'read_orders' => $decoded['read_orders'] ?? [],
                'ordered_reads' => $decoded['ordered_reads'] ?? [],
                'read_before' => $decoded['read_before'] ?? null,
            ];
        }
        return [
            'reads' => $decoded,
            'read_orders' => [],
            'ordered_reads' => [],
            'read_before' => null,
        ];
    }

    /** @param array{reads: array<string, int>, read_orders: array<string, string>, read_before: ?string} $state */
    private function encodeState(array $state): string
    {
        $plainReads = $state['reads'];
        $orderedReads = [];
        foreach ($state['read_orders'] as $storyId => $order) {
            $orderedReads[] = [$storyId, $state['reads'][$storyId], (int) substr($order, 0, 13)];
            unset($plainReads[$storyId]);
        }
        $encoded = json_encode([
            'state_version' => 2,
            'reads' => (object) $plainReads,
            'ordered_reads' => $orderedReads,
            'read_before' => $state['read_before'],
        ], JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
        if (strlen($encoded) > $this->config->maxStateBytes) {
            throw new SyncHttpError(413, 'sync_state_too_large', 'The merged read history is too large.');
        }
        return $encoded;
    }

    /** @param array{reads: array<string, int>, read_orders: array<string, string>, read_before: ?string} $state
     *  @return array<string, mixed>
     */
    private function publicState(array $state): array
    {
        return [
            'state_version' => 2,
            'reads' => (object) $state['reads'],
            'read_before' => $state['read_before'],
        ];
    }

    /** @param array<string, mixed> $state */
    private function rawStateReadCount(array $state): int
    {
        $storyIds = [];
        foreach (($state['reads'] ?? []) as $storyId => $_timestamp) {
            if (is_string($storyId)) {
                $storyIds[$storyId] = true;
            }
        }
        foreach (($state['ordered_reads'] ?? []) as $row) {
            if (is_array($row) && isset($row[0]) && is_string($row[0])) {
                $storyIds[$row[0]] = true;
            }
        }
        return count($storyIds);
    }

    private function normalizeStoryOrder(mixed $value, ?string $storyId, int $nowMilliseconds): ?string
    {
        if (!is_string($value)
            || preg_match('/\A([0-9]{13}):([a-zA-Z0-9._-]{1,128})\z/D', $value, $matches) !== 1
            || ($storyId !== null && $matches[2] !== $storyId)) {
            throw new SyncHttpError(400, 'invalid_story_order', 'A story read-order value is invalid.');
        }
        $timestamp = (int) $matches[1];
        $minimumTimestamp = $nowMilliseconds - $this->config->readRetentionSeconds * 1000;
        if ($timestamp < $minimumTimestamp) {
            return null;
        }
        if ($timestamp > $nowMilliseconds + 5 * 60 * 1000) {
            throw new SyncHttpError(400, 'future_story_order', 'A story read-order value is in the future.');
        }
        return $value;
    }

    private function deleteExpiredGroups(int $nowSeconds): int
    {
        $delete = $this->database->prepare('DELETE FROM sync_groups WHERE expires_at <= :now');
        $delete->execute(['now' => $nowSeconds]);
        return $delete->rowCount();
    }

    private function beginImmediate(): void
    {
        $this->database->exec('BEGIN IMMEDIATE');
    }

    private function commit(): void
    {
        $this->database->exec('COMMIT');
    }

    private function rollback(): void
    {
        try {
            $this->database->exec('ROLLBACK');
        } catch (Throwable) {
            // Preserve the original exception when a transaction already closed.
        }
    }
}

function sync_setting(string $name, string $default): string
{
    $serverValue = $_SERVER[$name] ?? null;
    if (is_string($serverValue) && $serverValue !== '') {
        return $serverValue;
    }
    $environmentValue = getenv($name);
    return is_string($environmentValue) && $environmentValue !== '' ? $environmentValue : $default;
}

function sync_positive_int(string $name, int $default, int $minimum, int $maximum): int
{
    $raw = sync_setting($name, (string) $default);
    if (preg_match('/\A[0-9]+\z/D', $raw) !== 1) {
        throw new RuntimeException($name . ' must be an integer');
    }
    $value = (int) $raw;
    if ($value < $minimum || $value > $maximum) {
        throw new RuntimeException($name . ' is outside the allowed range');
    }
    return $value;
}

function sync_base64url_encode(string $value): string
{
    return rtrim(strtr(base64_encode($value), '+/', '-_'), '=');
}
