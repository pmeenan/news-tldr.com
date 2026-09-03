<?php

declare(strict_types=1);

require_once __DIR__ . '/lib.php';

sync_send_headers();

try {
    $config = SyncConfig::fromEnvironment();
    sync_require_allowed_origin($config);
    $store = new SyncStore($config);
    $method = strtoupper((string) ($_SERVER['REQUEST_METHOD'] ?? 'GET'));
    $path = (string) (parse_url((string) ($_SERVER['REQUEST_URI'] ?? '/'), PHP_URL_PATH) ?: '/');

    if ($path === '/api/sync/v1/groups' && $method === 'POST') {
        $payload = sync_read_payload($config);
        sync_json_response(201, $store->createGroup(sync_payload_state($payload)));
    } elseif ($path === '/api/sync/v1/merge' && $method === 'POST') {
        $payload = sync_read_payload($config);
        sync_json_response(
            200,
            $store->mergeGroup(
                sync_bearer_token(),
                sync_payload_state($payload),
                sync_payload_known_revision($payload),
            ),
        );
    } elseif ($path === '/api/sync/v1/group' && $method === 'DELETE') {
        if (!$store->deleteGroup(sync_bearer_token())) {
            throw new SyncHttpError(404, 'sync_group_not_found', 'The sync link is invalid or expired.');
        }
        http_response_code(204);
    } elseif (in_array($path, [
        '/api/sync/v1/groups',
        '/api/sync/v1/merge',
        '/api/sync/v1/group',
    ], true)) {
        header('Allow: POST, DELETE');
        throw new SyncHttpError(405, 'method_not_allowed', 'This request method is not allowed.');
    } else {
        throw new SyncHttpError(404, 'endpoint_not_found', 'The sync endpoint was not found.');
    }
} catch (SyncHttpError $error) {
    sync_json_response($error->status, [
        'error' => $error->errorCode,
        'message' => $error->getMessage(),
    ]);
} catch (Throwable $error) {
    error_log('news-tldr sync API error: ' . $error->getMessage());
    sync_json_response(500, [
        'error' => 'internal_error',
        'message' => 'Read-history sync is temporarily unavailable.',
    ]);
}

function sync_send_headers(): void
{
    header('Content-Type: application/json; charset=utf-8');
    header('Cache-Control: private, no-store, max-age=0');
    header('Pragma: no-cache');
    header('X-Content-Type-Options: nosniff');
    header('Referrer-Policy: no-referrer');
    header('Cross-Origin-Resource-Policy: same-origin');
}

function sync_require_allowed_origin(SyncConfig $config): void
{
    $origin = (string) ($_SERVER['HTTP_ORIGIN'] ?? '');
    if ($origin === '' || !in_array($origin, $config->allowedOrigins, true)) {
        throw new SyncHttpError(403, 'origin_not_allowed', 'This request origin is not allowed.');
    }
}

/** @return array<string, mixed> */
function sync_read_payload(SyncConfig $config): array
{
    $contentType = strtolower(trim(explode(';', (string) ($_SERVER['CONTENT_TYPE'] ?? ''))[0]));
    if ($contentType !== 'application/json') {
        throw new SyncHttpError(415, 'json_required', 'Requests must use application/json.');
    }

    $body = file_get_contents('php://input', false, null, 0, $config->maxBodyBytes + 1);
    if (!is_string($body)) {
        throw new SyncHttpError(400, 'request_unreadable', 'The request body could not be read.');
    }
    if (strlen($body) > $config->maxBodyBytes) {
        throw new SyncHttpError(413, 'request_too_large', 'The request body is too large.');
    }
    try {
        $payload = json_decode($body, true, 16, JSON_THROW_ON_ERROR);
    } catch (JsonException) {
        throw new SyncHttpError(400, 'invalid_json', 'The request body is not valid JSON.');
    }
    if (!is_array($payload) || array_is_list($payload)) {
        throw new SyncHttpError(400, 'invalid_payload', 'The request body must be a JSON object.');
    }
    return $payload;
}

/** @param array<string, mixed> $payload
 *  @return array<string, mixed>
 */
function sync_payload_state(array $payload): array
{
    $reads = $payload['reads'] ?? null;
    if (!is_array($reads) || (array_is_list($reads) && $reads !== [])) {
        throw new SyncHttpError(400, 'invalid_reads', 'The reads field must be a JSON object.');
    }
    return [
        'reads' => $reads,
        'read_orders' => $payload['read_orders'] ?? [],
        'ordered_reads' => $payload['ordered_reads'] ?? [],
        'read_before' => $payload['read_before'] ?? null,
    ];
}

/** @param array<string, mixed> $payload */
function sync_payload_known_revision(array $payload): ?int
{
    $revision = $payload['known_revision'] ?? null;
    if ($revision === null) {
        return null;
    }
    if (!is_int($revision) || $revision < 1) {
        throw new SyncHttpError(400, 'invalid_revision', 'The known revision is invalid.');
    }
    return $revision;
}

function sync_bearer_token(): string
{
    $authorization = (string) ($_SERVER['HTTP_AUTHORIZATION'] ?? '');
    if (preg_match('/\ABearer ([A-Za-z0-9_-]{43})\z/D', $authorization, $matches) !== 1) {
        throw new SyncHttpError(401, 'invalid_sync_token', 'A valid sync token is required.');
    }
    return $matches[1];
}

/** @param array<string, mixed> $payload */
function sync_json_response(int $status, array $payload): never
{
    http_response_code($status);
    if ($status !== 204) {
        echo json_encode($payload, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES) . "\n";
    }
    exit;
}
