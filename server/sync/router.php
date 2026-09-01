<?php

declare(strict_types=1);

$path = (string) (parse_url((string) ($_SERVER['REQUEST_URI'] ?? '/'), PHP_URL_PATH) ?: '/');
if (str_starts_with($path, '/api/sync/v1/')) {
    require __DIR__ . '/api.php';
    return true;
}

return false;

