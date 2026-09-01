<?php

declare(strict_types=1);

require_once __DIR__ . '/lib.php';

$verbose = in_array('--verbose', $argv ?? [], true);

try {
    $config = SyncConfig::fromEnvironment();
    $store = new SyncStore($config);
    if ($verbose) {
        fwrite(STDERR, "Pruning expired sync groups and read-history entries...\n");
    }
    $stats = $store->cleanup();
    echo json_encode($stats, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES) . "\n";
} catch (Throwable $error) {
    fwrite(STDERR, 'news-tldr sync cleanup failed: ' . $error->getMessage() . "\n");
    exit(1);
}

