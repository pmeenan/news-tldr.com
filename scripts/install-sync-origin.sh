#!/usr/bin/env bash

set -euo pipefail

project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
php_version="$(php -r 'echo PHP_MAJOR_VERSION, ".", PHP_MINOR_VERSION;')"
fpm_service="php${php_version}-fpm.service"
fpm_pool_dir="/etc/php/${php_version}/fpm/pool.d"
api_dir="/opt/news-tldr-sync"
data_dir="/var/lib/news-tldr-sync"
socket_path="/run/php/news-tldr-sync.sock"

if (( EUID != 0 )); then
  echo "Run this installer as root." >&2
  exit 1
fi

if ! php -m | grep -Fxq 'pdo_sqlite'; then
  echo "The PHP pdo_sqlite extension is required." >&2
  exit 1
fi
if [[ ! -d "$fpm_pool_dir" ]]; then
  echo "PHP-FPM pool directory not found: $fpm_pool_dir" >&2
  exit 1
fi

nginx_identity="$(awk '
  $1 == "user" {
    gsub(/;/, "", $2)
    gsub(/;/, "", $3)
    print $2 ":" $3
    exit
  }
' /etc/nginx/nginx.conf)"
nginx_user="${nginx_identity%%:*}"
nginx_group="${nginx_identity#*:}"
if [[ ! "$nginx_user" =~ ^[A-Za-z_][A-Za-z0-9_.-]*\$?$ ]] || ! id "$nginx_user" >/dev/null 2>&1; then
  echo "Could not determine a valid Nginx worker user from /etc/nginx/nginx.conf." >&2
  exit 1
fi
if [[ -z "$nginx_group" ]]; then
  nginx_group="$(id -gn "$nginx_user")"
fi
if [[ ! "$nginx_group" =~ ^[A-Za-z_][A-Za-z0-9_.-]*\$?$ ]] \
  || ! getent group "$nginx_group" >/dev/null; then
  echo "Could not determine a valid Nginx worker group from /etc/nginx/nginx.conf." >&2
  exit 1
fi

pool_config_tmp="$(mktemp)"
trap 'rm -f -- "$pool_config_tmp"' EXIT
sed \
  -e "s/^listen.owner = .*/listen.owner = $nginx_user/" \
  -e "s/^listen.group = .*/listen.group = $nginx_group/" \
  "$project_dir/deploy/php-fpm/news-tldr-sync.conf" >"$pool_config_tmp"

install -d -o root -g root -m 0755 "$api_dir"
install -o root -g root -m 0644 "$project_dir/server/sync/lib.php" "$api_dir/lib.php"
install -o root -g root -m 0644 "$project_dir/server/sync/api.php" "$api_dir/api.php"
install -o root -g root -m 0644 "$project_dir/server/sync/cleanup.php" "$api_dir/cleanup.php"
install -d -o www-data -g www-data -m 0750 "$data_dir"
install -o root -g root -m 0644 \
  "$pool_config_tmp" \
  "$fpm_pool_dir/news-tldr-sync.conf"
install -o root -g root -m 0644 \
  "$project_dir/deploy/cron/news-tldr-sync" \
  /etc/cron.d/news-tldr-sync
install -o root -g root -m 0644 \
  "$project_dir/deploy/nginx/news-tldr.com" \
  /etc/nginx/sites-available/news-tldr.com

runuser -u www-data -- env SYNC_DB_PATH="$data_dir/sync.sqlite" \
  /usr/bin/php "$api_dir/cleanup.php" >/dev/null

"/usr/sbin/php-fpm${php_version}" -t
nginx -t
systemctl reload "$fpm_service"
for _ in {1..20}; do
  [[ -S "$socket_path" ]] && break
  sleep 0.25
done
if [[ ! -S "$socket_path" ]]; then
  echo "Sync PHP-FPM socket was not created: $socket_path" >&2
  exit 1
fi
# PHP-FPM can retain an existing socket across a graceful reload. Correct its
# ownership now as well as in the pool config used for future service starts.
chown "$nginx_user:$nginx_group" "$socket_path"
chmod 0660 "$socket_path"
systemctl reload nginx.service

api_status=""
for _ in {1..20}; do
  api_status="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
    --connect-timeout 1 --max-time 3 \
    --resolve news-tldr.com:443:127.0.0.1 \
    --request POST \
    --header 'Origin: https://news-tldr.com' \
    https://news-tldr.com/api/sync/v1/groups || true)"
  [[ "$api_status" == "415" ]] && break
  sleep 0.25
done
if [[ "$api_status" != "415" ]]; then
  echo "Sync API smoke check returned HTTP $api_status instead of 415." >&2
  exit 1
fi

echo "Installed the news-tldr sync API, PHP-FPM pool, cleanup schedule, and Nginx route."
echo "Next: set presentation.reader_sync_enabled=true in config/pipeline.json and publish the presentation."
