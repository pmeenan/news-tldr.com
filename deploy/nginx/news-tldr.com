limit_req_zone $http_cf_connecting_ip zone=news_tldr_sync_client:10m rate=30r/m;
limit_req_zone $binary_remote_addr zone=news_tldr_sync_peer:10m rate=120r/m;
limit_conn_zone $binary_remote_addr zone=news_tldr_sync_connections:10m;

server {
    server_name news-tldr.com www.news-tldr.com;

    root /var/www/news-tldr.com/;
    index index.html;

    add_header Content-Security-Policy "default-src 'self' data: blob:; script-src 'self' 'wasm-unsafe-eval'; connect-src 'self'; style-src 'self' 'unsafe-inline'; worker-src 'self' blob:; object-src 'none'; base-uri 'self'; form-action 'self'; frame-ancestors 'none'" always;

    # The anonymous read-history API runs in a dedicated, resource-bounded
    # PHP-FPM pool. Only these exact endpoints reach PHP; every other /api/sync
    # path is rejected rather than falling through to the static homepage.
    location ~ ^/api/sync/v1/(groups|merge|group)$ {
        client_max_body_size 256k;
        client_body_timeout 5s;
        # The client-keyed limit is accurate behind Cloudflare. The peer-keyed
        # ceiling still constrains direct requests and spoofed client headers.
        limit_req zone=news_tldr_sync_client burst=15 nodelay;
        limit_req zone=news_tldr_sync_peer burst=30 nodelay;
        limit_conn news_tldr_sync_connections 4;

        fastcgi_param QUERY_STRING $query_string;
        fastcgi_param REQUEST_METHOD $request_method;
        fastcgi_param CONTENT_TYPE $content_type;
        fastcgi_param CONTENT_LENGTH $content_length;
        fastcgi_param REQUEST_URI $request_uri;
        fastcgi_param SERVER_PROTOCOL $server_protocol;
        fastcgi_param REQUEST_SCHEME $scheme;
        fastcgi_param HTTPS $https if_not_empty;
        fastcgi_param GATEWAY_INTERFACE CGI/1.1;
        fastcgi_param SERVER_NAME $server_name;
        fastcgi_param REMOTE_ADDR $remote_addr;
        fastcgi_param REDIRECT_STATUS 200;
        fastcgi_param SCRIPT_FILENAME /opt/news-tldr-sync/api.php;
        fastcgi_param HTTP_AUTHORIZATION $http_authorization;
        fastcgi_param HTTP_ORIGIN $http_origin;
        fastcgi_param SYNC_DB_PATH /var/lib/news-tldr-sync/sync.sqlite;
        fastcgi_param SYNC_ALLOWED_ORIGINS "https://news-tldr.com,https://www.news-tldr.com";
        fastcgi_connect_timeout 2s;
        fastcgi_send_timeout 10s;
        fastcgi_read_timeout 10s;
        fastcgi_pass unix:/run/php/news-tldr-sync.sock;
        fastcgi_hide_header X-Powered-By;
    }

    location /api/sync/ {
        return 404;
    }

    # The site is rebuilt hourly. Let browsers and Cloudflare reuse generated
    # HTML for 10 minutes before checking the origin for a newer build.
    location ~* \.(html|htm)$ {
        expires 10m;
    }

    # CSS and JavaScript use content hashes in their filenames, so they can be
    # cached for a year without serving stale code after a presentation change.
    location ~ "^/assets/site\.[0-9a-f]{16}\.(css|js)$" {
        expires 1y;
        add_header Cache-Control "public, immutable";
    }

    location / {
        try_files $uri $uri/ /index.html;
    }

    listen 443 ssl; # managed by Certbot
    ssl_certificate /etc/letsencrypt/live/news-tldr.com/fullchain.pem; # managed by Certbot
    ssl_certificate_key /etc/letsencrypt/live/news-tldr.com/privkey.pem; # managed by Certbot
    include /etc/letsencrypt/options-ssl-nginx.conf; # managed by Certbot
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem; # managed by Certbot
}

server {
    if ($host = www.news-tldr.com) {
        return 301 https://$host$request_uri;
    } # managed by Certbot

    if ($host = news-tldr.com) {
        return 301 https://$host$request_uri;
    } # managed by Certbot

    server_name news-tldr.com www.news-tldr.com;
    listen 80;
    return 404; # managed by Certbot
}
