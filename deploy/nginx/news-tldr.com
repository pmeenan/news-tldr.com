server {
    server_name news-tldr.com www.news-tldr.com;

    root /var/www/news-tldr.com/;
    index index.html;

    add_header Content-Security-Policy "default-src 'self' data: blob:; script-src 'self' 'wasm-unsafe-eval'; connect-src 'self'; style-src 'self' 'unsafe-inline'; worker-src 'self' blob:; object-src 'none'; base-uri 'self'; form-action 'self'; frame-ancestors 'none'" always;

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
