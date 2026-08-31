from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_nginx_html_pages_have_ten_minute_cache_lifetime() -> None:
    config = (PROJECT_ROOT / "deploy" / "nginx" / "news-tldr.com").read_text(encoding="utf-8")

    assert r"location ~* \.(html|htm)$" in config
    assert "expires 10m;" in config
    assert "expires -1;" not in config


def test_nginx_versioned_assets_have_one_year_immutable_cache_lifetime() -> None:
    config = (PROJECT_ROOT / "deploy" / "nginx" / "news-tldr.com").read_text(encoding="utf-8")

    assert r'location ~ "^/assets/site\.[0-9a-f]{16}\.(css|js)$"' in config
    assert "expires 1y;" in config
    assert 'add_header Cache-Control "public, immutable";' in config
