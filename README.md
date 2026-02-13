# s3dupy

ncdu for S3. Async parallel scan + Textual TUI.

## Usage

```bash
pip install .
s3ncdu --endpoint=https://id.digitaloceanspaces.com --bucket=mybucket --ak=KEY --sk=SECRET
```

Or via env vars: `S3_ENDPOINT`, `S3_BUCKET`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.

## Keys

| Key | Action |
|---|---|
| Enter | Enter dir |
| Backspace | Go up |
| d | Delete |
| y / Escape | Confirm / cancel |
| s / n | Sort by size / name |
| q | Quit |
