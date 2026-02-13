# s3dupy

ncdu-like TUI for browsing and cleaning S3-compatible buckets (AWS S3, DigitalOcean Spaces, MinIO, etc.).

![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)

## Features

- **Parallel async scan** — uses aiobotocore with prefix-splitting to scan hundreds of thousands of objects in seconds
- **ncdu-style navigation** — browse your bucket as a virtual directory tree
- **Delete files and folders** — with confirmation prompt; folders delete all contained objects recursively
- **Sort by size or name**
- **Bar chart visualization** — see relative sizes at a glance

## Install

```bash
pip install .
```

Or run directly:

```bash
python s3ncdu.py --endpoint=URL --bucket=NAME --ak=KEY --sk=SECRET
```

## Usage

```bash
# Via CLI flags
s3ncdu --endpoint=https://fra1.digitaloceanspaces.com --bucket=mybucket --ak=KEY --sk=SECRET

# Via environment variables
export S3_ENDPOINT=https://fra1.digitaloceanspaces.com
export S3_BUCKET=mybucket
export AWS_ACCESS_KEY_ID=KEY
export AWS_SECRET_ACCESS_KEY=SECRET
s3ncdu
```

## Keybindings

| Key | Action |
|---|---|
| `Enter` | Enter directory |
| `Backspace` | Go up |
| `d` | Delete selected file/folder |
| `y` | Confirm delete |
| `Escape` | Cancel delete |
| `s` | Sort by size |
| `n` | Sort by name |
| `q` | Quit |

## How it works

1. **Scan phase** — discovers prefix structure via `Delimiter="/"`, then lists all prefixes in parallel (16 concurrent) using aiobotocore. For flat buckets with few prefixes, splits by character (0-9, a-z) for parallelism.
2. **TUI phase** — builds a virtual directory tree and renders it with Textual. Deletes use a sync boto3 client with batched `delete_objects` calls (1000 keys per batch).

## License

MIT
