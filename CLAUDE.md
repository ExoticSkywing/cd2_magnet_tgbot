# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CloudDrive2 Telegram Bot (cd2_magnet_tgbot) - A Telegram bot that manages offline downloads for CloudDrive2. It accepts magnet/http/ed2k links, submits them to CD2 for offline download, and provides automated cleanup functionality.

**Tech Stack**: Python 3.13 + python-telegram-bot (v22+) + gRPC + Docker

## Commands

### Development
```bash
# Run locally (requires environment variables set)
python main.py

# Build Docker image
docker build -t cd2-bot .

# Run with docker-compose
docker-compose up -d
```

### Dependencies
```bash
# Install dependencies
pip install -r requirements.txt

# Regenerate gRPC files (if clouddrive.proto changes)
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. clouddrive.proto
```

## Architecture

### Single-File Design
All business logic resides in `main.py` (~320 lines). The code is organized into 4 sections:
1. **Variable Configuration** (lines 30-48): Environment variables and constants
2. **Core Cleanup Logic** (lines 54-176): Recursive scanning, file deletion, empty directory cleanup
3. **Telegram Handlers** (lines 182-257): `handle_link()`, `cmd_clean()`, `cmd_blacklist()`
4. **Entry Point** (lines 260-324): Bot initialization and startup

### Key Components

| File | Purpose |
|------|---------|
| `main.py` | Main application with all handlers |
| `clouddrive_pb2.py` | gRPC protocol buffer generated code |
| `clouddrive_pb2_grpc.py` | gRPC service stub |
| `blacklist.txt` | Persistent blacklist keywords for cleanup |

### gRPC Communication Pattern
```python
async with grpc.aio.insecure_channel(CD2_IP_PORT) as channel:
    stub = clouddrive_pb2_grpc.CloudDriveFileSrvStub(channel)
    metadata = [('authorization', f'Bearer {CD2_TOKEN}')]
    # All gRPC calls require metadata and timeout (15-30s) to prevent hanging
```

### Telegram Bot v22+ Proxy Configuration
Both `request` and `get_updates_request` must be configured with proxy:
```python
from telegram.request import HTTPXRequest
q_request = HTTPXRequest(proxy=PROXY_URL, connection_pool_size=8, ...)
u_request = HTTPXRequest(proxy=PROXY_URL, ...)  # Required for getUpdates

builder = ApplicationBuilder().token(TG_BOT_TOKEN).request(q_request).get_updates_request(u_request)
```

### Scheduled Tasks
Use the built-in `JobQueue` instead of standalone `AsyncIOScheduler` to avoid event loop conflicts:
```python
application.job_queue.scheduler.add_job(
    run_auto_clean,
    CronTrigger.from_crontab(CLEAN_CRON)
)
```

### File Cleanup Logic (v1.1.4+)
Per-file deletion logic with recursive scanning:
```python
# Recursive scan of all files and directories
all_files, all_dirs = await get_all_items_recursive(stub, metadata, folder_path)

# Per-file deletion decision
for f in all_files:
    if f.size < threshold_bytes:
        # Size < threshold → delete
        files_to_delete.append(f.fullPathName)
    elif any(k.lower() in f.name.lower() for k in blacklist):
        # Size >= threshold but matches blacklist → delete
        files_to_delete.append(f.fullPathName)

# Clean empty directories (deepest first)
all_dirs.sort(key=lambda x: x.fullPathName.count('/'), reverse=True)
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| CD2_ADDRESS | Yes | 127.0.0.1:19798 | CloudDrive2 gRPC address |
| CD2_TOKEN | Yes | - | CD2 API authorization token |
| TG_TOKEN | Yes | - | Telegram Bot API token |
| ADMIN_IDS | Yes | - | Allowed user IDs (comma-separated) |
| SAVE_PATH | No | /115/离线下载 | Download save path |
| SIZE_THRESHOLD | No | 300 | Files smaller than this (MB) are deleted |
| PROXY_URL | No | - | Proxy for Telegram (http/socks5) |
| CLEAN_CRON | No | 30 3 * * * | Cleanup cron expression |
| MAX_RETRIES | No | 10 | Max network error retries |

## Critical Implementation Notes

1. **Proxy Configuration**: Both `request` and `get_updates_request` must have proxy configured, otherwise the bot won't receive messages (已读不回 issue)

2. **Scheduled Tasks**: Never use standalone `AsyncIOScheduler` - it causes event loop conflicts with gRPC/Telegram. Use the built-in `JobQueue` instead.

3. **gRPC Timeout**: All gRPC calls must have timeout (15-30s) to prevent hanging when CD2 mount points are stuck.

4. **Permission Control**: All handlers must check `update.effective_user.id in ADMIN_IDS` at the beginning.

5. **Error Handling**: The `error_handler` tracks network retry count and stops the application when `MAX_RETRIES` is exceeded. Non-network errors reset the counter.

6. **Cleanup Logic**: Files are deleted per-file, not per-folder. SIZE_THRESHOLD applies to each file individually. Blacklist only applies to files >= SIZE_THRESHOLD.

## Release Process

1. Update version in `main.py` (`__version__`) and `README.md`
2. Create and push a version tag: `git tag v1.x.x && git push origin v1.x.x`
3. GitHub Actions automatically builds and pushes Docker image to `ghcr.io/ymting/cd2_magnet_tgbot`
