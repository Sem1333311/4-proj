# LAN Messenger

## Overview
A LAN Messenger web application built with Python FastAPI. Features include user authentication with access codes, direct messaging, group chats, friend requests, voice calls (WebRTC), file sharing, user blocking, and privacy settings. The interface is in Russian.

## Project Architecture
- **Backend**: Python FastAPI with uvicorn
- **Database**: SQLite (file-based, `messenger.db`)
- **Frontend**: Vanilla HTML/CSS/JS served via Jinja2 templates
- **WebSocket**: Real-time messaging via FastAPI WebSocket support
- **File Uploads**: Stored in `uploads/` directory

## Key Files
- `app.py` - Main application (API routes, WebSocket handlers, database init)
- `templates/index.html` - Single-page application template
- `static/app.js` - Frontend JavaScript
- `static/style.css` - Styles
- `requirements.txt` - Python dependencies

## Running
- The app runs via `python app.py` which starts uvicorn on host `0.0.0.0` and port `8000` by default
- You can override the port with the `PORT` environment variable
- For local network access from a phone or another device on the same Wi-Fi, run `.\start_lan.bat` or `powershell -ExecutionPolicy Bypass -File .\start_lan.ps1`
- Open the address shown in the console, for example `http://192.168.x.x:8000`
