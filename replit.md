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
- The app runs via `python app.py` which starts uvicorn on port 5000
- PORT is configured via environment variable (set to 5000)
