// ════════════════════════════════════════════════════════════
//  LAN Messenger — app.js
//  Полная клиентская логика: UI, WebSocket, WebRTC звонки
// ════════════════════════════════════════════════════════════

// ─── Глобальное состояние ────────────────────────────────────
const state = {
    token: localStorage.getItem("token") || "",
    me: null,
    settings: null,
    chats: [],
    friends: [],
    currentChat: null,
    currentChatId: null,
    membersById: new Map(),
    ws: null,
    mediaRecorder: null,
    circleRecorder: null,

    call: {
        active: false,
        chatId: null,
        startedAt: null,
        timer: null,
        mic: true,
        cam: false,
        screen: false,
        localStream: null,
        peers: new Map(),
        tiles: new Map(),
        remoteStreams: new Map(),
        iceServers: null,
    },

    wsMeta: {
        reconnectTimer: null,
        pingTimer: null,
        pongTimer: null,
        retry: 0,
    },

    syncTimer: null,

    devicePrefs: {
        micId: "",
        camId: "",
        speakerId: "",
        audioMode: "speaker",
        camFacing: "user",
    },

    assets: [],

    ui: {
        currentTab: "chats",
        chatOpen: false,
        callMinimized: false,
        incomingCall: null,
        suppressMessageAutoScroll: false,
    },
};

// ─── Определение платформы ───────────────────────────────────
const isLikelyIOS     = /iPad|iPhone|iPod/.test(navigator.userAgent) ||
                        (navigator.platform === "MacIntel" && navigator.maxTouchPoints > 1);
const isLikelyAndroid = /Android/.test(navigator.userAgent);
const isMobile        = isLikelyIOS || isLikelyAndroid;
const isLikelySafari  = /^((?!chrome|android|crios|fxios).)*safari/i.test(navigator.userAgent);
const isLocalDevHost  = ["localhost", "127.0.0.1", "[::1]"].includes(location.hostname);

// ════════════════════════════════════════════════════════════
//  УТИЛИТЫ DOM
// ════════════════════════════════════════════════════════════

function qs(id) { return document.getElementById(id); }
function show(el) { if (el) el.classList.remove("hidden"); }
function hide(el) { if (el) el.classList.add("hidden"); }

function setError(id, text) {
    const el = qs(id);
    if (el) el.textContent = text || "";
}

function escapeHtml(v) {
    return (v || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
}

function syncViewportHeight() {
    const h = Math.round(
        window.visualViewport?.height ||
        window.innerHeight ||
        document.documentElement.clientHeight || 0
    );
    if (h > 0) document.documentElement.style.setProperty("--app-height", `${h}px`);
}

function installResponsiveEnvironment() {
    document.body.classList.toggle("platform-ios",     isLikelyIOS);
    document.body.classList.toggle("platform-android", isLikelyAndroid);
    document.body.classList.toggle("platform-safari",  isLikelySafari);
    document.body.classList.toggle("platform-mobile",  isMobile);
    syncViewportHeight();
    if (!window.__lanMessengerViewportBound) {
        window.addEventListener("resize", syncViewportHeight);
        window.visualViewport?.addEventListener("resize", syncViewportHeight);
        window.__lanMessengerViewportBound = true;
    }
}

// ════════════════════════════════════════════════════════════
//  НАВИГАЦИЯ И ВКЛАДКИ
// ════════════════════════════════════════════════════════════

function setMainTab(tab) {
    state.ui.currentTab = tab;
    const pairs = [
        ["chats",    "paneChats",    "tabChats"],
        ["requests", "paneRequests", "tabRequests"],
        ["search",   "paneSearch",   "tabSearch"],
        ["friends",  "paneFriends",  "tabFriends"],
    ];
    for (const [key, paneId, btnId] of pairs) {
        const pane = qs(paneId);
        const btn  = qs(btnId);
        if (!pane || !btn) continue;
        if (key === tab) { show(pane); btn.classList.add("active"); }
        else             { hide(pane); btn.classList.remove("active"); }
    }
    document.body.classList.remove("menu-open");
}

function setChatOpen(open) {
    state.ui.chatOpen = !!open;
    document.body.classList.toggle("chat-open", state.ui.chatOpen);
    if (window.innerWidth <= 980)
        document.body.classList.toggle("menu-open", !state.ui.chatOpen);
    const back = qs("btnBackToList");
    if (!back) return;
    if (state.ui.chatOpen) show(back); else hide(back);
}

// ════════════════════════════════════════════════════════════
//  ФОРМАТИРОВАНИЕ
// ════════════════════════════════════════════════════════════

function formatTime(iso) {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return "";
    return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function formatListTime(iso) {
    if (!iso) return "";
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return "";
    const now = new Date();
    if (d.toDateString() === now.toDateString())
        return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    return d.toLocaleDateString([], { day: "2-digit", month: "2-digit" });
}

function fmtDuration(sec) {
    const m = Math.floor(sec / 60);
    const s = sec % 60;
    return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

function truncateText(text, limit = 96) {
    const clean = String(text || "").trim();
    if (!clean) return "";
    return clean.length > limit ? `${clean.slice(0, limit).trimEnd()}…` : clean;
}

function toMultilineHtml(text) {
    return escapeHtml(text || "").replace(/\n/g, "<br>");
}

// ════════════════════════════════════════════════════════════
//  АВАТАРЫ И БЕЙДЖИ
// ════════════════════════════════════════════════════════════

function seedHue(seed) {
    const s = String(seed || "lm");
    let h = 0;
    for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) % 360;
    return Math.abs(h);
}

function initialsFromLabel(label) {
    const clean = String(label || "").trim();
    if (!clean) return "LM";
    const parts = clean.split(/\s+/).filter(Boolean);
    const letters = parts.slice(0, 2).map(p => p[0]).join("");
    return (letters || clean.slice(0, 2)).toUpperCase();
}

function withMediaToken(url) {
    if (!url || !state.token) return url;
    return `${url}${url.includes("?") ? "&" : "?"}token=${encodeURIComponent(state.token)}`;
}

function avatarMediaUrl(avatar) {
    if (!avatar) return "";
    const f = String(avatar);
    return withMediaToken(f.startsWith("/media/") ? f : `/media/${encodeURIComponent(f)}`);
}

function avatarMarkup({ avatar, label, seed, className = "avatar-md" }) {
    const cls   = ["avatar", className].filter(Boolean).join(" ");
    const title = escapeHtml(label || "LM");
    const url   = avatarMediaUrl(avatar);
    if (url) return `<img class="${cls}" src="${url}" alt="${title}" loading="lazy">`;
    return `<div class="${cls}" style="--avatar-hue:${seedHue(seed || label || "lm")}">${escapeHtml(initialsFromLabel(label))}</div>`;
}

function roleBadge(role) {
    if (role === "owner") return "👑 Владелец";
    if (role === "admin") return "🛡️ Админ";
    return "🙂 Участник";
}

function myGroupRole() {
    return state.membersById.get(state.me?.id)?.role || "member";
}

function peerNameById(id) {
    const m = state.membersById.get(id);
    return m ? `${m.nickname} @${m.username}` : `#${id}`;
}

// ════════════════════════════════════════════════════════════
//  ЗАГОЛОВОК И МЕТА ЧАТА
// ════════════════════════════════════════════════════════════

function chatTitleText(chat) {
    return chat ? (chat.title || chat.peer?.nickname || "Чат") : "Выберите чат";
}

function chatMetaText(chat) {
    if (!chat) return "Откройте диалог слева или создайте новую группу.";
    if (chat.type === "group") {
        const count = state.membersById.size || chat.member_count || 0;
        return count ? `${count} участников` : "Групповой чат";
    }
    return chat.peer?.username ? `@${chat.peer.username}` : "Личный чат";
}

function setComposerEnabled(enabled) {
    const input = qs("messageInput");
    if (input) {
        input.disabled    = !enabled;
        input.placeholder = enabled ? "Напишите сообщение…" : "Выберите чат, чтобы начать";
    }
    ["sendBtn", "btnAssets", "btnFile", "btnVoice", "btnCircle"].forEach(id => {
        const btn = qs(id);
        if (btn) btn.disabled = !enabled;
    });
}

function setEmptyState(showEmpty) {
    const empty = qs("chatEmptyState");
    const msgs  = qs("messages");
    if (showEmpty) { show(empty); hide(msgs); }
    else           { hide(empty); show(msgs); }
    setComposerEnabled(!showEmpty);
}

// ════════════════════════════════════════════════════════════
//  РЕНДЕР СООБЩЕНИЙ
// ════════════════════════════════════════════════════════════

function pickRecorderMime(kind) {
    const opts = kind === "audio"
        ? ["audio/mp4", "audio/webm;codecs=opus", "audio/webm"]
        : ["video/mp4", "video/webm;codecs=vp8,opus", "video/webm"];
    for (const m of opts)
        if (window.MediaRecorder && MediaRecorder.isTypeSupported?.(m)) return m;
    return "";
}

function extForMime(mime, fallback) {
    if (!mime) return fallback;
    if (mime.includes("mp4"))  return "mp4";
    if (mime.includes("webm")) return "webm";
    return fallback;
}

function messageBody(m) {
    const text  = toMultilineHtml(m.text || "");
    const parts = [];
    if (text) parts.push(`<div class="message-text">${text}</div>`);
    if (!m.file_url) return parts.join("");

    const fileUrl = withMediaToken(m.file_url);

    if (["image", "sticker", "emoji"].includes(m.kind)) {
        parts.push(`<img src="${fileUrl}" alt="${escapeHtml(m.file_name || "Вложение")}" loading="lazy">`);
        return parts.join("");
    }
    if (["video", "circle"].includes(m.kind)) {
        parts.push(`<video src="${fileUrl}" controls playsinline webkit-playsinline preload="metadata"></video>`);
        return parts.join("");
    }
    if (m.kind === "voice") {
        parts.push(`<audio src="${fileUrl}" controls preload="metadata"></audio>`);
        return parts.join("");
    }
    return `${text ? `${text}<br>` : ""}<a class="message-file" href="${fileUrl}" target="_blank">📎 ${escapeHtml(m.file_name || "Файл")}</a>`;
}

function appendMessage(m) {
    const mine   = m.user_id === state.me?.id;
    const isRead = mine && (m.read_count > 0);
    const item   = document.createElement("div");
    item.dataset.mid  = String(m.id);
    item.dataset.mine = mine ? "1" : "0";
    item.className    = `message ${mine ? "mine" : ""}`;

    const statusHtml = mine
        ? `<span class="msg-status ${isRead ? "read" : ""}" data-mid="${m.id}">${isRead ? "✓✓" : "✓"}</span>`
        : "";

    item.innerHTML = `
        ${mine ? "" : avatarMarkup({ avatar: m.avatar, label: m.nickname, seed: `user-${m.user_id}`, className: "avatar-sm" })}
        <div class="message-bubble">
            <div class="message-meta">
                <span class="message-author">${escapeHtml(m.nickname)}</span>
                <span class="message-username">@${escapeHtml(m.username)}</span>
                <span class="message-time">${formatTime(m.created_at)}</span>
                ${statusHtml}
            </div>
            <div class="message-content">${messageBody(m)}</div>
            <button class="msg-menu-btn ghost" title="Действия">⋮</button>
        </div>
    `;

    const container = qs("messages");
    if (container) {
        container.appendChild(item);
        if (!state.ui.suppressMessageAutoScroll) {
            container.scrollTop = container.scrollHeight;
        }
    }
}

function removeMessageById(messageId) {
    const target = qs("messages")?.querySelector(`[data-mid="${messageId}"]`);
    if (target) target.remove();
}

function updateReadStatusUpTo(upToId) {
    const container = qs("messages");
    if (!container) return;
    container.querySelectorAll(".msg-status[data-mid]").forEach(el => {
        if (Number(el.dataset.mid) <= upToId) {
            el.textContent = "✓✓";
            el.classList.add("read");
        }
    });
}

// ════════════════════════════════════════════════════════════
//  ПРОФИЛЬ И СПИСОК ЧАТОВ
// ════════════════════════════════════════════════════════════

function renderProfileMini() {
    if (!state.me) return;
    const el = qs("profileMini");
    if (!el) return;
    const about = truncateText(state.me.about, 82);
    el.innerHTML = `
        <div class="profile-card">
            ${avatarMarkup({ avatar: state.me.avatar, label: state.me.nickname, seed: `me-${state.me.id}`, className: "avatar-lg" })}
            <div class="profile-card-copy">
                <strong>${escapeHtml(state.me.nickname)}</strong>
                <span>@${escapeHtml(state.me.username)}</span>
                ${about ? `<p>${escapeHtml(about)}</p>` : ""}
            </div>
            <div class="profile-id">#${state.me.id}</div>
        </div>
    `;
}

function renderChatList(filter = "") {
    const q    = filter.trim().toLowerCase();
    const list = qs("chatList");
    if (!list) return;
    list.innerHTML = "";

    state.chats
        .filter(c => {
            if (!q) return true;
            const hay = [c.title, c.peer?.nickname, c.peer?.username, c.last_text]
                .filter(Boolean).join(" ").toLowerCase();
            return hay.includes(q);
        })
        .forEach(chat => {
            const title   = chatTitleText(chat);
            const preview = truncateText(
                chat.last_text || (chat.type === "group" ? "Групповой чат" : "Начните новый диалог"), 88
            );
            const subtitle = chat.type === "group"
                ? "Группа"
                : (chat.peer?.username ? `@${chat.peer.username}` : "Личный чат");

            const el = document.createElement("div");
            el.className = `item ${chat.id === state.currentChatId ? "selected" : ""}`;
            el.innerHTML = `
                <div class="item-head">
                    ${avatarMarkup({ avatar: chat.peer?.avatar, label: title, seed: `chat-${chat.id}-${title}`, className: "avatar-md" })}
                    <div class="item-copy">
                        <div class="item-title-row">
                            <b>${escapeHtml(title)}</b>
                            <span class="item-time">${escapeHtml(formatListTime(chat.last_at) || "")}</span>
                        </div>
                        <small>${escapeHtml(subtitle)}</small>
                        <p class="item-preview">${escapeHtml(preview)}</p>
                    </div>
                </div>
                <div class="item-tags"><span class="item-tag">${chat.type === "group" ? "Group" : "DM"}</span></div>
            `;
            el.onclick = () => openChat(chat.id);
            list.appendChild(el);
        });
}

// ════════════════════════════════════════════════════════════
//  ШАПКА ЧАТА
// ════════════════════════════════════════════════════════════

function setChatHeader(chat) {
    const titleEl      = qs("chatTitle");
    const metaEl       = qs("chatMeta");
    const avatarEl     = qs("chatAvatar");
    const btnCall      = qs("btnCallStart");
    const btnInvite    = qs("btnInviteGroup");
    const btnLeave     = qs("btnLeaveChat");
    const btnDelete    = qs("btnDeleteGroup");
    const btnMembers   = qs("btnShowMembers");
    const membersPanel = qs("groupMembersPanel");
    const stack        = document.querySelector(".chat-stack");

    const titleText = chatTitleText(chat);
    const group     = !!chat && chat.type === "group";

    if (titleEl) titleEl.textContent = titleText;
    if (metaEl)  metaEl.textContent  = chatMetaText(chat);
    if (avatarEl) {
        avatarEl.innerHTML = avatarMarkup({
            avatar:    chat?.peer?.avatar,
            label:     titleText,
            seed:      chat ? `chat-${chat.id}-${titleText}` : "empty-chat",
            className: "avatar-xl avatar-placeholder",
        });
    }

    if (chat?.can_call)           show(btnCall);   else hide(btnCall);
    if (group)                    show(btnInvite); else hide(btnInvite);
    if (group)                    show(btnMembers); else hide(btnMembers);
    if (chat)                     show(btnLeave);  else hide(btnLeave);
    if (chat?.can_delete && group) show(btnDelete); else hide(btnDelete);

    if (btnLeave)  btnLeave.title  = group ? "Выйти из группы" : "Выйти из чата";
    if (btnDelete) btnDelete.title = "Удалить группу";

    // Панель участников: на десктопе показываем для групп, на мобильном прячем (открывается по кнопке)
    if (membersPanel) {
        if (group) {
            membersPanel.removeAttribute("style");
            show(membersPanel);
            if (stack) stack.classList.add("has-members");
        } else {
            membersPanel.style.display = "none";
            if (stack) stack.classList.remove("has-members");
            closeMembersSheet();
        }
    }

    if (!chat) state.membersById = new Map();
    setEmptyState(!chat);
}

// ════════════════════════════════════════════════════════════
//  BOTTOM-SHEET УЧАСТНИКОВ (мобильные)
// ════════════════════════════════════════════════════════════

function openMembersSheet() {
    const panel    = qs("groupMembersPanel");
    const backdrop = qs("membersBackdrop");
    if (!panel) return;

    panel.classList.add("members-modal-open");
    show(panel);

    if (backdrop) {
        show(backdrop);
        requestAnimationFrame(() => {
            backdrop.classList.add("open");
            requestAnimationFrame(() => panel.classList.add("sheet-visible"));
        });
    }
    document.addEventListener("keydown", _membersEscHandler);
}

function closeMembersSheet() {
    const panel    = qs("groupMembersPanel");
    const backdrop = qs("membersBackdrop");
    if (!panel) return;

    panel.classList.remove("sheet-visible");
    if (backdrop) backdrop.classList.remove("open");

    const onEnd = () => {
        panel.classList.remove("members-modal-open");
        // На десктопе — восстанавливаем если группа открыта
        if (state.currentChat?.type === "group" && window.innerWidth > 980) {
            panel.removeAttribute("style");
            show(panel);
        } else {
            hide(panel);
        }
        if (backdrop) hide(backdrop);
        panel.removeEventListener("transitionend", onEnd);
    };
    panel.addEventListener("transitionend", onEnd);
    document.removeEventListener("keydown", _membersEscHandler);

    // Fallback если transition не сработал
    setTimeout(() => {
        if (!panel.classList.contains("sheet-visible")) {
            panel.classList.remove("members-modal-open");
            if (state.currentChat?.type !== "group" || window.innerWidth <= 980) hide(panel);
            if (backdrop) hide(backdrop);
        }
    }, 420);
}

function _membersEscHandler(e) {
    if (e.key === "Escape") closeMembersSheet();
}

// ════════════════════════════════════════════════════════════
//  API
// ════════════════════════════════════════════════════════════

async function api(path, opts = {}) {
    const headers = opts.headers || {};
    if (!(opts.body instanceof FormData)) headers["Content-Type"] = "application/json";
    if (state.token) headers.Authorization = `Bearer ${state.token}`;
    const res = await fetch(path, { ...opts, headers });
    if (!res.ok) {
        let msg = `HTTP ${res.status}`;
        try { const d = await res.json(); msg = d.detail || msg; } catch (_) {}
        throw new Error(msg);
    }
    return res.json();
}

// ════════════════════════════════════════════════════════════
//  ЗАГРУЗКА УЧАСТНИКОВ ГРУППЫ
// ════════════════════════════════════════════════════════════

async function loadMembers(chatId) {
    if (!chatId) return;
    try {
        const members = await api(`/api/chats/${chatId}/members`);
        state.membersById = new Map(members.map(m => [m.id, m]));

        const wrap = qs("chatMembers");
        if (!wrap) return;
        wrap.innerHTML = "";

        const currentRole = myGroupRole();

        members.forEach(m => {
            const el = document.createElement("div");
            el.className = "item";
            el.innerHTML = `
                <div class="item-head">
                    ${avatarMarkup({ avatar: m.avatar, label: m.nickname, seed: `member-${m.id}`, className: "avatar-sm" })}
                    <div class="item-copy">
                        <div class="item-title-row">
                            <b>${escapeHtml(m.nickname)}</b>
                            <span class="item-tag">${escapeHtml(roleBadge(m.role))}</span>
                        </div>
                        <small>@${escapeHtml(m.username)} #${m.id}</small>
                    </div>
                </div>
            `;

            const actions = document.createElement("div");
            actions.className = "actions";

            if (m.id !== state.me?.id) {
                const dm = document.createElement("button");
                dm.textContent = "💬";
                dm.title = "Личный чат";
                dm.onclick = async () => {
                    const out = await api("/api/chats/direct", { method: "POST", body: JSON.stringify({ user_id: m.id }) });
                    await loadChats();
                    await openChat(out.chat_id);
                    if (window.innerWidth <= 980) closeMembersSheet();
                };

                const callBtn = document.createElement("button");
                callBtn.textContent = "📞";
                callBtn.title = "Позвонить";
                callBtn.onclick = async () => {
                    const out = await api("/api/chats/direct", { method: "POST", body: JSON.stringify({ user_id: m.id }) });
                    await loadChats();
                    await openChat(out.chat_id);
                    if (window.innerWidth <= 980) closeMembersSheet();
                    await startCall();
                };

                actions.appendChild(dm);
                actions.appendChild(callBtn);
            }

            // Управление ролями — только owner
            if (currentRole === "owner" && m.id !== state.me?.id) {
                const promote = document.createElement("button");
                promote.className = "ghost";
                promote.textContent = m.role === "admin" ? "🙂" : "🛡️";
                promote.title      = m.role === "admin" ? "Снять админку" : "Выдать админку";
                promote.onclick = async () => {
                    try {
                        await api(`/api/groups/${chatId}/members/${m.id}/role`, {
                            method: "POST",
                            body: JSON.stringify({ role: m.role === "admin" ? "member" : "admin" }),
                        });
                        await Promise.all([loadMembers(chatId), loadChats()]);
                    } catch (e) { alert(e.message); }
                };

                const transfer = document.createElement("button");
                transfer.className = "ghost";
                transfer.textContent = "👑";
                transfer.title = "Передать владельца";
                transfer.onclick = async () => {
                    if (!confirm(`Передать owner @${m.username}?`)) return;
                    try {
                        await api(`/api/groups/${chatId}/members/${m.id}/role`, {
                            method: "POST",
                            body: JSON.stringify({ role: "owner" }),
                        });
                        await Promise.all([loadMembers(chatId), loadChats()]);
                    } catch (e) { alert(e.message); }
                };

                actions.appendChild(promote);
                actions.appendChild(transfer);
            }

            // Кик
            const canKick =
                (currentRole === "owner" && m.id !== state.me?.id) ||
                (currentRole === "admin"  && m.role === "member" && m.id !== state.me?.id);

            if (canKick) {
                const kick = document.createElement("button");
                kick.className   = "danger";
                kick.textContent = "⛔";
                kick.title = "Исключить из группы";
                kick.onclick = async () => {
                    if (!confirm(`Исключить @${m.username}?`)) return;
                    try {
                        await api(`/api/groups/${chatId}/members/${m.id}`, { method: "DELETE" });
                        await Promise.all([loadMembers(chatId), loadChats()]);
                    } catch (e) { alert(e.message); }
                };
                actions.appendChild(kick);
            }

            el.appendChild(actions);
            wrap.appendChild(el);
        });

        if (state.currentChatId === chatId) setChatHeader(state.currentChat);
    } catch (e) {
        console.error("Ошибка загрузки участников:", e);
    }
}

// ════════════════════════════════════════════════════════════
//  ОТКРЫТИЕ ЧАТА И ЗАГРУЗКА ДАННЫХ
// ════════════════════════════════════════════════════════════

async function openChat(chatId) {
    try {
        const chat = state.chats.find(c => c.id === chatId) || null;
        state.currentChat   = chat;
        state.currentChatId = chatId;
        state.membersById   = new Map();
        setChatHeader(chat);
        setChatOpen(true);

        const container = qs("messages");
        if (container) container.innerHTML = "";

        const data = await api(`/api/chats/${chatId}/messages`);
        data.forEach(appendMessage);
        await markChatRead(chatId);

        // Участников загружаем только для групп
        if (chat?.type === "group") {
            await loadMembers(chatId);
        } else {
            const wrap = qs("chatMembers");
            if (wrap) wrap.innerHTML = "";
        }
    } catch (e) {
        console.error("Ошибка открытия чата:", e);
    }
}

async function loadChats() {
    try {
        state.chats = await api("/api/chats");
        renderChatList(qs("chatSearch")?.value || "");
        if (state.currentChatId) {
            const still = state.chats.find(c => c.id === state.currentChatId);
            if (!still) {
                state.currentChat   = null;
                state.currentChatId = null;
                setChatHeader(null);
                setChatOpen(false);
                const msgEl = qs("messages");
                const memEl = qs("chatMembers");
                if (msgEl) msgEl.innerHTML = "";
                if (memEl) memEl.innerHTML = "";
            } else {
                state.currentChat = still;
                setChatHeader(still);
            }
        }
    } catch (e) { console.error("Ошибка загрузки чатов:", e); }
}

async function loadFriends() {
    try {
        state.friends = await api("/api/friends");
        const list = qs("friendsList");
        if (!list) return;
        list.innerHTML = "";
        state.friends.forEach(f => {
            const el = document.createElement("div");
            el.className = "item";
            el.innerHTML = `
                <div class="item-head">
                    ${avatarMarkup({ avatar: f.avatar, label: f.nickname, seed: `friend-${f.id}`, className: "avatar-md" })}
                    <div class="item-copy">
                        <div class="item-title-row">
                            <b>${escapeHtml(f.nickname)}</b>
                            <span class="item-tag">Friend</span>
                        </div>
                        <small>@${escapeHtml(f.username)} #${f.id}</small>
                        ${f.about ? `<p class="item-preview">${escapeHtml(truncateText(f.about, 80))}</p>` : ""}
                    </div>
                </div>
            `;
            const actions = document.createElement("div");
            actions.className = "actions";

            const dm = document.createElement("button");
            dm.textContent = "Чат";
            dm.onclick = async () => {
                const out = await api("/api/chats/direct", { method: "POST", body: JSON.stringify({ user_id: f.id }) });
                await loadChats();
                await openChat(out.chat_id);
                setMainTab("chats");
            };

            const block = document.createElement("button");
            block.className   = "danger";
            block.textContent = "Блок";
            block.onclick = async () => {
                await api(`/api/users/${f.id}/block`, { method: "POST", body: "{}" });
                await refreshSide();
            };

            actions.appendChild(dm);
            actions.appendChild(block);
            el.appendChild(actions);
            list.appendChild(el);
        });
    } catch (e) { console.error("Ошибка загрузки друзей:", e); }
}

async function loadFriendRequests() {
    try {
        const rows = await api("/api/friends/requests");
        const list = qs("friendRequests");
        if (!list) return;
        list.innerHTML = "";
        rows.forEach(r => {
            const el = document.createElement("div");
            el.className = "item";
            el.innerHTML = `
                <div class="item-head">
                    ${avatarMarkup({ avatar: r.avatar, label: r.nickname, seed: `req-${r.id}`, className: "avatar-md" })}
                    <div class="item-copy">
                        <b>${escapeHtml(r.nickname)}</b>
                        <small>@${escapeHtml(r.username)}</small>
                        <p class="item-preview">Хочет добавить вас в друзья.</p>
                    </div>
                </div>
            `;
            const actions = document.createElement("div");
            actions.className = "actions";

            const yes = document.createElement("button");
            yes.textContent = "Принять";
            yes.onclick = async () => {
                await api(`/api/friends/request/${r.id}/accept`, { method: "POST", body: "{}" });
                await refreshSide();
            };

            const no = document.createElement("button");
            no.className   = "danger";
            no.textContent = "Отклонить";
            no.onclick = async () => {
                await api(`/api/friends/request/${r.id}/reject`, { method: "POST", body: "{}" });
                await refreshSide();
            };

            actions.appendChild(yes);
            actions.appendChild(no);
            el.appendChild(actions);
            list.appendChild(el);
        });
    } catch (e) { console.error("Ошибка загрузки запросов:", e); }
}

async function loadGroupInvites() {
    try {
        const rows = await api("/api/groups/invites");
        const list = qs("groupInvites");
        if (!list) return;
        list.innerHTML = "";
        rows.forEach(r => {
            const el = document.createElement("div");
            el.className = "item";
            el.innerHTML = `
                <div class="item-head">
                    ${avatarMarkup({ label: r.chat_title || "Группа", seed: `inv-${r.id}`, className: "avatar-md" })}
                    <div class="item-copy">
                        <b>${escapeHtml(r.chat_title || "Группа")}</b>
                        <small>от @${escapeHtml(r.inviter_username)}</small>
                        <p class="item-preview">Вас приглашают в групповой чат.</p>
                    </div>
                </div>
            `;
            const actions = document.createElement("div");
            actions.className = "actions";

            const yes = document.createElement("button");
            yes.textContent = "Принять";
            yes.onclick = async () => {
                await api(`/api/groups/invites/${r.id}/accept`, { method: "POST", body: "{}" });
                await Promise.all([loadGroupInvites(), loadChats()]);
            };

            const no = document.createElement("button");
            no.className   = "danger";
            no.textContent = "Отклонить";
            no.onclick = async () => {
                await api(`/api/groups/invites/${r.id}/reject`, { method: "POST", body: "{}" });
                await loadGroupInvites();
            };

            actions.appendChild(yes);
            actions.appendChild(no);
            el.appendChild(actions);
            list.appendChild(el);
        });
    } catch (e) { console.error("Ошибка загрузки приглашений:", e); }
}

async function loadBlockedList() {
    try {
        const rows = await api("/api/blocks");
        const list = qs("blockedList");
        if (!list) return;
        list.innerHTML = "";
        rows.forEach(u => {
            const el = document.createElement("div");
            el.className = "item";
            el.innerHTML = `
                <div class="item-head">
                    ${avatarMarkup({ avatar: u.avatar, label: u.nickname, seed: `blocked-${u.id}`, className: "avatar-sm" })}
                    <div class="item-copy">
                        <b>${escapeHtml(u.nickname)}</b>
                        <small>@${escapeHtml(u.username)} #${u.id}</small>
                    </div>
                </div>
            `;
            const actions = document.createElement("div");
            actions.className = "actions";
            const un = document.createElement("button");
            un.textContent = "Разблокировать";
            un.onclick = async () => {
                await api(`/api/users/${u.id}/block`, { method: "DELETE" });
                await loadBlockedList();
                await refreshSide();
            };
            actions.appendChild(un);
            el.appendChild(actions);
            list.appendChild(el);
        });
    } catch (e) { console.error("Ошибка загрузки блокировок:", e); }
}

async function loadSettings() {
    try {
        state.settings = await api("/api/settings");
        const map = {
            setFriendReq: "allow_friend_requests",
            setCalls:     "allow_calls_from",
            setInvites:   "allow_group_invites",
            setLastSeen:  "show_last_seen",
        };
        for (const [elId, key] of Object.entries(map)) {
            const el = qs(elId);
            if (el) el.value = state.settings[key];
        }
    } catch (e) { console.error("Ошибка настроек:", e); }
}

async function loadAssets() {
    try {
        state.assets = await api("/api/assets");
        const list = qs("assetsList");
        if (!list) return;
        list.innerHTML = "";
        state.assets.forEach(a => {
            const el = document.createElement("div");
            const preview = ["emoji", "sticker"].includes(a.kind)
                ? `<img class="avatar avatar-md" src="${withMediaToken(a.file_url)}" alt="${escapeHtml(a.title || a.kind)}" loading="lazy">`
                : avatarMarkup({ label: a.kind, seed: `asset-${a.id}`, className: "avatar-md" });
            el.className = "item";
            el.innerHTML = `
                <div class="item-head">
                    ${preview}
                    <div class="item-copy">
                        <div class="item-title-row">
                            <b>${escapeHtml(a.title || a.kind)}</b>
                            <span class="item-tag">${escapeHtml(a.kind)}</span>
                        </div>
                    </div>
                </div>
            `;
            const actions = document.createElement("div");
            actions.className = "actions";

            const send = document.createElement("button");
            send.textContent = "Отправить";
            send.onclick = async () => { await sendAssetMessage(a.id); qs("assetsDialog")?.close(); };

            const del = document.createElement("button");
            del.className   = "danger";
            del.textContent = "Удалить";
            del.onclick = async () => {
                await api(`/api/assets/${a.id}`, { method: "DELETE" });
                await loadAssets();
            };

            actions.appendChild(send);
            actions.appendChild(del);
            el.appendChild(actions);
            list.appendChild(el);
        });
    } catch (e) { console.error("Ошибка загрузки стикеров:", e); }
}

// ════════════════════════════════════════════════════════════
//  ОТПРАВКА И ПРОЧТЕНИЕ СООБЩЕНИЙ
// ════════════════════════════════════════════════════════════

async function markChatRead(chatId) {
    try { await api(`/api/chats/${chatId}/read`, { method: "POST", body: "{}" }); } catch (_) {}
}

async function refreshSide() {
    try {
        await Promise.all([loadFriends(), loadFriendRequests(), loadGroupInvites(), loadChats()]);
    } catch (e) { console.error("Ошибка обновления:", e); }
}

async function sendMessage({ text = "", file = null, kind = "text" }) {
    if (!state.currentChatId) return;
    const form = new FormData();
    form.append("text", text);
    form.append("kind", kind);
    if (file) form.append("file", file, file.name || "upload.bin");
    try {
        await api(`/api/chats/${state.currentChatId}/messages`, { method: "POST", body: form });
        const input = qs("messageInput");
        if (input) input.value = "";
    } catch (e) {
        alert("Ошибка отправки: " + e.message);
    }
}

async function sendAssetMessage(assetId) {
    if (!state.currentChatId) return;
    const form = new FormData();
    form.append("asset_id", String(assetId));
    try {
        await api(`/api/chats/${state.currentChatId}/messages/asset`, { method: "POST", body: form });
    } catch (e) { alert("Ошибка отправки стикера: " + e.message); }
}

// ════════════════════════════════════════════════════════════
//  ВСТАВКА ИЗОБРАЖЕНИЯ ИЗ БУФЕРА ОБМЕНА (Ctrl+V / ⌘+V)
// ════════════════════════════════════════════════════════════

document.addEventListener("paste", async (e) => {
    if (!state.currentChatId) return;

    // Игнорируем вставку в другие поля ввода
    if (e.target.tagName === "INPUT" || e.target.tagName === "TEXTAREA") {
        if (e.target.id !== "messageInput") return;
    }

    const items = e.clipboardData?.items;
    if (!items) return;

    for (const item of items) {
        if (!item.type.startsWith("image/")) continue;

        const file = item.getAsFile();
        if (!file) continue;

        e.preventDefault();

        const ext   = item.type.split("/")[1] || "png";
        const named = new File([file], `paste_${Date.now()}.${ext}`, { type: item.type });

        const input = qs("messageInput");
        const oldPh = input?.placeholder;
        if (input) { input.placeholder = "Отправка…"; input.disabled = true; }

        try {
            await sendMessage({ file: named, kind: "image" });
        } catch (err) {
            alert("Ошибка вставки: " + err.message);
        } finally {
            if (input) { input.placeholder = oldPh; input.disabled = false; input.focus(); }
        }
        return;
    }
});

// ════════════════════════════════════════════════════════════
//  ЗАПИСЬ ГОЛОСОВЫХ И КРУЖКОВ
// ════════════════════════════════════════════════════════════

async function startVoiceRecord() {
    if (state.mediaRecorder) { state.mediaRecorder.stop(); return; }
    try {
        const stream   = await navigator.mediaDevices.getUserMedia({ audio: true });
        const chunks   = [];
        const mimeType = pickRecorderMime("audio");
        const rec      = mimeType ? new MediaRecorder(stream, { mimeType }) : new MediaRecorder(stream);
        state.mediaRecorder = rec;

        const btn = qs("btnVoice");
        if (btn) { btn.textContent = "⏹ Стоп"; btn.classList.add("recording"); }

        rec.ondataavailable = e => chunks.push(e.data);
        rec.onstop = async () => {
            const outMime = rec.mimeType || mimeType || "audio/webm";
            const ext     = extForMime(outMime, "webm");
            const blob    = new Blob(chunks, { type: outMime });
            const file    = new File([blob], `voice_${Date.now()}.${ext}`, { type: outMime });
            await sendMessage({ file, kind: "voice" });
            stream.getTracks().forEach(t => t.stop());
            state.mediaRecorder = null;
            if (btn) { btn.textContent = "🎤"; btn.classList.remove("recording"); }
        };
        rec.start();
    } catch (e) { alert("Ошибка микрофона: " + e.message); }
}

async function startCircleRecord() {
    if (state.circleRecorder) { state.circleRecorder.stop(); return; }
    try {
        const stream   = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
        const chunks   = [];
        const mimeType = pickRecorderMime("video");
        const rec      = mimeType ? new MediaRecorder(stream, { mimeType }) : new MediaRecorder(stream);
        state.circleRecorder = rec;

        const btn = qs("btnCircle");
        if (btn) { btn.textContent = "⏹ Стоп"; btn.classList.add("recording"); }

        rec.ondataavailable = e => chunks.push(e.data);
        rec.onstop = async () => {
            const outMime = rec.mimeType || mimeType || "video/webm";
            const ext     = extForMime(outMime, "webm");
            const blob    = new Blob(chunks, { type: outMime });
            const file    = new File([blob], `circle_${Date.now()}.${ext}`, { type: outMime });
            await sendMessage({ file, kind: "circle" });
            stream.getTracks().forEach(t => t.stop());
            state.circleRecorder = null;
            if (btn) { btn.textContent = "📹"; btn.classList.remove("recording"); }
        };
        rec.start();
    } catch (e) { alert("Ошибка камеры: " + e.message); }
}

// ════════════════════════════════════════════════════════════
//  КОНТЕКСТНОЕ МЕНЮ СООБЩЕНИЙ (ПКМ / долгое нажатие / кнопка ⋮)
// ════════════════════════════════════════════════════════════

let _ctxMenuEl = null;
let _ctxLongPressTimer = null;
let _messageContextMenuBound = false;

function hideContextMenu() {
    if (_ctxMenuEl) { _ctxMenuEl.remove(); _ctxMenuEl = null; }
}

function showMessageContextMenu(x, y, msgEl) {
    hideContextMenu();
    const mid    = Number(msgEl.dataset.mid);
    const isMine = msgEl.dataset.mine === "1";
    if (!mid) return;

    const menu = document.createElement("div");
    menu.className = "msg-ctx-menu";
    _ctxMenuEl     = menu;

    const btnDelMe = document.createElement("button");
    btnDelMe.textContent = "🗑️ Удалить у себя";
    btnDelMe.onclick = async () => {
        hideContextMenu();
        try {
            await api(`/api/messages/${mid}?mode=me`, { method: "DELETE" });
            msgEl.remove();
        } catch (e) { alert(e.message); }
    };
    menu.appendChild(btnDelMe);

    if (isMine) {
        const btnDelAll = document.createElement("button");
        btnDelAll.className   = "danger";
        btnDelAll.textContent = "🗑️ Удалить у всех";
        btnDelAll.onclick = async () => {
            hideContextMenu();
            if (!confirm("Удалить сообщение у всех?")) return;
            try { await api(`/api/messages/${mid}?mode=all`, { method: "DELETE" }); }
            catch (e) { alert(e.message); }
        };
        menu.appendChild(btnDelAll);
    }

    document.body.appendChild(menu);
    menu.style.cssText = `position:fixed;left:${x}px;top:${y}px;z-index:200`;

    requestAnimationFrame(() => {
        const rect = menu.getBoundingClientRect();
        if (rect.right  > window.innerWidth  - 8) menu.style.left = `${window.innerWidth  - rect.width  - 8}px`;
        if (rect.bottom > window.innerHeight - 8) menu.style.top  = `${y - rect.height - 4}px`;
    });

    setTimeout(() => document.addEventListener("click", hideContextMenu, { once: true }), 0);
}

function bindMessageContextMenu(container) {
    if (!container || _messageContextMenuBound) return;
    _messageContextMenuBound = true;

    // ПКМ — десктоп
    container.addEventListener("contextmenu", e => {
        const msgEl = e.target.closest(".message[data-mid]");
        if (!msgEl) return;
        e.preventDefault();
        showMessageContextMenu(e.clientX, e.clientY, msgEl);
    });

    // Кнопка ⋮
    container.addEventListener("click", e => {
        const btn = e.target.closest(".msg-menu-btn");
        if (!btn) return;
        const msgEl = btn.closest(".message[data-mid]");
        if (!msgEl) return;
        e.stopPropagation();
        const rect = btn.getBoundingClientRect();
        showMessageContextMenu(rect.left, rect.bottom + 4, msgEl);
    });

    // Долгое нажатие — мобильные
    container.addEventListener("touchstart", e => {
        const msgEl = e.target.closest(".message[data-mid]");
        if (!msgEl) return;
        const touch = e.touches[0];
        _ctxLongPressTimer = setTimeout(() => {
            if (navigator.vibrate) navigator.vibrate(30);
            showMessageContextMenu(touch.clientX, touch.clientY, msgEl);
        }, 550);
    }, { passive: true });

    container.addEventListener("touchend",  () => clearTimeout(_ctxLongPressTimer), { passive: true });
    container.addEventListener("touchmove", () => clearTimeout(_ctxLongPressTimer), { passive: true });
}

// ════════════════════════════════════════════════════════════
//  WEBRTC — УСТРОЙСТВА
// ════════════════════════════════════════════════════════════

async function listMediaDevices() {
    try {
        const d = await navigator.mediaDevices.enumerateDevices();
        return {
            mics:     d.filter(x => x.kind === "audioinput"),
            cams:     d.filter(x => x.kind === "videoinput"),
            speakers: d.filter(x => x.kind === "audiooutput"),
        };
    } catch (_) { return { mics: [], cams: [], speakers: [] }; }
}

function fillSelect(selectEl, devices, selectedId, fallbackLabel) {
    if (!selectEl) return;
    selectEl.innerHTML = "";
    const auto = document.createElement("option");
    auto.value = ""; auto.textContent = fallbackLabel;
    selectEl.appendChild(auto);
    devices.forEach(d => {
        const opt = document.createElement("option");
        opt.value       = d.deviceId;
        opt.textContent = d.label || d.deviceId.slice(0, 10);
        if (selectedId && selectedId === d.deviceId) opt.selected = true;
        selectEl.appendChild(opt);
    });
}

async function refreshDevicePanel() {
    try { await navigator.mediaDevices.getUserMedia({ audio: true, video: true }); } catch (_) {}
    const { mics, cams, speakers } = await listMediaDevices();
    fillSelect(qs("selMic"),     mics,     state.devicePrefs.micId,     "Системный микрофон");
    fillSelect(qs("selCam"),     cams,     state.devicePrefs.camId,     "Системная камера");
    fillSelect(qs("selSpeaker"), speakers, state.devicePrefs.speakerId, "Системный вывод");

    const speakerSel = qs("selSpeaker");
    if (speakerSel) {
        const hasSinkId = "setSinkId" in HTMLMediaElement.prototype;
        speakerSel.disabled = !hasSinkId;
        speakerSel.title    = hasSinkId ? "" : "Safari/iOS не поддерживает выбор вывода";
    }
    const modeSel = qs("selAudioMode");
    if (modeSel) modeSel.value = state.devicePrefs.audioMode || "speaker";
}

async function applySpeakerToMedia(mediaEl) {
    if (!mediaEl || typeof mediaEl.setSinkId !== "function") return;
    try { await mediaEl.setSinkId(state.devicePrefs.speakerId || ""); } catch (_) {}
}

async function applySpeakerToAllTiles() {
    for (const [uid, card] of state.call.tiles) {
        if (uid !== state.me?.id) await applySpeakerToMedia(card.video);
    }
}

async function applyAudioMode(mode) {
    state.devicePrefs.audioMode = mode || "speaker";
    const audioSession = navigator.audioSession;
    if (audioSession && "type" in audioSession) {
        try { audioSession.type = mode === "phone" ? "play-and-record" : "playback"; } catch (_) {}
    }
    if (!state.call.active) return;
    if (isLikelyIOS && isLikelySafari && !("setSinkId" in HTMLMediaElement.prototype)) {
        await applySpeakerToAllTiles(); return;
    }
    if (!state.devicePrefs.speakerId) {
        try {
            const { speakers } = await listMediaDevices();
            if (speakers.length) {
                const needle = mode === "phone" ? ["head","ear","phone"] : ["speaker","spk","loud"];
                const pref = speakers.find(s => needle.some(n => (s.label || "").toLowerCase().includes(n)));
                if (pref) state.devicePrefs.speakerId = pref.deviceId;
            }
        } catch (_) {}
    }
    await applySpeakerToAllTiles();
}

function sendCallState() {
    if (state.ws?.readyState !== WebSocket.OPEN || !state.call.active) return;
    state.ws.send(JSON.stringify({
        type: "call:state",
        chat_id: state.call.chatId,
        mic: state.call.mic,
        cam: state.call.cam,
        screen: state.call.screen,
    }));
}

async function createLocalAudioIfMissing() {
    if (!state.call.localStream) {
        state.call.localStream = new MediaStream();
    }
    if (state.call.localStream.getAudioTracks().length > 0) return;

    const micStream = await navigator.mediaDevices.getUserMedia({
        audio: state.devicePrefs.micId ? { deviceId: { exact: state.devicePrefs.micId } } : true,
        video: false,
    });
    const track = micStream.getAudioTracks()[0];
    if (!track) {
        throw new Error("Не удалось получить микрофон");
    }
    track.enabled = state.call.mic;
    state.call.localStream.addTrack(track);
}

function getPeerSender(pc, kind) {
    if (!pc) return null;
    const cached = kind === "audio" ? pc._audioSender : pc._videoSender;
    if (cached) return cached;

    const sender = pc.getSenders().find(item => item.track?.kind === kind) || null;
    if (kind === "audio") pc._audioSender = sender;
    if (kind === "video") pc._videoSender = sender;
    return sender;
}

async function renegotiatePeer(userId) {
    const uid = Number(userId);
    const pc = state.call.peers.get(uid);
    if (!pc || pc._makingOffer || pc.signalingState !== "stable") return;

    try {
        pc._makingOffer = true;
        const offer = await pc.createOffer({
            offerToReceiveAudio: true,
            offerToReceiveVideo: true,
        });
        await pc.setLocalDescription(offer);
        if (state.ws?.readyState === WebSocket.OPEN) {
            state.ws.send(JSON.stringify({
                type: "call:signal",
                chat_id: state.call.chatId,
                to_user: uid,
                signal: { type: "offer", sdp: pc.localDescription },
            }));
        }
    } catch (e) {
        console.error("Renegotiation failed:", e);
    } finally {
        pc._makingOffer = false;
    }
}

async function renegotiateAllPeers() {
    const jobs = [];
    for (const uid of state.call.peers.keys()) {
        jobs.push(renegotiatePeer(uid));
    }
    await Promise.allSettled(jobs);
}

function getVideoConstraints() {
    if (state.devicePrefs.camId) return { deviceId: { exact: state.devicePrefs.camId } };
    if (isMobile) {
        const facing = state.devicePrefs.camFacing || "user";
        return { facingMode: isLikelyIOS ? { exact: facing } : { ideal: facing } };
    }
    return true;
}

function attachTrackToPeer(userId, track, streamHint = null) {
    const card = ensureCallTile(userId, false);
    if (!card?.video || !track) return;

    let remoteStream = state.call.remoteStreams.get(userId);
    if (!remoteStream) {
        remoteStream = new MediaStream();
        state.call.remoteStreams.set(userId, remoteStream);
    }

    const tracksToAdd = streamHint?.getTracks?.().length ? streamHint.getTracks() : [track];
    tracksToAdd.forEach(item => {
        if (!remoteStream.getTracks().some(existing => existing.id === item.id)) {
            remoteStream.addTrack(item);
        }
    });

    card.video.srcObject = remoteStream;
    card.video.style.transform = "none";
    applySpeakerToMedia(card.video);
    safePlay(card.video);
}

function stopCallTimer() {
    if (state.call.timer) {
        clearInterval(state.call.timer);
        state.call.timer = null;
    }
}

function stopAllLocalTracks() {
    state.call.localStream?.getTracks().forEach(track => track.stop());
}

function resetCallState() {
    for (const uid of Array.from(state.call.peers.keys())) {
        removePeer(uid);
    }

    stopAllLocalTracks();
    state.call.localStream = null;
    state.call.active = false;
    state.call.chatId = null;
    state.call.startedAt = null;
    state.call.screen = false;
    state.call.cam = false;
    state.call.mic = true;
    state.call.iceServers = null;
    state.call.tiles.clear();
    state.call.remoteStreams.clear();
    stopCallTimer();

    hide(qs("callOverlay"));
    hide(qs("btnCallRestore"));
    const callGrid = qs("callGrid");
    if (callGrid) callGrid.innerHTML = "";
    const label = qs("callTitleLabel");
    if (label) label.textContent = "Звонок";
    updateCallButtons();
    state.ui.callMinimized = false;
}

function resetCallPeersForRejoin() {
    for (const uid of Array.from(state.call.peers.keys())) {
        removePeer(uid);
    }

    const callGrid = qs("callGrid");
    if (callGrid) callGrid.innerHTML = "";
    state.call.tiles.clear();
    state.call.remoteStreams.clear();

    if (!state.call.localStream) return;
    const localCard = ensureCallTile(state.me?.id, true);
    if (!localCard) return;
    localCard.video.srcObject = new MediaStream(state.call.localStream.getTracks());
    safePlay(localCard.video);
    setTileState(state.me?.id, {
        mic: state.call.mic,
        cam: state.call.cam,
        screen: state.call.screen,
    });
}

async function syncCurrentChatIfOpen() {
    if (!state.currentChatId) return;
    const container = qs("messages");
    if (!container) return;

    const wasNearBottom = container.scrollHeight - container.scrollTop - container.clientHeight < 60;
    const prevScrollTop = container.scrollTop;
    const prevScrollHeight = container.scrollHeight;
    const data = await api(`/api/chats/${state.currentChatId}/messages`);

    state.ui.suppressMessageAutoScroll = true;
    try {
        container.innerHTML = "";
        data.forEach(appendMessage);
    } finally {
        state.ui.suppressMessageAutoScroll = false;
    }

    if (wasNearBottom) {
        container.scrollTop = container.scrollHeight;
    } else {
        const nextScrollHeight = container.scrollHeight;
        container.scrollTop = Math.max(0, prevScrollTop + (nextScrollHeight - prevScrollHeight));
    }
}

async function switchMicDevice(deviceId) {
    state.devicePrefs.micId = deviceId || "";
    if (!state.call.active || !state.call.localStream) return;
    try {
        const stream = await navigator.mediaDevices.getUserMedia({
            audio: state.devicePrefs.micId ? { deviceId: { exact: state.devicePrefs.micId } } : true,
            video: false,
        });
        const track = stream.getAudioTracks()[0];
        if (!track) return;
        track.enabled = state.call.mic;
        state.call.localStream.getAudioTracks().forEach(t => { t.stop(); state.call.localStream.removeTrack(t); });
        state.call.localStream.addTrack(track);
        for (const pc of state.call.peers.values()) {
            const sender = getPeerSender(pc, "audio");
            if (sender) await sender.replaceTrack(track).catch(() => {});
        }
    } catch (e) { console.error("Ошибка смены микрофона:", e); }
}

async function switchCamDevice(deviceId) {
    state.devicePrefs.camId = deviceId || "";
    if (!state.call.active || !state.call.cam || state.call.screen) return;
    try {
        const stream = await navigator.mediaDevices.getUserMedia({
            video: getVideoConstraints(),
            audio: false,
        });
        const track = stream.getVideoTracks()[0];
        if (!track) return;
        state.call.localStream.getVideoTracks().forEach(t => { t.stop(); state.call.localStream.removeTrack(t); });
        state.call.localStream.addTrack(track);
        for (const pc of state.call.peers.values()) {
            const sender = getPeerSender(pc, "video");
            if (sender) await sender.replaceTrack(track).catch(() => {});
        }
        const localCard = state.call.tiles.get(state.me?.id);
        if (localCard) {
            localCard.video.srcObject = new MediaStream(state.call.localStream.getTracks());
            await safePlay(localCard.video);
        }
        await renegotiateAllPeers();
        updateCallButtons();
    } catch (e) { console.error("Ошибка смены камеры:", e); }
}

async function rotateCamera() {
    if (isMobile) {
        state.devicePrefs.camFacing = state.devicePrefs.camFacing === "environment" ? "user" : "environment";
        state.devicePrefs.camId     = "";
    } else {
        const { cams } = await listMediaDevices();
        if (cams.length > 1) {
            const idx  = Math.max(0, cams.findIndex(c => c.deviceId === state.devicePrefs.camId));
            const next = cams[(idx + 1) % cams.length];
            if (next) state.devicePrefs.camId = next.deviceId;
        }
    }
    if (state.call.active && state.call.cam && !state.call.screen)
        await switchCamDevice(state.devicePrefs.camId);
}

// ════════════════════════════════════════════════════════════
//  WEBRTC — ТАЙЛЫ И ИНДИКАТОРЫ ЗВОНКА
// ════════════════════════════════════════════════════════════

function updateCallTimer() {
    const el = qs("callTimer");
    if (!state.call.startedAt) { if (el) el.textContent = "00:00"; return; }
    const sec = Math.max(0, Math.floor((Date.now() - state.call.startedAt) / 1000));
    if (el) el.textContent = fmtDuration(sec);
}

function updateCallButtons() {
    const btnMic    = qs("btnToggleMic");
    const btnCam    = qs("btnToggleCam");
    const btnScreen = qs("btnShareScreen");
    const btnRotate = qs("btnRotateCam");

    if (btnMic) {
        btnMic.textContent = state.call.mic ? "🎤" : "🔇";
        btnMic.title       = state.call.mic ? "Выключить микрофон" : "Включить микрофон";
    }
    if (btnCam) {
        btnCam.textContent = state.call.cam ? (state.call.screen ? "🖥️" : "📷") : "🚫";
        btnCam.title       = state.call.cam ? "Выключить камеру" : "Включить камеру";
    }
    if (btnScreen) {
        const canShare = !isMobile && !!navigator.mediaDevices?.getDisplayMedia;
        btnScreen.textContent = state.call.screen ? "🖥️" : "🪟";
        btnScreen.disabled    = !canShare;
        if (canShare) show(btnScreen); else hide(btnScreen);
    }
    if (btnRotate) {
        btnRotate.disabled = !state.call.active || state.call.screen;
        if (state.call.active) show(btnRotate); else hide(btnRotate);
    }
}

async function safePlay(mediaEl) {
    if (!mediaEl) return;
    try {
        await mediaEl.play();
    } catch (e) {
        // iOS/Android blocks autoplay with audio — try muted first
        if (!mediaEl.muted) {
            mediaEl.muted = true;
            try {
                await mediaEl.play();
                // Show unmute button on the tile
                showUnmuteOverlay(mediaEl);
            } catch (e2) {
                console.warn("Autoplay blocked even muted:", e2);
                showPlayOverlay(mediaEl);
            }
        } else {
            showPlayOverlay(mediaEl);
        }
    }
}

function showUnmuteOverlay(mediaEl) {
    const parent = mediaEl.parentElement;
    if (!parent || parent.querySelector(".call-unmute")) return;
    const btn = document.createElement("button");
    btn.className = "call-unmute";
    btn.textContent = "🔇 Нажмите для включения звука";
    btn.onclick = async (e) => {
        e.stopPropagation();
        mediaEl.muted = false;
        btn.remove();
        try { await mediaEl.play(); } catch (_) {}
    };
    parent.appendChild(btn);
}

function showPlayOverlay(mediaEl) {
    const parent = mediaEl.parentElement;
    if (!parent || parent.querySelector(".call-unmute")) return;
    const btn = document.createElement("button");
    btn.className = "call-unmute";
    btn.textContent = "▶ Нажмите для просмотра";
    btn.onclick = async (e) => {
        e.stopPropagation();
        mediaEl.muted = false;
        btn.remove();
        try { await mediaEl.play(); } catch (_) {}
    };
    parent.appendChild(btn);
}

// Unlock all paused/muted remote video elements on any user tap
document.addEventListener("click", () => {
    for (const [uid, card] of state.call.tiles) {
        if (uid === state.me?.id) continue;
        if (card.video && card.video.srcObject) {
            const overlay = card.tile?.querySelector(".call-unmute");
            if (overlay) {
                card.video.muted = false;
                overlay.remove();
            }
            if (card.video.paused) card.video.play().catch(() => {});
        }
    }
}, { passive: true });

function ensureCallTile(userId, isLocal = false) {
    userId = Number(userId);
    if (isNaN(userId)) return null;
    if (state.call.tiles.has(userId)) return state.call.tiles.get(userId);

    const wrap = qs("callGrid");
    if (!wrap) return null;

    // Убираем зависшие дубликаты
    wrap.querySelectorAll(`.call-tile[data-uid="${userId}"]`).forEach(t => t.remove());

    const tile  = document.createElement("div");
    tile.className   = `call-tile${isLocal ? " local" : ""}`;
    tile.dataset.uid = userId;

    const video = document.createElement("video");
    video.autoplay = true;
    video.playsInline = true;
    video.setAttribute("playsinline", "");
    video.setAttribute("webkit-playsinline", "");
    video.muted  = isLocal;
    video.style.transform = isLocal ? "scaleX(-1)" : "none";

    const who   = document.createElement("div"); who.className = "who";
    const left  = document.createElement("span"); left.textContent  = isLocal ? "Вы" : peerNameById(userId);
    const right = document.createElement("span"); right.textContent = "mic:on cam:off";
    who.appendChild(left); who.appendChild(right);

    tile.appendChild(video);
    tile.appendChild(who);
    wrap.appendChild(tile);

    const card = { tile, video, right };
    state.call.tiles.set(userId, card);
    return card;
}

function setTileState(userId, payload = {}) {
    const card = ensureCallTile(userId, userId === state.me?.id);
    if (!card) return;
    const mic = payload.mic ? "on" : "off";
    const cam = payload.cam ? (payload.screen ? "screen" : "on") : "off";
    card.right.textContent = `mic:${mic} cam:${cam}`;
    if (userId !== state.me?.id && cam !== "off" && card.video?.srcObject && card.video.paused)
        safePlay(card.video);
}

function removePeer(userId) {
    userId = Number(userId);
    const pc = state.call.peers.get(userId);
    if (pc) try { pc.close(); } catch (_) {}
    state.call.peers.delete(userId);
    state.call.remoteStreams?.delete(userId);
    const card = state.call.tiles.get(userId);
    if (card) card.tile.remove();
    state.call.tiles.delete(userId);
    qs("callGrid")?.querySelectorAll(`.call-tile[data-uid="${userId}"]`).forEach(t => t.remove());
}

// ════════════════════════════════════════════════════════════
//  WEBRTC — СОЗДАНИЕ PEER-СОЕДИНЕНИЯ (Perfect Negotiation)
//
//  Паттерн:
//   • addTrack() → автоматически запускает onnegotiationneeded
//   • Offerer создаётся на стороне с ID ≥
//   • ICE-кандидаты буферируются до setRemoteDescription
// ════════════════════════════════════════════════════════════

async function ensurePeer(userId, createOffer = false) {
    userId = Number(userId);
    if (state.call.peers.has(userId)) return state.call.peers.get(userId);

    const defaultIceServers = [
        { urls: "stun:stun.l.google.com:19302" },
        { urls: "stun:stun1.l.google.com:19302" },
    ];
    const iceServers = isLocalDevHost ? [] : (state.call.iceServers || defaultIceServers);

    const pc = new RTCPeerConnection({ iceServers, iceCandidatePoolSize: isLocalDevHost ? 0 : 4 });
    pc._makingOffer = false;
    pc._ignoreOffer = false;
    pc._pendingCandidates = [];

    pc._audioSender = pc.addTransceiver("audio", { direction: "sendrecv" }).sender;
    pc._videoSender = pc.addTransceiver("video", { direction: "sendrecv" }).sender;

    const localAudio = state.call.localStream?.getAudioTracks()[0] || null;
    const localVideo = state.call.localStream?.getVideoTracks()[0] || null;
    if (localAudio) await pc._audioSender.replaceTrack(localAudio);
    if (localVideo) await pc._videoSender.replaceTrack(localVideo);

    // Приём потока от удалённого участника
    pc.ontrack = ev => {
        const remoteTrack = ev.track;
        const remoteStream = ev.streams?.[0] || null;
        attachTrackToPeer(userId, remoteTrack, remoteStream);
        remoteTrack.onunmute = () => attachTrackToPeer(userId, remoteTrack, remoteStream);
        remoteTrack.onended = () => {
            const stored = state.call.remoteStreams.get(userId);
            if (!stored) return;
            stored.getTracks().forEach(existing => {
                if (existing.id === remoteTrack.id) stored.removeTrack(existing);
            });
        };
        // iOS/Android: повтор если autoplay не сработал
        setTimeout(() => {
            const card = state.call.tiles.get(userId);
            if (state.call.active && card?.video?.paused && card.video.srcObject) {
                safePlay(card.video);
            }
        }, 900);
    };

    // Отправка ICE-кандидатов
    pc.onicecandidate = ev => {
        if (!ev.candidate || state.ws?.readyState !== WebSocket.OPEN) return;
        state.ws.send(JSON.stringify({
            type: "call:signal", chat_id: state.call.chatId,
            to_user: userId,
            signal: { type: "candidate", candidate: ev.candidate },
        }));
    };

    pc.onconnectionstatechange = () => {
        console.log(`Peer ${userId}: ${pc.connectionState}`);
        if (pc.connectionState === "failed") try { pc.restartIce(); } catch (_) {}
    };

    state.call.peers.set(userId, pc);
    if (createOffer) {
        await renegotiatePeer(userId);
    }
    return pc;
}

// ════════════════════════════════════════════════════════════
//  WEBRTC — ОБРАБОТКА СИГНАЛОВ
// ════════════════════════════════════════════════════════════

async function handleSignal(fromUser, signal) {
    if (!state.call.active) return;
    fromUser = Number(fromUser);

    const pc       = await ensurePeer(fromUser);
    const isPolite = state.me?.id < fromUser;

    try {
        if (signal.type === "offer") {
            const collision = pc._makingOffer || pc.signalingState !== "stable";
            pc._ignoreOffer = !isPolite && collision;
            if (pc._ignoreOffer) return;

            await pc.setRemoteDescription(new RTCSessionDescription(signal.sdp));

            // Сброс буфера ICE
            for (const c of pc._pendingCandidates) {
                try { await pc.addIceCandidate(new RTCIceCandidate(c)); } catch (_) {}
            }
            pc._pendingCandidates = [];

            // Явный createAnswer() — Safari не поддерживает setLocalDescription() без аргументов
            const answer = await pc.createAnswer();
            await pc.setLocalDescription(answer);
            if (state.ws?.readyState === WebSocket.OPEN) {
                state.ws.send(JSON.stringify({
                    type: "call:signal", chat_id: state.call.chatId,
                    to_user: fromUser,
                    signal: { type: "answer", sdp: pc.localDescription },
                }));
            }

        } else if (signal.type === "answer") {
            if (pc.signalingState !== "have-local-offer") return;
            await pc.setRemoteDescription(new RTCSessionDescription(signal.sdp));
            // Сброс буфера ICE
            for (const c of pc._pendingCandidates) {
                try { await pc.addIceCandidate(new RTCIceCandidate(c)); } catch (_) {}
            }
            pc._pendingCandidates = [];

        } else if (signal.type === "candidate") {
            if (pc._ignoreOffer) return;
            if (!pc.remoteDescription) {
                pc._pendingCandidates.push(signal.candidate);
            } else {
                try { await pc.addIceCandidate(new RTCIceCandidate(signal.candidate)); }
                catch (e) { if (!pc._ignoreOffer) console.warn("ICE error:", e); }
            }
        }
    } catch (err) {
        console.error("Signal error:", err);
    }
}

// ════════════════════════════════════════════════════════════
//  ЗВОНОК — СТАРТ И ЗАВЕРШЕНИЕ
// ════════════════════════════════════════════════════════════

async function startCall() {
    if (!state.currentChat || state.call.active) return;
    if (!state.ws || state.ws.readyState !== WebSocket.OPEN) {
        connectWs();
        alert("Соединение восстанавливается. Попробуйте начать звонок через 1-2 секунды.");
        return;
    }

    try {
        let iceServers = [
            { urls: "stun:stun.l.google.com:19302" },
            { urls: "stun:stun1.l.google.com:19302" },
        ];
        try {
            const cfg = await api("/api/rtc-config");
            if (!isLocalDevHost && cfg.ice_servers?.length) iceServers = cfg.ice_servers;
        } catch (_) {}

        for (const uid of Array.from(state.call.peers.keys())) {
            removePeer(uid);
        }
        stopAllLocalTracks();
        stopCallTimer();

        state.call.active = true;
        state.call.chatId = state.currentChatId;
        state.call.startedAt = Date.now();
        state.call.mic = true;
        state.call.cam = false;
        state.call.screen = false;
        state.call.iceServers = isLocalDevHost ? [] : iceServers;
        state.call.peers.clear();
        state.call.tiles.clear();
        state.call.remoteStreams.clear();

        const grid = qs("callGrid");
        if (grid) grid.innerHTML = "";

        state.call.localStream = new MediaStream();
        await createLocalAudioIfMissing();

        const localCard = ensureCallTile(state.me?.id, true);
        if (localCard) {
            localCard.video.srcObject = new MediaStream(state.call.localStream.getTracks());
            await safePlay(localCard.video);
        }
        setTileState(state.me?.id, { mic: true, cam: false });

        const lbl = qs("callTitleLabel");
        if (lbl) lbl.textContent = `Звонок: ${state.currentChat.title || state.currentChat.peer?.nickname || "чат"}`;

        show(qs("callOverlay"));
        hide(qs("btnCallRestore"));
        updateCallButtons();
        updateCallTimer();
        state.call.timer = setInterval(updateCallTimer, 1000);

        state.ws.send(JSON.stringify({
            type: "call:join", chat_id: state.call.chatId,
            mic: state.call.mic, cam: state.call.cam, screen: state.call.screen,
        }));

        await applyAudioMode(state.devicePrefs.audioMode || "speaker");
    } catch (e) {
        alert("Не удалось запустить звонок: " + e.message);
        resetCallState();
    }
}

function leaveCall() {
    if (!state.call.active) return;
    if (state.ws?.readyState === WebSocket.OPEN)
        state.ws.send(JSON.stringify({ type: "call:leave", chat_id: state.call.chatId }));
    resetCallState();
}

// ════════════════════════════════════════════════════════════
//  ЗВОНОК — УПРАВЛЕНИЕ МЕДИА
// ════════════════════════════════════════════════════════════

async function toggleMic() {
    if (!state.call.active || !state.call.localStream) return;
    state.call.mic = !state.call.mic;
    state.call.localStream.getAudioTracks().forEach(t => { t.enabled = state.call.mic; });
    setTileState(state.me?.id, { mic: state.call.mic, cam: state.call.cam, screen: state.call.screen });
    updateCallButtons();
    sendCallState();
}

async function toggleCam() {
    if (!state.call.active || !state.call.localStream) return;

    if (state.call.cam) {
        state.call.cam    = false;
        state.call.screen = false;
        state.call.localStream.getVideoTracks().forEach(t => { t.stop(); state.call.localStream.removeTrack(t); });
        for (const pc of state.call.peers.values()) {
            const sender = getPeerSender(pc, "video");
            if (sender) await sender.replaceTrack(null).catch(() => {});
        }
        await renegotiateAllPeers();
    } else {
        try {
            const stream = await navigator.mediaDevices.getUserMedia({ video: getVideoConstraints(), audio: false });
            const track  = stream.getVideoTracks()[0];
            if (!track) return;
            state.call.localStream.getVideoTracks().forEach(t => { t.stop(); state.call.localStream.removeTrack(t); });
            state.call.localStream.addTrack(track);
            state.call.cam    = true;
            state.call.screen = false;
            for (const pc of state.call.peers.values()) {
                const sender = getPeerSender(pc, "video");
                if (sender) await sender.replaceTrack(track).catch(() => {});
            }
            await renegotiateAllPeers();
        } catch (e) { alert("Не удалось включить камеру: " + e.message); return; }
    }

    const localCard = state.call.tiles.get(state.me?.id);
    if (localCard) {
        localCard.video.srcObject = new MediaStream(state.call.localStream.getTracks());
        await safePlay(localCard.video);
    }
    setTileState(state.me?.id, { mic: state.call.mic, cam: state.call.cam, screen: state.call.screen });
    updateCallButtons();
    sendCallState();
}

async function toggleScreenShare() {
    if (!state.call.active) return;
    if (!("getDisplayMedia" in navigator.mediaDevices)) { alert("Демонстрация экрана недоступна."); return; }

    if (state.call.screen) {
        state.call.screen = false;
        state.call.cam    = false;
        state.call.localStream.getVideoTracks().forEach(t => { t.stop(); state.call.localStream.removeTrack(t); });
        for (const pc of state.call.peers.values()) {
            const sender = getPeerSender(pc, "video");
            if (sender) await sender.replaceTrack(null).catch(() => {});
        }
        await renegotiateAllPeers();
    } else {
        try {
            const display = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: false });
            const track   = display.getVideoTracks()[0];
            if (!track) return;
            track.onended = async () => {
                if (state.call.screen) {
                    await toggleScreenShare();
                }
            };

            state.call.localStream.getVideoTracks().forEach(t => { t.stop(); state.call.localStream.removeTrack(t); });
            state.call.localStream.addTrack(track);
            state.call.cam    = true;
            state.call.screen = true;

            for (const pc of state.call.peers.values()) {
                const sender = getPeerSender(pc, "video");
                if (sender) await sender.replaceTrack(track).catch(() => {});
            }
            await renegotiateAllPeers();
        } catch (e) { alert("Не удалось начать демонстрацию: " + e.message); return; }
    }

    const localCard = state.call.tiles.get(state.me?.id);
    if (localCard) {
        localCard.video.srcObject = new MediaStream(state.call.localStream.getTracks());
        await safePlay(localCard.video);
    }
    setTileState(state.me?.id, { mic: state.call.mic, cam: state.call.cam, screen: state.call.screen });
    updateCallButtons();
    sendCallState();
}

// ════════════════════════════════════════════════════════════
//  WEBSOCKET
// ════════════════════════════════════════════════════════════

function stopWsHeartbeat() {
    if (state.wsMeta.pingTimer) { clearInterval(state.wsMeta.pingTimer); state.wsMeta.pingTimer = null; }
    if (state.wsMeta.pongTimer) { clearTimeout(state.wsMeta.pongTimer);  state.wsMeta.pongTimer = null; }
}

function startWsHeartbeat() {
    stopWsHeartbeat();
    state.wsMeta.pingTimer = setInterval(() => {
        if (state.ws?.readyState !== WebSocket.OPEN) return;
        state.ws.send(JSON.stringify({ type: "ping" }));
        if (state.wsMeta.pongTimer) clearTimeout(state.wsMeta.pongTimer);
        state.wsMeta.pongTimer = setTimeout(() => { try { state.ws.close(); } catch (_) {} }, 9000);
    }, 15000);
}

function scheduleWsReconnect() {
    if (state.wsMeta.reconnectTimer) return;
    const delay = Math.min(6000, 900 + state.wsMeta.retry * 450);
    state.wsMeta.retry++;
    state.wsMeta.reconnectTimer = setTimeout(() => {
        state.wsMeta.reconnectTimer = null;
        connectWs();
    }, delay);
}

function connectWs() {
    if (!state.token) return;
    if (state.ws && [WebSocket.OPEN, WebSocket.CONNECTING].includes(state.ws.readyState)) return;

    const proto = location.protocol === "https:" ? "wss" : "ws";
    state.ws = new WebSocket(`${proto}://${location.host}/ws?token=${encodeURIComponent(state.token)}`);

    state.ws.onopen = async () => {
        state.wsMeta.retry = 0;
        stopWsHeartbeat();
        startWsHeartbeat();
        try {
            await Promise.all([loadChats(), loadFriends(), loadFriendRequests(), loadGroupInvites()]);
            await syncCurrentChatIfOpen();
        } catch (_) {}
        // Переподключение во время звонка
        if (state.call.active && state.call.chatId) {
            resetCallPeersForRejoin();
            state.ws.send(JSON.stringify({
                type: "call:join", chat_id: state.call.chatId,
                mic: state.call.mic, cam: state.call.cam, screen: state.call.screen,
            }));
        }
    };

    state.ws.onmessage = async (ev) => {
        let msg;
        try { msg = JSON.parse(ev.data); } catch (_) { return; }

        // ─ Heartbeat ─
        if (msg.type === "pong") {
            if (state.wsMeta.pongTimer) { clearTimeout(state.wsMeta.pongTimer); state.wsMeta.pongTimer = null; }
            return;
        }

        // ─ Сообщения ─
        if (msg.type === "message:new") {
            if (msg.payload.chat_id === state.currentChatId) {
                appendMessage(msg.payload);
                if (msg.payload.user_id !== state.me?.id) markChatRead(state.currentChatId);
            }
            loadChats();
        }
        if (msg.type === "message:read") {
            const { chat_id, reader_id, up_to_id } = msg.payload;
            if (reader_id !== state.me?.id && chat_id === state.currentChatId)
                updateReadStatusUpTo(up_to_id);
        }
        if (msg.type === "message:deleted_all" || msg.type === "message:deleted_me") {
            if (msg.payload.chat_id === state.currentChatId) removeMessageById(msg.payload.message_id);
            loadChats();
        }

        // ─ Друзья ─
        if (["friend:request","friend:accepted","chat:added"].includes(msg.type)) refreshSide();

        // ─ Приглашения в группы ─
        if (["group:invite","group:invite_answer"].includes(msg.type)) loadGroupInvites();

        // ─ События группы ─
        if (msg.type === "group:deleted") {
            if (state.currentChatId === msg.payload.chat_id) {
                state.currentChatId = null; state.currentChat = null;
                const m = qs("messages"); if (m) m.innerHTML = "";
                const w = qs("chatMembers"); if (w) w.innerHTML = "";
                setChatHeader(null); setChatOpen(false);
            }
            refreshSide();
        }

        if (msg.type === "group:member_removed") {
            if (msg.payload.user_id === state.me?.id && state.currentChatId === msg.payload.chat_id) {
                state.currentChatId = null; state.currentChat = null;
                const m = qs("messages"); if (m) m.innerHTML = "";
                const w = qs("chatMembers"); if (w) w.innerHTML = "";
                setChatHeader(null); setChatOpen(false);
            } else if (state.currentChatId === msg.payload.chat_id) {
                loadMembers(msg.payload.chat_id);
            }
            refreshSide();
        }

        if (msg.type === "group:member_role") {
            if (state.currentChatId === msg.payload.chat_id) loadMembers(msg.payload.chat_id);
            loadChats();
        }

        if (msg.type === "group:member_added") {
            if (state.currentChatId === msg.payload.chat_id) loadMembers(msg.payload.chat_id);
            loadChats();
        }

        if (msg.type === "user:blocked") refreshSide();

        // ─ Звонки ─
        if (msg.type === "call:participants") {
            try {
                const list   = msg.payload.users  || [];
                const states = msg.payload.states || {};
                Object.keys(states).forEach(uid => setTileState(Number(uid), states[uid]));
                for (const uid of list) {
                    if (Number(state.me?.id) > Number(uid)) await ensurePeer(Number(uid), true);
                }
            } catch (e) { console.error("call:participants:", e); }
        }

        if (msg.type === "call:ring") {
            const chat = state.chats.find(c => c.id === msg.payload.chat_id);
            state.ui.incomingCall = { chatId: msg.payload.chat_id, title: chat?.title || "Входящий звонок" };
            const el = qs("incomingCallText");
            if (el) el.textContent = `Входящий звонок: ${state.ui.incomingCall.title}`;
            show(qs("incomingCallToast"));
        }

        if (msg.type === "call:user_joined") {
            try {
                const uid = Number(msg.payload.user_id);
                setTileState(uid, msg.payload.state || {});
                if (state.call.active && state.call.chatId === msg.payload.chat_id && Number(state.me?.id) > uid)
                    await ensurePeer(uid, true);
            } catch (e) { console.error("call:user_joined:", e); }
        }

        if (msg.type === "call:user_left")   removePeer(msg.payload.user_id);
        if (msg.type === "call:user_state")  setTileState(msg.payload.user_id, msg.payload.state || {});
        if (msg.type === "call:signal")      await handleSignal(msg.payload.from_user, msg.payload.signal);
    };

    state.ws.onclose = () => { stopWsHeartbeat(); scheduleWsReconnect(); };
    state.ws.onerror = () => { try { state.ws.close(); } catch (_) {} };
}

// ─── Фолбэк-синхронизация ────────────────────────────────────
function startFallbackSync() {
    if (state.syncTimer) clearInterval(state.syncTimer);
    state.syncTimer = setInterval(async () => {
        try {
            await loadChats();
            await syncCurrentChatIfOpen();
            if (state.ws?.readyState !== WebSocket.OPEN) {
                await Promise.all([loadFriends(), loadFriendRequests(), loadGroupInvites()]);
            }
        } catch (_) {}
    }, 12000);
}

// ════════════════════════════════════════════════════════════
//  СЕССИЯ И АВТОРИЗАЦИЯ
// ════════════════════════════════════════════════════════════

async function ensureSession() {
    if (!state.token) return false;
    try { state.me = await api("/api/me"); return true; }
    catch (_) { localStorage.removeItem("token"); state.token = ""; return false; }
}

async function onAuthorized() {
    hide(qs("gateScreen")); hide(qs("authScreen")); show(qs("app"));
    renderProfileMini();
    try {
        await Promise.all([loadChats(), loadFriends(), loadFriendRequests(), loadGroupInvites(), loadSettings()]);
    } catch (e) { console.error("Ошибка инициализации:", e); }
    connectWs();
    startFallbackSync();
    const container = qs("messages");
    if (container) bindMessageContextMenu(container);
}

function resetToAuthUi() {
    hide(qs("app")); show(qs("authScreen")); hide(qs("gateScreen"));
    setError("authError", "");
    hideContextMenu();
    state.me            = null;
    state.settings      = null;
    state.chats         = [];
    state.friends       = [];
    state.currentChat   = null;
    state.currentChatId = null;
    state.membersById   = new Map();
}

// ════════════════════════════════════════════════════════════
//  ПРИВЯЗКА UI
// ════════════════════════════════════════════════════════════

function bindUi() {
    setMainTab("chats");
    setChatOpen(false);
    setEmptyState(true);

    // ─ Вкладки навигации ────────────────────────────────────
    const tabs = { tabChats: "chats", tabRequests: "requests", tabSearch: "search", tabFriends: "friends" };
    for (const [id, tab] of Object.entries(tabs)) {
        const el = qs(id); if (el) el.onclick = () => setMainTab(tab);
    }
    if (qs("btnBackToList")) qs("btnBackToList").onclick = () => setChatOpen(false);
    if (qs("btnMobileMenu")) qs("btnMobileMenu").onclick = () => document.body.classList.toggle("menu-open");

    // ─ Авторизация ──────────────────────────────────────────
    if (qs("tabLogin")) qs("tabLogin").onclick = () => {
        qs("tabLogin").classList.add("active");
        qs("tabRegister")?.classList.remove("active");
        show(qs("loginPane")); hide(qs("registerPane"));
    };
    if (qs("tabRegister")) qs("tabRegister").onclick = () => {
        qs("tabRegister").classList.add("active");
        qs("tabLogin")?.classList.remove("active");
        hide(qs("loginPane")); show(qs("registerPane"));
    };
    if (qs("loginBtn")) qs("loginBtn").onclick = async () => {
        setError("authError", "");
        try {
            const r = await api("/api/login", { method: "POST", body: JSON.stringify({
                username: qs("loginUsername")?.value?.trim() || "",
                password: qs("loginPassword")?.value || "",
            })});
            state.token = r.token; state.me = r.user;
            localStorage.setItem("token", state.token);
            await onAuthorized();
        } catch (e) { setError("authError", e.message); }
    };
    if (qs("registerBtn")) qs("registerBtn").onclick = async () => {
        setError("authError", "");
        try {
            const r = await api("/api/register", { method: "POST", body: JSON.stringify({
                username: qs("regUsername")?.value?.trim() || "",
                password: qs("regPassword")?.value || "",
                nickname: qs("regNickname")?.value?.trim() || "",
            })});
            state.token = r.token; state.me = r.user;
            localStorage.setItem("token", state.token);
            await onAuthorized();
        } catch (e) { setError("authError", e.message); }
    };
    if (qs("btnLogout")) qs("btnLogout").onclick = async () => {
        try { await api("/api/logout", { method: "POST", body: "{}" }); } catch (_) {}
        leaveCall();
        if (state.syncTimer) clearInterval(state.syncTimer);
        stopWsHeartbeat();
        if (state.ws) try { state.ws.close(); } catch (_) {}
        localStorage.removeItem("token"); state.token = "";
        resetToAuthUi();
    };

    // ─ Чат и сообщения ──────────────────────────────────────
    if (qs("chatSearch")) qs("chatSearch").oninput = () => renderChatList(qs("chatSearch").value);
    if (qs("sendBtn")) qs("sendBtn").onclick = () => {
        const text = qs("messageInput")?.value?.trim() || "";
        if (text) sendMessage({ text });
    };
    if (qs("messageInput")) qs("messageInput").addEventListener("keydown", e => {
        if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            const text = qs("messageInput").value?.trim() || "";
            if (text) sendMessage({ text });
        }
    });
    if (qs("btnFile"))   qs("btnFile").onclick   = () => qs("fileInput")?.click();
    if (qs("fileInput")) qs("fileInput").onchange = async () => {
        const f = qs("fileInput").files?.[0]; if (!f) return;
        let kind = "file";
        if (f.type.startsWith("image/")) kind = "image";
        if (f.type.startsWith("video/")) kind = "video";
        await sendMessage({ file: f, kind }); qs("fileInput").value = "";
    };
    if (qs("btnVoice"))  qs("btnVoice").onclick  = startVoiceRecord;
    if (qs("btnCircle")) qs("btnCircle").onclick = startCircleRecord;

    // ─ Профиль ──────────────────────────────────────────────
    if (qs("btnProfile")) qs("btnProfile").onclick = () => {
        if (qs("profileNickname")) qs("profileNickname").value = state.me?.nickname || "";
        if (qs("profileAbout"))    qs("profileAbout").value    = state.me?.about    || "";
        qs("profileDialog")?.showModal();
    };
    if (qs("profileClose")) qs("profileClose").onclick = () => qs("profileDialog")?.close();
    if (qs("profileSave")) qs("profileSave").onclick = async () => {
        try {
            state.me = await api("/api/profile", { method: "POST", body: JSON.stringify({
                nickname: qs("profileNickname")?.value || "",
                about:    qs("profileAbout")?.value    || "",
            })});
            const avatar = qs("profileAvatar")?.files?.[0];
            if (avatar) {
                const form = new FormData(); form.append("file", avatar);
                state.me = await api("/api/profile/avatar", { method: "POST", body: form });
            }
            renderProfileMini(); qs("profileDialog")?.close();
        } catch (e) { alert("Ошибка: " + e.message); }
    };

    // ─ Стикеры ──────────────────────────────────────────────
    if (qs("btnAssets")) qs("btnAssets").onclick = async () => { await loadAssets(); qs("assetsDialog")?.showModal(); };
    if (qs("assetsClose")) qs("assetsClose").onclick = () => qs("assetsDialog")?.close();
    if (qs("btnUploadAsset")) qs("btnUploadAsset").onclick = async () => {
        const f = qs("assetFile")?.files?.[0]; if (!f) { alert("Выберите файл"); return; }
        const form = new FormData();
        form.append("kind",  qs("assetKind")?.value  || "emoji");
        form.append("title", qs("assetTitle")?.value || "");
        form.append("file",  f);
        try {
            await api("/api/assets", { method: "POST", body: form });
            if (qs("assetFile"))  qs("assetFile").value  = "";
            if (qs("assetTitle")) qs("assetTitle").value = "";
            await loadAssets();
        } catch (e) { alert("Ошибка: " + e.message); }
    };

    // ─ Группы ───────────────────────────────────────────────
    if (qs("btnGroup"))   qs("btnGroup").onclick   = () => qs("groupDialog")?.showModal();
    if (qs("groupClose")) qs("groupClose").onclick = () => qs("groupDialog")?.close();
    if (qs("groupCreate")) qs("groupCreate").onclick = async () => {
        try {
            const members = (qs("groupMembers")?.value || "")
                .split(",").map(v => v.trim().replace(/^@/, "").toLowerCase()).filter(Boolean);
            const out = await api("/api/groups", { method: "POST", body: JSON.stringify({
                title: qs("groupTitle")?.value || "", members,
            })});
            if (qs("groupTitle"))   qs("groupTitle").value   = "";
            if (qs("groupMembers")) qs("groupMembers").value = "";
            qs("groupDialog")?.close();
            await loadChats(); await openChat(out.chat_id);
        } catch (e) { alert(e.message); }
    };
    if (qs("btnInviteGroup")) qs("btnInviteGroup").onclick = async () => {
        const raw = prompt("Введите @username для приглашения"); if (!raw) return;
        try {
            await api(`/api/groups/${state.currentChatId}/invite/username`, {
                method: "POST", body: JSON.stringify({ username: raw.trim().replace(/^@/, "") }),
            });
            alert("Приглашение отправлено!");
        } catch (e) { alert(e.message); }
    };
    if (qs("btnLeaveChat")) qs("btnLeaveChat").onclick = async () => {
        const isGroup = state.currentChat?.type === "group";
        if (!confirm(`Выйти из ${isGroup ? "группы" : "чата"}?`)) return;
        if (state.call.active && state.call.chatId === state.currentChatId) leaveCall();
        try {
            await api(`/api/chats/${state.currentChatId}/leave`, { method: "POST", body: "{}" });
            state.currentChat = null; state.currentChatId = null;
            setChatHeader(null); setChatOpen(false); await loadChats();
        } catch (e) { alert(e.message); }
    };
    if (qs("btnDeleteGroup")) qs("btnDeleteGroup").onclick = async () => {
        if (!confirm("Удалить группу навсегда?")) return;
        try {
            await api(`/api/groups/${state.currentChatId}`, { method: "DELETE" });
            state.currentChat = null; state.currentChatId = null;
            setChatHeader(null); setChatOpen(false); await loadChats();
        } catch (e) { alert(e.message); }
    };

    // ─ Поиск и прочее ───────────────────────────────────────
    if (qs("btnFriends")) qs("btnFriends").onclick = refreshSide;
    if (qs("btnCopyMyId")) qs("btnCopyMyId").onclick = async () => {
        const id = String(state.me?.id || "");
        try { await navigator.clipboard.writeText(id); alert(`ID скопирован: ${id}`); }
        catch (_) { prompt("Ваш ID:", id); }
    };
    if (qs("userSearch")) qs("userSearch").oninput = async () => {
        const q   = qs("userSearch").value.trim();
        const out = qs("userResults"); if (!out) return;
        if (q.length < 2) { out.innerHTML = ""; return; }
        try {
            const users = await api(`/api/users/search?q=${encodeURIComponent(q)}`);
            out.innerHTML = "";
            users.forEach(u => {
                const el = document.createElement("div"); el.className = "item";
                el.innerHTML = `
                    <div class="item-head">
                        ${avatarMarkup({ avatar: u.avatar, label: u.nickname, seed: `search-${u.id}`, className: "avatar-md" })}
                        <div class="item-copy">
                            <div class="item-title-row">
                                <b>${escapeHtml(u.nickname)}</b>
                                <span class="item-tag">User</span>
                            </div>
                            <small>@${escapeHtml(u.username)} #${u.id}</small>
                            ${u.about ? `<p class="item-preview">${escapeHtml(truncateText(u.about, 80))}</p>` : ""}
                        </div>
                    </div>
                `;
                const actions = document.createElement("div"); actions.className = "actions";
                const add = document.createElement("button"); add.textContent = "В друзья";
                add.onclick = async () => {
                    try {
                        await api("/api/friends/request", { method: "POST", body: JSON.stringify({ username: u.username }) });
                        alert("Заявка отправлена!");
                    } catch (e) { alert(e.message); }
                };
                const block = document.createElement("button"); block.className = "danger"; block.textContent = "Блок";
                block.onclick = async () => { await api(`/api/users/${u.id}/block`, { method: "POST", body: "{}" }); await refreshSide(); };
                actions.appendChild(add); actions.appendChild(block); el.appendChild(actions); out.appendChild(el);
            });
        } catch (_) {}
    };

    // ─ Настройки ────────────────────────────────────────────
    if (qs("btnSettings")) qs("btnSettings").onclick = async () => {
        await loadSettings(); await loadBlockedList(); qs("settingsDialog")?.showModal();
    };
    if (qs("settingsClose")) qs("settingsClose").onclick = () => qs("settingsDialog")?.close();
    if (qs("settingsSave")) qs("settingsSave").onclick = async () => {
        try {
            state.settings = await api("/api/settings", { method: "POST", body: JSON.stringify({
                allow_friend_requests: qs("setFriendReq")?.value || "everyone",
                allow_calls_from:      qs("setCalls")?.value     || "friends",
                allow_group_invites:   qs("setInvites")?.value   || "friends",
                show_last_seen:        qs("setLastSeen")?.value   || "friends",
            })});
            qs("settingsDialog")?.close();
        } catch (e) { alert("Ошибка: " + e.message); }
    };
    if (qs("btnChangePassword")) qs("btnChangePassword").onclick = async () => {
        const oldPwd = qs("oldPassword")?.value || "";
        const newPwd = qs("newPassword")?.value  || "";
        if (!oldPwd || !newPwd) { alert("Введите оба пароля"); return; }
        try {
            await api("/api/account/password", { method: "POST", body: JSON.stringify({ old_password: oldPwd, new_password: newPwd }) });
            if (qs("oldPassword")) qs("oldPassword").value = "";
            if (qs("newPassword")) qs("newPassword").value  = "";
            alert("Пароль успешно изменён");
        } catch (e) { alert(e.message); }
    };
    if (qs("btnDeleteAccount")) qs("btnDeleteAccount").onclick = async () => {
        if (!confirm("Удалить аккаунт? Это необратимо.")) return;
        try {
            await api("/api/account", { method: "DELETE" });
            leaveCall(); if (state.syncTimer) clearInterval(state.syncTimer);
            stopWsHeartbeat(); if (state.ws) try { state.ws.close(); } catch (_) {}
            localStorage.removeItem("token"); state.token = ""; resetToAuthUi();
        } catch (e) { alert(e.message); }
    };

    // ─ Звонки ───────────────────────────────────────────────
    if (qs("btnCallStart"))   qs("btnCallStart").onclick   = startCall;
    if (qs("btnLeaveCall"))   qs("btnLeaveCall").onclick   = leaveCall;
    if (qs("btnToggleMic"))   qs("btnToggleMic").onclick   = toggleMic;
    if (qs("btnToggleCam"))   qs("btnToggleCam").onclick   = toggleCam;
    if (qs("btnShareScreen")) qs("btnShareScreen").onclick = toggleScreenShare;
    if (qs("btnRotateCam"))   qs("btnRotateCam").onclick   = rotateCamera;

    if (qs("btnDevices")) qs("btnDevices").onclick = async () => {
        const dp = qs("devicePanel"); if (!dp) return;
        if (dp.classList.contains("hidden")) { await refreshDevicePanel(); show(dp); } else hide(dp);
    };
    if (qs("selMic"))     qs("selMic").onchange     = () => switchMicDevice(qs("selMic").value);
    if (qs("selCam"))     qs("selCam").onchange     = () => switchCamDevice(qs("selCam").value);
    if (qs("selSpeaker")) qs("selSpeaker").onchange = () => { state.devicePrefs.speakerId = qs("selSpeaker").value || ""; applySpeakerToAllTiles(); };
    if (qs("selAudioMode")) qs("selAudioMode").onchange = () => applyAudioMode(qs("selAudioMode").value);

    if (qs("btnMinimizeCall")) qs("btnMinimizeCall").onclick = () => {
        hide(qs("callOverlay")); state.ui.callMinimized = true; show(qs("btnCallRestore"));
    };
    if (qs("btnCallRestore")) qs("btnCallRestore").onclick = () => {
        if (!state.call.active) return;
        show(qs("callOverlay")); hide(qs("btnCallRestore")); state.ui.callMinimized = false;
    };

    // ─ Входящий звонок ──────────────────────────────────────
    if (qs("btnIncomingAccept")) qs("btnIncomingAccept").onclick = async () => {
        const incoming = state.ui.incomingCall; hide(qs("incomingCallToast"));
        if (!incoming) return;
        const chat = state.chats.find(c => c.id === incoming.chatId);
        if (chat) { await openChat(chat.id); await startCall(); }
        state.ui.incomingCall = null;
    };
    if (qs("btnIncomingDecline")) qs("btnIncomingDecline").onclick = () => {
        hide(qs("incomingCallToast")); state.ui.incomingCall = null;
    };

    // ─ Панель участников (bottom-sheet на мобильных) ─────────
    if (qs("btnShowMembers"))  qs("btnShowMembers").onclick  = openMembersSheet;
    if (qs("btnCloseMembers")) qs("btnCloseMembers").onclick = closeMembersSheet;
    if (qs("membersBackdrop")) qs("membersBackdrop").onclick = closeMembersSheet;
}

// ════════════════════════════════════════════════════════════
//  ТОЧКА ВХОДА
// ════════════════════════════════════════════════════════════

async function boot() {
    if (window.__lanMessengerBooted) return;
    window.__lanMessengerBooted = true;

    installResponsiveEnvironment();
    bindUi();

    if (await ensureSession()) {
        await onAuthorized();
    } else {
        hide(qs("app"));
        show(qs("authScreen"));
    }
}

boot();
