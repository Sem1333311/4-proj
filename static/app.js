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
        pendingCandidates: new Map(),
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
    },
};

const isLikelyIOS =
    /iPad|iPhone|iPod/.test(navigator.userAgent) ||
    (navigator.platform === "MacIntel" && navigator.maxTouchPoints > 1);
const isLikelyAndroid = /Android/.test(navigator.userAgent);
const isMobile = isLikelyIOS || isLikelyAndroid;
const isLikelySafari = /^((?!chrome|android|crios|fxios).)*safari/i.test(
    navigator.userAgent,
);
const isLocalDevHost = ["localhost", "127.0.0.1", "[::1]"].includes(
    location.hostname,
);

// ─── Разблокировка AudioContext (autoplay) ────────────────────────────────────
let _audioCtxUnlocked = false;

async function unlockAudioContext() {
    if (_audioCtxUnlocked) return;
    try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const buf = ctx.createBuffer(1, 1, 22050);
        const src = ctx.createBufferSource();
        src.buffer = buf;
        src.connect(ctx.destination);
        src.start(0);
        await ctx.resume();
        _audioCtxUnlocked = true;
        console.log("AudioContext unlocked");
    } catch (e) {
        console.warn("AudioContext unlock failed:", e);
    }
}

// ─── Утилиты ──────────────────────────────────────────────────────────────────

function qs(id) {
    return document.getElementById(id);
}
function show(el) {
    if (el) el.classList.remove("hidden");
}
function hide(el) {
    if (el) el.classList.add("hidden");
}

function setMainTab(tab) {
    state.ui.currentTab = tab;
    const pairs = [
        ["chats", "paneChats", "tabChats"],
        ["requests", "paneRequests", "tabRequests"],
        ["search", "paneSearch", "tabSearch"],
        ["friends", "paneFriends", "tabFriends"],
    ];
    for (const [key, paneId, btnId] of pairs) {
        const pane = qs(paneId);
        const btn = qs(btnId);
        if (!pane || !btn) continue;
        if (key === tab) {
            show(pane);
            btn.classList.add("active");
        } else {
            hide(pane);
            btn.classList.remove("active");
        }
    }
    document.body.classList.remove("menu-open");
}

function setChatOpen(open) {
    state.ui.chatOpen = !!open;
    document.body.classList.toggle("chat-open", state.ui.chatOpen);
    if (window.innerWidth <= 980) {
        document.body.classList.toggle("menu-open", !state.ui.chatOpen);
    }
    const back = qs("btnBackToList");
    if (!back) return;
    if (state.ui.chatOpen) show(back);
    else hide(back);
}

function escapeHtml(v) {
    return (v || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
}

function setError(id, text) {
    const el = qs(id);
    if (el) el.textContent = text || "";
}

function syncViewportHeight() {
    const h = Math.round(
        window.visualViewport?.height ||
            window.innerHeight ||
            document.documentElement.clientHeight ||
            0,
    );
    if (h > 0)
        document.documentElement.style.setProperty("--app-height", `${h}px`);
}

function installResponsiveEnvironment() {
    document.body.classList.toggle("platform-ios", isLikelyIOS);
    document.body.classList.toggle("platform-android", isLikelyAndroid);
    document.body.classList.toggle("platform-safari", isLikelySafari);
    document.body.classList.toggle("platform-mobile", isMobile);
    syncViewportHeight();
    if (!window.__lanMessengerViewportBound) {
        window.addEventListener("resize", syncViewportHeight);
        window.visualViewport?.addEventListener("resize", syncViewportHeight);
        window.__lanMessengerViewportBound = true;
    }
}

function formatTime(iso) {
    const date = new Date(iso);
    if (Number.isNaN(date.getTime())) return "";
    return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function formatListTime(iso) {
    if (!iso) return "";
    const date = new Date(iso);
    if (Number.isNaN(date.getTime())) return "";
    const now = new Date();
    if (date.toDateString() === now.toDateString()) {
        return date.toLocaleTimeString([], {
            hour: "2-digit",
            minute: "2-digit",
        });
    }
    return date.toLocaleDateString([], { day: "2-digit", month: "2-digit" });
}

function fmtDuration(sec) {
    const m = Math.floor(sec / 60);
    const s = sec % 60;
    return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

function pickRecorderMime(kind) {
    const options =
        kind === "audio"
            ? ["audio/mp4", "audio/webm;codecs=opus", "audio/webm"]
            : ["video/mp4", "video/webm;codecs=vp8,opus", "video/webm"];
    for (const m of options) {
        if (
            window.MediaRecorder &&
            MediaRecorder.isTypeSupported &&
            MediaRecorder.isTypeSupported(m)
        )
            return m;
    }
    return "";
}

function extForMime(mime, fallback) {
    if (!mime) return fallback;
    if (mime.includes("mp4")) return "mp4";
    if (mime.includes("webm")) return "webm";
    return fallback;
}

async function api(path, opts = {}) {
    const headers = opts.headers || {};
    if (!(opts.body instanceof FormData))
        headers["Content-Type"] = "application/json";
    if (state.token) headers.Authorization = `Bearer ${state.token}`;
    const res = await fetch(path, { ...opts, headers });
    if (!res.ok) {
        let msg = `HTTP ${res.status}`;
        try {
            const data = await res.json();
            msg = data.detail || msg;
        } catch (_) {}
        throw new Error(msg);
    }
    return res.json();
}

function peerNameById(id) {
    const m = state.membersById.get(id);
    if (!m) return `#${id}`;
    return `${m.nickname} @${m.username}`;
}

function withMediaToken(url) {
    if (!url || !state.token) return url;
    return `${url}${url.includes("?") ? "&" : "?"}token=${encodeURIComponent(state.token)}`;
}

function avatarMediaUrl(avatar) {
    if (!avatar) return "";
    const f = String(avatar);
    return withMediaToken(
        f.startsWith("/media/") ? f : `/media/${encodeURIComponent(f)}`,
    );
}

function truncateText(text, limit = 96) {
    const clean = String(text || "").trim();
    if (!clean) return "";
    return clean.length > limit
        ? `${clean.slice(0, limit).trimEnd()}...`
        : clean;
}

function toMultilineHtml(text) {
    return escapeHtml(text || "").replace(/\n/g, "<br>");
}

function seedHue(seed) {
    const source = String(seed || "lm");
    let hash = 0;
    for (let i = 0; i < source.length; i++)
        hash = (hash * 31 + source.charCodeAt(i)) % 360;
    return Math.abs(hash);
}

function initialsFromLabel(label) {
    const clean = String(label || "").trim();
    if (!clean) return "LM";
    const parts = clean.split(/\s+/).filter(Boolean);
    const letters = parts
        .slice(0, 2)
        .map((p) => p[0])
        .join("");
    return (letters || clean.slice(0, 2)).toUpperCase();
}

function avatarMarkup({ avatar, label, seed, className = "avatar-md" }) {
    const classes = ["avatar", className].filter(Boolean).join(" ");
    const title = escapeHtml(label || "LM");
    const url = avatarMediaUrl(avatar);
    if (url)
        return `<img class="${classes}" src="${url}" alt="${title}" loading="lazy">`;
    return `<div class="${classes}" style="--avatar-hue:${seedHue(seed || label || "lm")}">${escapeHtml(initialsFromLabel(label))}</div>`;
}

function roleBadge(role) {
    if (role === "owner") return "👑 Владелец";
    if (role === "admin") return "🛡️ Админ";
    return "🙂 Участник";
}

function myGroupRole() {
    return state.membersById.get(state.me?.id)?.role || "member";
}

function chatTitleText(chat) {
    return chat ? chat.title || chat.peer?.nickname || "Чат" : "Выберите чат";
}

function chatMetaText(chat) {
    if (!chat) return "Откройте диалог слева или создайте новую группу.";
    if (chat.type === "group") {
        const count = state.membersById.size || chat.member_count || 0;
        return count ? `${count} участников` : "Групповой чат";
    }
    if (chat.peer?.username) return `@${chat.peer.username}`;
    return "Личный чат";
}

function setComposerEnabled(enabled) {
    const input = qs("messageInput");
    if (input) {
        input.disabled = !enabled;
        input.placeholder = enabled
            ? "Напишите сообщение..."
            : "Выберите чат, чтобы начать переписку";
    }
    ["sendBtn", "btnAssets", "btnFile", "btnVoice", "btnCircle"].forEach(
        (id) => {
            const btn = qs(id);
            if (btn) btn.disabled = !enabled;
        },
    );
}

function setEmptyState(showEmpty) {
    const empty = qs("chatEmptyState");
    const messages = qs("messages");
    if (showEmpty) {
        show(empty);
        hide(messages);
    } else {
        hide(empty);
        show(messages);
    }
    setComposerEnabled(!showEmpty);
}

function messageBody(m) {
    const text = toMultilineHtml(m.text || "");
    const parts = [];
    if (text) parts.push(`<div class="message-text">${text}</div>`);
    if (!m.file_url) return parts.join("");
    const fileUrl = withMediaToken(m.file_url);
    if (m.kind === "image" || m.kind === "sticker" || m.kind === "emoji") {
        parts.push(
            `<img src="${fileUrl}" alt="${escapeHtml(m.file_name || "Вложение")}" loading="lazy">`,
        );
        return parts.join("");
    }
    if (m.kind === "video" || m.kind === "circle") {
        parts.push(
            `<video src="${fileUrl}" controls playsinline webkit-playsinline preload="metadata"></video>`,
        );
        return parts.join("");
    }
    if (m.kind === "voice") {
        parts.push(
            `<audio src="${fileUrl}" controls preload="metadata"></audio>`,
        );
        return parts.join("");
    }
    return `${text ? `${text}<br>` : ""}<a href="${fileUrl}" target="_blank">${escapeHtml(m.file_name || "Файл")}</a>`;
}

function appendMessage(m) {
    const mine = m.user_id === state.me?.id;
    const isRead = mine && m.read_count > 0;
    const item = document.createElement("div");
    item.dataset.mid = String(m.id);
    item.dataset.mine = mine ? "1" : "0";
    item.className = `message ${mine ? "mine" : ""}`;
    const statusHtml = mine
        ? `<span class="msg-status ${isRead ? "read" : ""}" data-mid="${m.id}">${isRead ? "✓✓" : "✓"}</span>`
        : "";
    item.innerHTML = `
        ${mine ? "" : avatarMarkup({ avatar: m.avatar, label: m.nickname, seed: `user-${m.user_id}`, className: "avatar-sm" })}
        <div class="message-bubble">
            <div class="message-meta">
                <span class="message-author">${escapeHtml(m.nickname)}</span>
                <span>@${escapeHtml(m.username)}</span>
                <span>${formatTime(m.created_at)}</span>
                ${statusHtml}
            </div>
            <div class="message-content">${messageBody(m)}</div>
        </div>
    `;
    const messages = qs("messages");
    if (messages) {
        messages.appendChild(item);
        messages.scrollTop = messages.scrollHeight;
    }
}

function removeMessageById(messageId) {
    qs("messages")?.querySelector(`[data-mid="${messageId}"]`)?.remove();
}

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
    const q = filter.trim().toLowerCase();
    const list = qs("chatList");
    if (!list) return;
    list.innerHTML = "";
    state.chats
        .filter((c) => {
            const h = [c.title, c.peer?.nickname, c.peer?.username, c.last_text]
                .filter(Boolean)
                .join(" ")
                .toLowerCase();
            return !q || h.includes(q);
        })
        .forEach((chat) => {
            const title = chatTitleText(chat);
            const preview = truncateText(
                chat.last_text ||
                    (chat.type === "group"
                        ? "Групповой чат"
                        : "Начните новый диалог"),
                88,
            );
            const subtitle =
                chat.type === "group"
                    ? "Группа"
                    : chat.peer?.username
                      ? `@${chat.peer.username}`
                      : "Личный чат";
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

function setChatHeader(chat) {
    const titleEl = qs("chatTitle");
    const titleText = chatTitleText(chat);
    const metaEl = qs("chatMeta");
    const avatarEl = qs("chatAvatar");
    const canCall = !!chat && !!chat.can_call;
    const group = !!chat && chat.type === "group";
    const canDelete = !!chat && chat.type === "group" && !!chat.can_delete;
    const stack = document.querySelector(".chat-stack");
    const btnCall = qs("btnCallStart");
    const btnInvite = qs("btnInviteGroup");
    const btnLeave = qs("btnLeaveChat");
    const btnDelete = qs("btnDeleteGroup");
    const membersPanel = qs("groupMembersPanel");
    const btnShowMembers = qs("btnShowMembers");

    if (titleEl) titleEl.textContent = titleText;
    if (metaEl) metaEl.textContent = chatMetaText(chat);
    if (avatarEl)
        avatarEl.innerHTML = avatarMarkup({
            avatar: chat?.peer?.avatar,
            label: titleText,
            seed: chat ? `chat-${chat.id}-${titleText}` : "empty-chat",
            className: "avatar-xl avatar-placeholder",
        });
    if (!chat) state.membersById = new Map();

    if (canCall) show(btnCall);
    else hide(btnCall);
    if (group) show(btnInvite);
    else hide(btnInvite);
    if (group) show(btnShowMembers);
    else hide(btnShowMembers);
    if (btnInvite)
        btnInvite.title = group ? "Пригласить по username" : "Пригласить";
    if (chat) show(btnLeave);
    else hide(btnLeave);
    if (btnLeave) {
        btnLeave.textContent = "🚪";
        btnLeave.title = group ? "Выйти из группы" : "Выйти из чата";
    }
    if (canDelete) show(btnDelete);
    else hide(btnDelete);
    if (btnDelete) {
        btnDelete.textContent = "🗑️";
        btnDelete.title = "Удалить группу";
    }
    if (group) show(membersPanel);
    else hide(membersPanel);
    if (!group) closeMembersSheet();
    if (stack) stack.classList.toggle("has-members", group);
    setEmptyState(!chat);
}

async function loadMembers(chatId) {
    if (!chatId) return;
    const members = await api(`/api/chats/${chatId}/members`);
    state.membersById = new Map(members.map((m) => [m.id, m]));
    const wrap = qs("chatMembers");
    if (!wrap) return;
    wrap.innerHTML = "";
    const currentRole = myGroupRole();
    members.forEach((m) => {
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
        if (m.id !== state.me.id) {
            const dm = document.createElement("button");
            dm.textContent = "💬";
            dm.title = "Личный чат";
            dm.onclick = async () => {
                const out = await api("/api/chats/direct", {
                    method: "POST",
                    body: JSON.stringify({ user_id: m.id }),
                });
                await loadChats();
                await openChat(out.chat_id);
            };
            const call = document.createElement("button");
            call.textContent = "📞";
            call.title = "Позвонить";
            call.onclick = async () => {
                const out = await api("/api/chats/direct", {
                    method: "POST",
                    body: JSON.stringify({ user_id: m.id }),
                });
                await loadChats();
                await openChat(out.chat_id);
                await startCall();
            };
            actions.appendChild(dm);
            actions.appendChild(call);
        }
        if (currentRole === "owner" && m.id !== state.me.id) {
            const promote = document.createElement("button");
            promote.className = "ghost";
            promote.textContent = m.role === "admin" ? "🙂" : "🛡️";
            promote.title =
                m.role === "admin" ? "Сделать участником" : "Выдать админку";
            promote.onclick = async () => {
                try {
                    await api(`/api/groups/${chatId}/members/${m.id}/role`, {
                        method: "POST",
                        body: JSON.stringify({
                            role: m.role === "admin" ? "member" : "admin",
                        }),
                    });
                    await Promise.all([loadMembers(chatId), loadChats()]);
                } catch (e) {
                    alert(e.message);
                }
            };
            const transfer = document.createElement("button");
            transfer.className = "ghost";
            transfer.textContent = "👑";
            transfer.title = "Передать owner";
            transfer.onclick = async () => {
                if (
                    !confirm(`Передать роль owner пользователю @${m.username}?`)
                )
                    return;
                try {
                    await api(`/api/groups/${chatId}/members/${m.id}/role`, {
                        method: "POST",
                        body: JSON.stringify({ role: "owner" }),
                    });
                    await Promise.all([loadMembers(chatId), loadChats()]);
                } catch (e) {
                    alert(e.message);
                }
            };
            actions.appendChild(promote);
            actions.appendChild(transfer);
        }
        if (
            (currentRole === "owner" && m.id !== state.me.id) ||
            (currentRole === "admin" &&
                m.role === "member" &&
                m.id !== state.me.id)
        ) {
            const kick = document.createElement("button");
            kick.className = "danger";
            kick.textContent = "⛔";
            kick.title = "Исключить из группы";
            kick.onclick = async () => {
                if (!confirm(`Исключить @${m.username} из группы?`)) return;
                try {
                    await api(`/api/groups/${chatId}/members/${m.id}`, {
                        method: "DELETE",
                    });
                    await Promise.all([loadMembers(chatId), loadChats()]);
                } catch (e) {
                    alert(e.message);
                }
            };
            actions.appendChild(kick);
        }
        el.appendChild(actions);
        wrap.appendChild(el);
    });
    if (state.currentChatId === chatId) setChatHeader(state.currentChat);
}

async function openChat(chatId) {
    const chat = state.chats.find((c) => c.id === chatId) || null;
    state.currentChat = chat;
    state.currentChatId = chatId;
    state.membersById = new Map();
    setChatHeader(chat);
    setChatOpen(true);
    if (window.innerWidth <= 980) document.body.classList.remove("menu-open");
    const messages = qs("messages");
    if (messages) messages.innerHTML = "";
    const data = await api(`/api/chats/${chatId}/messages`);
    data.forEach(appendMessage);
    await markChatRead(chatId);
    if (chat && chat.type === "group") await loadMembers(chatId);
    else state.membersById = new Map();
}

async function loadChats() {
    state.chats = await api("/api/chats");
    renderChatList(qs("chatSearch")?.value || "");
    if (state.currentChatId) {
        const still = state.chats.find((c) => c.id === state.currentChatId);
        if (!still) {
            state.currentChat = null;
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
}

async function loadFriends() {
    state.friends = await api("/api/friends");
    const list = qs("friendsList");
    if (!list) return;
    list.innerHTML = "";
    state.friends.forEach((f) => {
        const el = document.createElement("div");
        el.className = "item";
        el.innerHTML = `
            <div class="item-head">
                ${avatarMarkup({ avatar: f.avatar, label: f.nickname, seed: `friend-${f.id}`, className: "avatar-md" })}
                <div class="item-copy">
                    <div class="item-title-row"><b>${escapeHtml(f.nickname)}</b><span class="item-tag">Friend</span></div>
                    <small>@${escapeHtml(f.username)} #${f.id}</small>
                    ${f.about ? `<p class="item-preview">${escapeHtml(truncateText(f.about, 90))}</p>` : ""}
                </div>
            </div>
        `;
        const actions = document.createElement("div");
        actions.className = "actions";
        const dm = document.createElement("button");
        dm.textContent = "Чат";
        dm.onclick = async () => {
            const out = await api("/api/chats/direct", {
                method: "POST",
                body: JSON.stringify({ user_id: f.id }),
            });
            await loadChats();
            await openChat(out.chat_id);
        };
        const block = document.createElement("button");
        block.className = "danger";
        block.textContent = "Блок";
        block.onclick = async () => {
            await api(`/api/users/${f.id}/block`, {
                method: "POST",
                body: "{}",
            });
            await refreshSide();
        };
        actions.appendChild(dm);
        actions.appendChild(block);
        el.appendChild(actions);
        list.appendChild(el);
    });
}

async function loadFriendRequests() {
    const rows = await api("/api/friends/requests");
    const list = qs("friendRequests");
    if (!list) return;
    list.innerHTML = "";
    rows.forEach((r) => {
        const el = document.createElement("div");
        el.className = "item";
        el.innerHTML = `
            <div class="item-head">
                ${avatarMarkup({ avatar: r.avatar, label: r.nickname, seed: `request-${r.user_id || r.id}`, className: "avatar-md" })}
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
            await api(`/api/friends/request/${r.id}/accept`, {
                method: "POST",
                body: "{}",
            });
            await refreshSide();
        };
        const no = document.createElement("button");
        no.className = "danger";
        no.textContent = "Отклонить";
        no.onclick = async () => {
            await api(`/api/friends/request/${r.id}/reject`, {
                method: "POST",
                body: "{}",
            });
            await refreshSide();
        };
        actions.appendChild(yes);
        actions.appendChild(no);
        el.appendChild(actions);
        list.appendChild(el);
    });
}

async function loadGroupInvites() {
    const rows = await api("/api/groups/invites");
    const list = qs("groupInvites");
    if (!list) return;
    list.innerHTML = "";
    rows.forEach((r) => {
        const el = document.createElement("div");
        el.className = "item";
        el.innerHTML = `
            <div class="item-head">
                ${avatarMarkup({ label: r.chat_title || "Группа", seed: `invite-${r.chat_id || r.id}`, className: "avatar-md" })}
                <div class="item-copy">
                    <b>${escapeHtml(r.chat_title || "Группа")}</b>
                    <small>от ${escapeHtml(r.inviter_nickname)} @${escapeHtml(r.inviter_username)}</small>
                    <p class="item-preview">Вас приглашают в групповой чат.</p>
                </div>
            </div>
        `;
        const actions = document.createElement("div");
        actions.className = "actions";
        const yes = document.createElement("button");
        yes.textContent = "Принять";
        yes.onclick = async () => {
            await api(`/api/groups/invites/${r.id}/accept`, {
                method: "POST",
                body: "{}",
            });
            await Promise.all([loadGroupInvites(), loadChats()]);
        };
        const no = document.createElement("button");
        no.className = "danger";
        no.textContent = "Отклонить";
        no.onclick = async () => {
            await api(`/api/groups/invites/${r.id}/reject`, {
                method: "POST",
                body: "{}",
            });
            await loadGroupInvites();
        };
        actions.appendChild(yes);
        actions.appendChild(no);
        el.appendChild(actions);
        list.appendChild(el);
    });
}

async function loadBlockedList() {
    const rows = await api("/api/blocks");
    const list = qs("blockedList");
    if (!list) return;
    list.innerHTML = "";
    rows.forEach((u) => {
        const el = document.createElement("div");
        el.className = "item";
        el.innerHTML = `
            <div class="item-head">
                ${avatarMarkup({ avatar: u.avatar, label: u.nickname, seed: `blocked-${u.id}`, className: "avatar-sm" })}
                <div class="item-copy"><b>${escapeHtml(u.nickname)}</b><small>@${escapeHtml(u.username)} #${u.id}</small></div>
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
}

async function markChatRead(chatId) {
    try {
        await api(`/api/chats/${chatId}/read`, { method: "POST", body: "{}" });
    } catch (_) {}
}

function updateReadStatusUpTo(upToId) {
    const messages = qs("messages");
    if (!messages) return;
    messages.querySelectorAll(".msg-status[data-mid]").forEach((el) => {
        if (Number(el.dataset.mid) <= upToId) {
            el.textContent = "✓✓";
            el.classList.add("read");
        }
    });
}

async function refreshSide() {
    await Promise.all([
        loadFriends(),
        loadFriendRequests(),
        loadGroupInvites(),
        loadChats(),
    ]);
}

async function sendMessage({ text = "", file = null, kind = "text" }) {
    if (!state.currentChatId) return;
    const form = new FormData();
    form.append("text", text);
    form.append("kind", kind);
    if (file) form.append("file", file, file.name || "upload.bin");
    await api(`/api/chats/${state.currentChatId}/messages`, {
        method: "POST",
        body: form,
        headers: {},
    });
    const input = qs("messageInput");
    if (input) input.value = "";
}

async function sendAssetMessage(assetId) {
    if (!state.currentChatId) return;
    const form = new FormData();
    form.append("asset_id", String(assetId));
    await api(`/api/chats/${state.currentChatId}/messages/asset`, {
        method: "POST",
        body: form,
        headers: {},
    });
}

async function loadAssets() {
    state.assets = await api("/api/assets");
    const list = qs("assetsList");
    if (!list) return;
    list.innerHTML = "";
    state.assets.forEach((a) => {
        const preview =
            a.kind === "emoji" || a.kind === "sticker"
                ? `<img class="avatar avatar-md" src="${withMediaToken(a.file_url)}" alt="${escapeHtml(a.title || a.kind)}" loading="lazy">`
                : avatarMarkup({
                      label: a.kind,
                      seed: `asset-${a.id}`,
                      className: "avatar-md",
                  });
        const el = document.createElement("div");
        el.className = "item";
        el.innerHTML = `
            <div class="item-head">
                ${preview}
                <div class="item-copy">
                    <div class="item-title-row"><b>${escapeHtml(a.title || a.kind)}</b><span class="item-tag">${escapeHtml(a.kind)}</span></div>
                    <small>Персональный набор</small>
                </div>
            </div>
        `;
        const actions = document.createElement("div");
        actions.className = "actions";
        const send = document.createElement("button");
        send.textContent = "Отправить";
        send.onclick = async () => {
            await sendAssetMessage(a.id);
            qs("assetsDialog")?.close();
        };
        const del = document.createElement("button");
        del.className = "danger";
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
}

async function startVoiceRecord() {
    if (state.mediaRecorder) {
        state.mediaRecorder.stop();
        return;
    }
    try {
        const stream = await navigator.mediaDevices.getUserMedia({
            audio: true,
        });
        const chunks = [];
        const mimeType = pickRecorderMime("audio");
        const rec = mimeType
            ? new MediaRecorder(stream, { mimeType })
            : new MediaRecorder(stream);
        state.mediaRecorder = rec;
        const btn = qs("btnVoice");
        if (btn) btn.textContent = "Стоп";
        rec.ondataavailable = (e) => chunks.push(e.data);
        rec.onstop = async () => {
            const outMime = rec.mimeType || mimeType || "audio/webm";
            const ext = extForMime(outMime, "webm");
            const blob = new Blob(chunks, { type: outMime });
            const file = new File([blob], `voice_${Date.now()}.${ext}`, {
                type: outMime,
            });
            await sendMessage({ file, kind: "voice" });
            stream.getTracks().forEach((t) => t.stop());
            state.mediaRecorder = null;
            if (btn) btn.textContent = "Голос";
        };
        rec.start();
    } catch (e) {
        alert("Ошибка доступа к микрофону: " + e.message);
    }
}

async function startCircleRecord() {
    if (state.circleRecorder) {
        state.circleRecorder.stop();
        return;
    }
    try {
        const stream = await navigator.mediaDevices.getUserMedia({
            video: true,
            audio: true,
        });
        const chunks = [];
        const mimeType = pickRecorderMime("video");
        const rec = mimeType
            ? new MediaRecorder(stream, { mimeType })
            : new MediaRecorder(stream);
        state.circleRecorder = rec;
        const btn = qs("btnCircle");
        if (btn) btn.textContent = "Стоп";
        rec.ondataavailable = (e) => chunks.push(e.data);
        rec.onstop = async () => {
            const outMime = rec.mimeType || mimeType || "video/webm";
            const ext = extForMime(outMime, "webm");
            const blob = new Blob(chunks, { type: outMime });
            const file = new File([blob], `circle_${Date.now()}.${ext}`, {
                type: outMime,
            });
            await sendMessage({ file, kind: "circle" });
            stream.getTracks().forEach((t) => t.stop());
            state.circleRecorder = null;
            if (btn) btn.textContent = "Кружок";
        };
        rec.start();
    } catch (e) {
        alert("Ошибка доступа к камере: " + e.message);
    }
}

// =============================================================================
// === ЛОГИКА ЗВОНКОВ ===
// =============================================================================

function getDefaultIceServers() {
    return [
        { urls: "stun:stun.l.google.com:19302" },
        { urls: "stun:stun1.l.google.com:19302" },
    ];
}

async function fetchIceServers() {
    try {
        const cfg = await api("/api/rtc-config");
        return cfg.ice_servers && cfg.ice_servers.length
            ? cfg.ice_servers
            : getDefaultIceServers();
    } catch {
        return getDefaultIceServers();
    }
}

// ─── Воспроизведение медиа ────────────────────────────────────────────────────
// forceMuted=true  → <video> без звука (звук идёт через отдельный <audio>)
// forceMuted=false → <audio> удалённого участника (нужен звук)
async function safePlayMedia(mediaEl, forceMuted = false) {
    if (!mediaEl) return;
    mediaEl.playsInline = true;
    mediaEl.autoplay = true;

    if (forceMuted) {
        mediaEl.muted = true;
        try {
            await mediaEl.play();
        } catch {}
        return;
    }

    // Пробуем сразу со звуком
    mediaEl.muted = false;
    try {
        await mediaEl.play();
        return;
    } catch {}

    // Fallback: muted → play → unmute (Safari)
    mediaEl.muted = true;
    try {
        await mediaEl.play();
        setTimeout(() => {
            mediaEl.muted = false;
        }, 300);
    } catch (e) {
        console.warn("safePlayMedia failed:", e);
    }
}

// ─── Tile участника ───────────────────────────────────────────────────────────
function createCallTile(userId, isLocal) {
    const grid = qs("callGrid");
    if (!grid) return null;

    // Удаляем дубли
    grid.querySelector(`[data-uid="${userId}"]`)?.remove();

    const tile = document.createElement("div");
    tile.className = `call-tile ${isLocal ? "local" : "remote"}`;
    tile.dataset.uid = String(userId);

    // <video> — ВСЕГДА muted, картинка без звука
    const video = document.createElement("video");
    video.autoplay = true;
    video.playsInline = true;
    video.setAttribute("playsinline", "");
    video.setAttribute("webkit-playsinline", "");
    video.muted = true;
    if (isLocal) video.style.transform = "scaleX(-1)";

    // <audio> — только звук, для удалённых участников
    const audio = document.createElement("audio");
    audio.autoplay = true;
    audio.muted = isLocal; // себя не слышим

    const who = document.createElement("div");
    who.className = "who";
    const nameSpan = document.createElement("span");
    nameSpan.textContent = isLocal ? "Вы" : peerNameById(userId);
    const stateSpan = document.createElement("span");
    stateSpan.className = "tile-state";
    stateSpan.textContent = "🎤";
    who.appendChild(nameSpan);
    who.appendChild(stateSpan);

    tile.appendChild(video);
    tile.appendChild(audio);
    tile.appendChild(who);
    grid.appendChild(tile);

    return { tile, video, audio, who, stateSpan };
}

// ─── Привязка удалённого потока к tile ───────────────────────────────────────
function attachRemoteStream(userId, stream) {
    let card = state.call.tiles.get(userId);
    if (!card) {
        card = createCallTile(userId, false);
        if (!card) return;
        state.call.tiles.set(userId, card);
    }

    state.call.remoteStreams.set(userId, stream);

    const videoTracks = stream.getVideoTracks();
    if (videoTracks.length > 0) {
        card.video.srcObject = new MediaStream(videoTracks);
        safePlayMedia(card.video, true); // видео без звука
    }

    const audioTracks = stream.getAudioTracks();
    if (audioTracks.length > 0) {
        card.audio.srcObject = new MediaStream(audioTracks);
        safePlayMedia(card.audio, false); // аудио со звуком
    }

    // Реагируем на новые треки (включилась камера)
    stream.onaddtrack = () => attachRemoteStream(userId, stream);
}

// ─── Построение RTCPeerConnection ─────────────────────────────────────────────
function buildPeerConnection(userId) {
    const pc = new RTCPeerConnection({
        iceServers: state.call.iceServers || getDefaultIceServers(),
        iceCandidatePoolSize: 4,
        bundlePolicy: "max-bundle",
        rtcpMuxPolicy: "require",
    });

    pc._userId = userId;
    pc._makingOffer = false;

    // Добавляем свои треки
    if (state.call.localStream) {
        state.call.localStream
            .getTracks()
            .forEach((t) => pc.addTrack(t, state.call.localStream));
    }

    pc.onicecandidate = ({ candidate }) => {
        if (candidate) sendCallSignal(userId, { type: "candidate", candidate });
    };

    pc.ontrack = ({ streams, track }) => {
        let stream = streams?.[0];
        if (!stream) {
            stream = state.call.remoteStreams.get(userId) || new MediaStream();
            stream.addTrack(track);
            state.call.remoteStreams.set(userId, stream);
        }
        attachRemoteStream(userId, stream);
    };

    pc.onconnectionstatechange = () => {
        console.log(`Peer ${userId}: ${pc.connectionState}`);
        const card = state.call.tiles.get(userId);
        if (card)
            card.tile.style.opacity =
                pc.connectionState === "connected" ? "1" : "0.6";
        if (pc.connectionState === "failed") pc.restartIce();
    };

    pc.oniceconnectionstatechange = () => {
        if (pc.iceConnectionState === "failed") pc.restartIce();
    };

    // КРИТИЧНО: переговоры при динамическом добавлении треков (включили камеру)
    pc.onnegotiationneeded = async () => {
        if (!state.call.active || pc._makingOffer) return;
        try {
            pc._makingOffer = true;
            const offer = await pc.createOffer({
                offerToReceiveAudio: true,
                offerToReceiveVideo: true,
            });
            if (pc.signalingState !== "stable") return;
            await pc.setLocalDescription(offer);
            sendCallSignal(userId, { type: "offer", sdp: pc.localDescription });
        } catch (e) {
            console.error("onnegotiationneeded error:", e);
        } finally {
            pc._makingOffer = false;
        }
    };

    state.call.peers.set(userId, pc);
    return pc;
}

// Создаёт PC и отправляет первый offer
async function createOfferPeer(userId) {
    userId = Number(userId);
    if (state.call.peers.has(userId)) return state.call.peers.get(userId);

    const pc = buildPeerConnection(userId);
    pc._makingOffer = true;
    try {
        const offer = await pc.createOffer({
            offerToReceiveAudio: true,
            offerToReceiveVideo: true,
        });
        await pc.setLocalDescription(offer);
        sendCallSignal(userId, { type: "offer", sdp: pc.localDescription });
    } catch (e) {
        console.error("createOfferPeer failed:", e);
    } finally {
        pc._makingOffer = false;
    }
    return pc;
}

// ─── Буфер ICE кандидатов ─────────────────────────────────────────────────────
async function flushCandidates(userId, pc) {
    const buffered = state.call.pendingCandidates.get(userId) || [];
    state.call.pendingCandidates.delete(userId);
    for (const c of buffered) {
        try {
            await pc.addIceCandidate(new RTCIceCandidate(c));
        } catch {}
    }
}

// ─── Обработка входящих сигналов ─────────────────────────────────────────────
async function handleCallSignal(fromUserId, signal) {
    if (!state.call.active) return;
    fromUserId = Number(fromUserId);

    // Вежливый = у кого ID больше (отступает при столкновении offer)
    const polite = state.me.id > fromUserId;

    try {
        if (signal.type === "offer") {
            let pc = state.call.peers.get(fromUserId);
            if (!pc) pc = buildPeerConnection(fromUserId);

            const collision = pc._makingOffer || pc.signalingState !== "stable";
            if (collision) {
                if (!polite) return;
                await pc.setLocalDescription({ type: "rollback" });
            }

            await pc.setRemoteDescription(
                new RTCSessionDescription(signal.sdp),
            );
            const answer = await pc.createAnswer();
            await pc.setLocalDescription(answer);
            sendCallSignal(fromUserId, {
                type: "answer",
                sdp: pc.localDescription,
            });
            await flushCandidates(fromUserId, pc);
        } else if (signal.type === "answer") {
            const pc = state.call.peers.get(fromUserId);
            if (!pc || pc.signalingState !== "have-local-offer") return;
            await pc.setRemoteDescription(
                new RTCSessionDescription(signal.sdp),
            );
            await flushCandidates(fromUserId, pc);
        } else if (signal.type === "candidate") {
            const pc = state.call.peers.get(fromUserId);
            if (!pc || !pc.remoteDescription) {
                if (!state.call.pendingCandidates.has(fromUserId)) {
                    state.call.pendingCandidates.set(fromUserId, []);
                }
                state.call.pendingCandidates
                    .get(fromUserId)
                    .push(signal.candidate);
            } else {
                await pc.addIceCandidate(new RTCIceCandidate(signal.candidate));
            }
        }
    } catch (e) {
        console.error("handleCallSignal error:", e);
    }
}

function sendCallSignal(toUserId, signal) {
    if (!state.ws || state.ws.readyState !== WebSocket.OPEN) return;
    state.ws.send(
        JSON.stringify({
            type: "call:signal",
            chat_id: state.call.chatId,
            to_user: toUserId,
            signal,
        }),
    );
}

// ─── Удаление участника ───────────────────────────────────────────────────────
function removePeer(userId) {
    userId = Number(userId);
    const pc = state.call.peers.get(userId);
    if (pc) {
        pc.close();
        state.call.peers.delete(userId);
    }
    const card = state.call.tiles.get(userId);
    if (card) {
        card.tile.remove();
        state.call.tiles.delete(userId);
    }
    state.call.remoteStreams.delete(userId);
    state.call.pendingCandidates.delete(userId);
}

// ─── Управление звонком ───────────────────────────────────────────────────────

async function startCall() {
    if (!state.currentChat || state.call.active) return;
    await unlockAudioContext();

    if (!state.ws || state.ws.readyState !== WebSocket.OPEN) {
        alert("Нет соединения. Попробуйте ещё раз.");
        return;
    }

    state.call.iceServers = await fetchIceServers();

    try {
        const audioStream = await navigator.mediaDevices.getUserMedia({
            audio: {
                echoCancellation: true,
                noiseSuppression: true,
                autoGainControl: true,
            },
            video: false,
        });
        state.call.localStream = new MediaStream();
        audioStream
            .getAudioTracks()
            .forEach((t) => state.call.localStream.addTrack(t));
    } catch (e) {
        alert("Нет доступа к микрофону: " + e.message);
        return;
    }

    state.call.active = true;
    state.call.chatId = state.currentChatId;
    state.call.startedAt = Date.now();
    state.call.mic = true;
    state.call.cam = false;
    state.call.screen = false;

    const grid = qs("callGrid");
    if (grid) grid.innerHTML = "";
    state.call.peers.clear();
    state.call.tiles.clear();
    state.call.remoteStreams.clear();
    state.call.pendingCandidates.clear();

    const localCard = createCallTile(state.me?.id, true);
    if (localCard) {
        localCard.video.srcObject = new MediaStream(
            state.call.localStream.getTracks(),
        );
        safePlayMedia(localCard.video, true);
        state.call.tiles.set(state.me?.id, localCard);
    }

    const titleEl = qs("callTitleLabel");
    if (titleEl)
        titleEl.textContent = `Звонок: ${state.currentChat.title || state.currentChat.peer?.nickname || "чат"}`;

    show(qs("callOverlay"));
    hide(qs("btnCallRestore"));
    updateCallButtons();
    startCallTimer();

    state.ws.send(
        JSON.stringify({
            type: "call:join",
            chat_id: state.call.chatId,
            mic: true,
            cam: false,
            screen: false,
        }),
    );
}

function leaveCall() {
    if (!state.call.active) return;

    // Сохраняем chatId ДО очистки состояния
    const chatId = state.call.chatId;

    state.call.localStream?.getTracks().forEach((t) => t.stop());
    state.call.peers.forEach((pc) => pc.close());
    state.call.peers.clear();
    state.call.tiles.clear();
    state.call.remoteStreams.clear();
    state.call.pendingCandidates.clear();
    state.call.localStream = null;
    state.call.active = false;
    state.call.chatId = null;
    state.call.cam = false;
    state.call.screen = false;
    state.call.mic = true;

    stopCallTimer();

    // Отправляем с сохранённым chatId
    if (state.ws?.readyState === WebSocket.OPEN) {
        state.ws.send(JSON.stringify({ type: "call:leave", chat_id: chatId }));
    }

    hide(qs("callOverlay"));
    hide(qs("btnCallRestore"));
    const grid = qs("callGrid");
    if (grid) grid.innerHTML = "";
    updateCallButtons();
    state.ui.callMinimized = false;
}

async function toggleMic() {
    if (!state.call.active) return;
    state.call.mic = !state.call.mic;
    state.call.localStream?.getAudioTracks().forEach((t) => {
        t.enabled = state.call.mic;
    });
    updateLocalTileState();
    broadcastCallState();
    updateCallButtons();
}

async function toggleCam() {
    if (!state.call.active) return;

    const existing = state.call.localStream?.getVideoTracks()[0];

    if (existing) {
        // Выключаем камеру
        existing.stop();
        state.call.localStream.removeTrack(existing);
        state.call.cam = false;
        state.call.screen = false;

        for (const pc of state.call.peers.values()) {
            const sender = pc
                .getSenders()
                .find((s) => s.track?.kind === "video");
            if (sender) await sender.replaceTrack(null);
        }
    } else {
        // Включаем камеру
        try {
            const constraints = isMobile
                ? {
                      video: {
                          facingMode: { ideal: state.devicePrefs.camFacing },
                      },
                  }
                : { video: { width: { ideal: 1280 }, height: { ideal: 720 } } };
            const stream =
                await navigator.mediaDevices.getUserMedia(constraints);
            const track = stream.getVideoTracks()[0];
            state.call.localStream.addTrack(track);
            state.call.cam = true;
            state.call.screen = false;

            for (const pc of state.call.peers.values()) {
                const sender = pc
                    .getSenders()
                    .find((s) => s.track?.kind === "video");
                if (sender) {
                    await sender.replaceTrack(track);
                } else {
                    // addTrack → onnegotiationneeded → автоматические переговоры
                    pc.addTrack(track, state.call.localStream);
                }
            }
        } catch (e) {
            alert("Нет доступа к камере: " + e.message);
            return;
        }
    }

    const localCard = state.call.tiles.get(state.me?.id);
    if (localCard) {
        const vtracks = state.call.localStream?.getVideoTracks() || [];
        if (vtracks.length) {
            localCard.video.srcObject = new MediaStream(vtracks);
            safePlayMedia(localCard.video, true);
        } else {
            localCard.video.srcObject = null;
        }
    }

    updateLocalTileState();
    broadcastCallState();
    updateCallButtons();
}

async function toggleScreenShare() {
    if (!state.call.active) return;
    if (isMobile || !navigator.mediaDevices?.getDisplayMedia) {
        alert("Демонстрация экрана недоступна на мобильных устройствах");
        return;
    }

    if (state.call.screen) {
        state.call.localStream?.getVideoTracks().forEach((t) => {
            t.stop();
            state.call.localStream.removeTrack(t);
        });
        state.call.screen = false;
        state.call.cam = false;
        for (const pc of state.call.peers.values()) {
            const sender = pc
                .getSenders()
                .find((s) => s.track?.kind === "video");
            if (sender) await sender.replaceTrack(null);
        }
    } else {
        try {
            const stream = await navigator.mediaDevices.getDisplayMedia({
                video: true,
                audio: false,
            });
            const track = stream.getVideoTracks()[0];
            state.call.localStream?.getVideoTracks().forEach((t) => {
                t.stop();
                state.call.localStream.removeTrack(t);
            });
            state.call.localStream.addTrack(track);
            state.call.screen = true;
            state.call.cam = true;

            for (const pc of state.call.peers.values()) {
                const sender = pc
                    .getSenders()
                    .find((s) => s.track?.kind === "video");
                if (sender) {
                    await sender.replaceTrack(track);
                } else {
                    pc.addTrack(track, state.call.localStream);
                }
            }
            track.onended = () => {
                if (state.call.screen) toggleScreenShare();
            };
        } catch (e) {
            console.error("Screen share failed:", e);
            return;
        }
    }

    const localCard = state.call.tiles.get(state.me?.id);
    if (localCard) {
        const vtracks = state.call.localStream?.getVideoTracks() || [];
        if (vtracks.length) {
            localCard.video.srcObject = new MediaStream(vtracks);
            safePlayMedia(localCard.video, true);
        } else {
            localCard.video.srcObject = null;
        }
    }

    updateLocalTileState();
    broadcastCallState();
    updateCallButtons();
}

async function rotateCamera() {
    state.devicePrefs.camFacing =
        state.devicePrefs.camFacing === "environment" ? "user" : "environment";
    if (!state.call.cam || state.call.screen) return;

    const oldTrack = state.call.localStream?.getVideoTracks()[0];
    if (oldTrack) {
        oldTrack.stop();
        state.call.localStream.removeTrack(oldTrack);
    }

    try {
        const stream = await navigator.mediaDevices.getUserMedia({
            video: { facingMode: { exact: state.devicePrefs.camFacing } },
        });
        const newTrack = stream.getVideoTracks()[0];
        state.call.localStream.addTrack(newTrack);

        for (const pc of state.call.peers.values()) {
            const sender = pc
                .getSenders()
                .find((s) => s.track?.kind === "video");
            if (sender) await sender.replaceTrack(newTrack);
        }

        const localCard = state.call.tiles.get(state.me?.id);
        if (localCard) {
            localCard.video.srcObject = new MediaStream([newTrack]);
            safePlayMedia(localCard.video, true);
        }
    } catch (e) {
        console.error("rotateCamera failed:", e);
    }
}

function updateLocalTileState() {
    const card = state.call.tiles.get(state.me?.id);
    if (!card) return;
    card.stateSpan.textContent = `${state.call.mic ? "🎤" : "🔇"} ${state.call.cam ? (state.call.screen ? "🖥️" : "📷") : ""}`;
}

function broadcastCallState() {
    if (
        !state.ws ||
        state.ws.readyState !== WebSocket.OPEN ||
        !state.call.active
    )
        return;
    state.ws.send(
        JSON.stringify({
            type: "call:state",
            chat_id: state.call.chatId,
            mic: state.call.mic,
            cam: state.call.cam,
            screen: state.call.screen,
        }),
    );
}

function setRemoteTileState(userId, s) {
    if (!s) return;
    const card = state.call.tiles.get(Number(userId));
    if (!card) return;
    card.stateSpan.textContent = `${s.mic ? "🎤" : "🔇"} ${s.cam ? (s.screen ? "🖥️" : "📷") : ""}`;
}

function startCallTimer() {
    if (state.call.timer) clearInterval(state.call.timer);
    state.call.timer = setInterval(() => {
        const el = qs("callTimer");
        if (el)
            el.textContent = fmtDuration(
                Math.floor((Date.now() - state.call.startedAt) / 1000),
            );
    }, 1000);
}

function stopCallTimer() {
    if (state.call.timer) {
        clearInterval(state.call.timer);
        state.call.timer = null;
    }
}

function updateCallButtons() {
    const btnMic = qs("btnToggleMic");
    const btnCam = qs("btnToggleCam");
    const btnScreen = qs("btnShareScreen");
    const btnRotate = qs("btnRotateCam");
    if (btnMic) {
        btnMic.textContent = state.call.mic ? "🎤" : "🔇";
        btnMic.title = state.call.mic
            ? "Выключить микрофон"
            : "Включить микрофон";
    }
    if (btnCam) {
        btnCam.textContent = state.call.cam
            ? state.call.screen
                ? "🖥️"
                : "📷"
            : "🚫";
        btnCam.title = state.call.cam ? "Выключить камеру" : "Включить камеру";
    }
    if (btnScreen) {
        const canShare = !isMobile && !!navigator.mediaDevices?.getDisplayMedia;
        btnScreen.textContent = state.call.screen ? "🖥️" : "🪟";
        btnScreen.disabled = !canShare;
        canShare ? show(btnScreen) : hide(btnScreen);
    }
    if (btnRotate) {
        if (isMobile && state.call.active) show(btnRotate);
        else hide(btnRotate);
    }
}

// ─── WS-обработчик событий звонка ────────────────────────────────────────────
function handleCallWebSocketMessage(msg) {
    // call:ring разрешён даже без активного звонка
    if (msg.type === "call:ring") {
        const chat = state.chats.find((c) => c.id === msg.payload.chat_id);
        state.ui.incomingCall = {
            chatId: msg.payload.chat_id,
            title: chat?.title || chat?.peer?.nickname || "Входящий звонок",
        };
        const el = qs("incomingCallText");
        if (el)
            el.textContent = `Входящий звонок: ${state.ui.incomingCall.title}`;
        show(qs("incomingCallToast"));
        return;
    }

    if (!state.call.active) return;

    switch (msg.type) {
        case "call:participants": {
            const others = (msg.payload.users || []).map(Number);
            const states = msg.payload.states || {};
            others.forEach(async (uid) => {
                if (uid === state.me?.id) return;
                setRemoteTileState(uid, states[uid]);
                // Инициатор — у кого меньший ID
                if (state.me.id < uid) await createOfferPeer(uid);
            });
            break;
        }
        case "call:user_joined": {
            const uid = Number(msg.payload.user_id);
            if (uid === state.me?.id) break;
            setRemoteTileState(uid, msg.payload.state);
            if (state.me.id < uid) createOfferPeer(uid);
            break;
        }
        case "call:user_left":
            removePeer(Number(msg.payload.user_id));
            break;
        case "call:signal":
            handleCallSignal(Number(msg.payload.from_user), msg.payload.signal);
            break;
        case "call:user_state":
            setRemoteTileState(
                Number(msg.payload.user_id),
                msg.payload.state || {},
            );
            break;
    }
}

// =============================================================================
// === КОНЕЦ ЛОГИКИ ЗВОНКОВ ===
// =============================================================================

// ─── Устройства ──────────────────────────────────────────────────────────────

async function listMediaDevices() {
    try {
        const devices = await navigator.mediaDevices.enumerateDevices();
        return {
            mics: devices.filter((d) => d.kind === "audioinput"),
            cams: devices.filter((d) => d.kind === "videoinput"),
            speakers: devices.filter((d) => d.kind === "audiooutput"),
        };
    } catch {
        return { mics: [], cams: [], speakers: [] };
    }
}

function fillSelect(selectEl, devices, selectedId, fallbackLabel) {
    if (!selectEl) return;
    selectEl.innerHTML = "";
    const auto = document.createElement("option");
    auto.value = "";
    auto.textContent = fallbackLabel;
    selectEl.appendChild(auto);
    devices.forEach((d) => {
        const opt = document.createElement("option");
        opt.value = d.deviceId;
        opt.textContent = d.label || d.deviceId.slice(0, 8);
        if (selectedId && selectedId === d.deviceId) opt.selected = true;
        selectEl.appendChild(opt);
    });
}

async function refreshDevicePanel() {
    try {
        await navigator.mediaDevices.getUserMedia({ audio: true, video: true });
    } catch (_) {}
    const { mics, cams, speakers } = await listMediaDevices();
    fillSelect(
        qs("selMic"),
        mics,
        state.devicePrefs.micId,
        "Системный микрофон",
    );
    fillSelect(qs("selCam"), cams, state.devicePrefs.camId, "Системная камера");
    fillSelect(
        qs("selSpeaker"),
        speakers,
        state.devicePrefs.speakerId,
        "Системный вывод",
    );
    const speakerSel = qs("selSpeaker");
    if (speakerSel) {
        if (!("setSinkId" in HTMLMediaElement.prototype)) {
            speakerSel.disabled = true;
            speakerSel.title = "Safari/iOS ограничивает выбор аудиовыхода";
        } else {
            speakerSel.disabled = false;
            speakerSel.title = "";
        }
    }
    const modeSel = qs("selAudioMode");
    if (modeSel) modeSel.value = state.devicePrefs.audioMode || "speaker";
}

async function applySpeakerToMedia(mediaEl) {
    if (!mediaEl || typeof mediaEl.setSinkId !== "function") return;
    try {
        await mediaEl.setSinkId(state.devicePrefs.speakerId || "");
    } catch (_) {}
}

async function applySpeakerToAllTiles() {
    for (const [uid, card] of state.call.tiles.entries()) {
        if (uid === state.me?.id) continue;
        if (card.audio) await applySpeakerToMedia(card.audio);
        if (card.video) await applySpeakerToMedia(card.video);
    }
}

async function switchMicDevice(deviceId) {
    state.devicePrefs.micId = deviceId || "";
    if (!state.call.active) return;
    try {
        const stream = await navigator.mediaDevices.getUserMedia({
            audio: state.devicePrefs.micId
                ? { deviceId: { exact: state.devicePrefs.micId } }
                : true,
            video: false,
        });
        const newTrack = stream.getAudioTracks()[0];
        if (!newTrack) return;
        newTrack.enabled = state.call.mic;
        state.call.localStream?.getAudioTracks().forEach((t) => {
            t.stop();
            state.call.localStream.removeTrack(t);
        });
        state.call.localStream?.addTrack(newTrack);
        for (const pc of state.call.peers.values()) {
            const sender = pc
                .getSenders()
                .find((s) => s.track?.kind === "audio");
            if (sender) await sender.replaceTrack(newTrack);
        }
    } catch (e) {
        console.error("switchMicDevice failed:", e);
    }
}

async function switchCamDevice(deviceId) {
    state.devicePrefs.camId = deviceId || "";
    if (!state.call.active || !state.call.cam) return;
    try {
        const stream = await navigator.mediaDevices.getUserMedia({
            video: state.devicePrefs.camId
                ? { deviceId: { exact: state.devicePrefs.camId } }
                : true,
        });
        const newTrack = stream.getVideoTracks()[0];
        if (!newTrack) return;
        state.call.localStream?.getVideoTracks().forEach((t) => {
            t.stop();
            state.call.localStream.removeTrack(t);
        });
        state.call.localStream?.addTrack(newTrack);
        for (const pc of state.call.peers.values()) {
            const sender = pc
                .getSenders()
                .find((s) => s.track?.kind === "video");
            if (sender) await sender.replaceTrack(newTrack);
        }
        const localCard = state.call.tiles.get(state.me?.id);
        if (localCard) {
            localCard.video.srcObject = new MediaStream(
                state.call.localStream.getTracks(),
            );
            safePlayMedia(localCard.video, true);
        }
    } catch (e) {
        console.error("switchCamDevice failed:", e);
    }
}

async function applyAudioMode(mode) {
    state.devicePrefs.audioMode = mode || "speaker";
    const audioSession = navigator.audioSession;
    if (audioSession && "type" in audioSession) {
        try {
            audioSession.type =
                mode === "phone" ? "play-and-record" : "playback";
        } catch (_) {}
    }
    await applySpeakerToAllTiles();
}

// ─── Контекстное меню сообщений ──────────────────────────────────────────────

let _ctxMenuEl = null;
let _ctxLongPressTimer = null;

function hideContextMenu() {
    if (_ctxMenuEl) {
        _ctxMenuEl.remove();
        _ctxMenuEl = null;
    }
}

function showMessageContextMenu(x, y, msgEl) {
    hideContextMenu();
    const mid = Number(msgEl.dataset.mid);
    const isMine = msgEl.dataset.mine === "1";
    if (!mid) return;

    const menu = document.createElement("div");
    menu.className = "msg-ctx-menu";
    menu.style.cssText = `position:fixed;z-index:200;left:${x}px;top:${y}px`;
    _ctxMenuEl = menu;

    const btnDelMe = document.createElement("button");
    btnDelMe.textContent = "🗑️ Удалить у себя";
    btnDelMe.onclick = async () => {
        hideContextMenu();
        try {
            await api(`/api/messages/${mid}?mode=me`, { method: "DELETE" });
            msgEl.remove();
        } catch (e) {
            alert(e.message);
        }
    };
    menu.appendChild(btnDelMe);

    if (isMine) {
        const btnDelAll = document.createElement("button");
        btnDelAll.textContent = "🗑️ Удалить у всех";
        btnDelAll.onclick = async () => {
            hideContextMenu();
            if (!confirm("Удалить сообщение у всех?")) return;
            try {
                await api(`/api/messages/${mid}?mode=all`, {
                    method: "DELETE",
                });
            } catch (e) {
                alert(e.message);
            }
        };
        menu.appendChild(btnDelAll);
    }

    document.body.appendChild(menu);
    const rect = menu.getBoundingClientRect();
    if (rect.right > window.innerWidth - 8)
        menu.style.left = `${window.innerWidth - rect.width - 8}px`;
    if (rect.bottom > window.innerHeight - 8)
        menu.style.top = `${y - rect.height}px`;
    setTimeout(
        () =>
            document.addEventListener("click", hideContextMenu, { once: true }),
        0,
    );
}

function bindMessageContextMenu(container) {
    container.addEventListener("contextmenu", (e) => {
        const msgEl = e.target.closest(".message[data-mid]");
        if (!msgEl) return;
        e.preventDefault();
        showMessageContextMenu(e.clientX, e.clientY, msgEl);
    });
    container.addEventListener(
        "touchstart",
        (e) => {
            const msgEl = e.target.closest(".message[data-mid]");
            if (!msgEl) return;
            const touch = e.touches[0];
            _ctxLongPressTimer = setTimeout(() => {
                if (navigator.vibrate) navigator.vibrate(30);
                showMessageContextMenu(touch.clientX, touch.clientY, msgEl);
            }, 550);
        },
        { passive: true },
    );
    container.addEventListener(
        "touchend",
        () => clearTimeout(_ctxLongPressTimer),
        { passive: true },
    );
    container.addEventListener(
        "touchmove",
        () => clearTimeout(_ctxLongPressTimer),
        { passive: true },
    );
}

// ─── Вставка изображения из буфера обмена ────────────────────────────────────

function bindClipboardPaste(inputEl) {
    inputEl.addEventListener("paste", async (e) => {
        if (!state.currentChatId) return;
        const items = e.clipboardData?.items;
        if (!items) return;
        for (const item of items) {
            if (item.type.startsWith("image/")) {
                const file = item.getAsFile();
                if (!file) continue;
                e.preventDefault();
                const ext = item.type.split("/")[1] || "png";
                const named = new File([file], `paste_${Date.now()}.${ext}`, {
                    type: item.type,
                });
                try {
                    await sendMessage({ file: named, kind: "image" });
                } catch (err) {
                    console.error("Paste send error:", err);
                }
                return;
            }
        }
    });
}

// ─── Панель участников ────────────────────────────────────────────────────────

function openMembersSheet() {
    const panel = qs("groupMembersPanel");
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
    const panel = qs("groupMembersPanel");
    const backdrop = qs("membersBackdrop");
    if (!panel) return;
    panel.classList.remove("sheet-visible");
    if (backdrop) backdrop.classList.remove("open");
    const onEnd = () => {
        panel.classList.remove("members-modal-open");
        hide(panel);
        if (backdrop) hide(backdrop);
        panel.removeEventListener("transitionend", onEnd);
    };
    panel.addEventListener("transitionend", onEnd);
    document.removeEventListener("keydown", _membersEscHandler);
    setTimeout(() => {
        if (!panel.classList.contains("sheet-visible")) {
            panel.classList.remove("members-modal-open");
            hide(panel);
            if (backdrop) hide(backdrop);
        }
    }, 400);
}

function _membersEscHandler(e) {
    if (e.key === "Escape") closeMembersSheet();
}

// ─── WebSocket ────────────────────────────────────────────────────────────────

function stopWsHeartbeat() {
    if (state.wsMeta.pingTimer) {
        clearInterval(state.wsMeta.pingTimer);
        state.wsMeta.pingTimer = null;
    }
    if (state.wsMeta.pongTimer) {
        clearTimeout(state.wsMeta.pongTimer);
        state.wsMeta.pongTimer = null;
    }
}

function scheduleWsReconnect() {
    if (state.wsMeta.reconnectTimer) return;
    const delay = Math.min(6000, 900 + state.wsMeta.retry * 450);
    state.wsMeta.retry += 1;
    state.wsMeta.reconnectTimer = setTimeout(() => {
        state.wsMeta.reconnectTimer = null;
        connectWs();
    }, delay);
}

function startWsHeartbeat() {
    stopWsHeartbeat();
    state.wsMeta.pingTimer = setInterval(() => {
        if (!state.ws || state.ws.readyState !== WebSocket.OPEN) return;
        state.ws.send(JSON.stringify({ type: "ping" }));
        if (state.wsMeta.pongTimer) clearTimeout(state.wsMeta.pongTimer);
        state.wsMeta.pongTimer = setTimeout(() => {
            try {
                state.ws.close();
            } catch (_) {}
        }, 9000);
    }, 15000);
}

function resetCallPeersForRejoin() {
    state.call.peers.forEach((pc) => pc.close());
    state.call.peers.clear();
    state.call.pendingCandidates.clear();

    const grid = qs("callGrid");
    if (grid) grid.innerHTML = "";
    state.call.tiles.clear();

    if (state.call.localStream) {
        const localCard = createCallTile(state.me?.id, true);
        if (localCard) {
            localCard.video.srcObject = new MediaStream(
                state.call.localStream.getTracks(),
            );
            safePlayMedia(localCard.video, true);
            state.call.tiles.set(state.me?.id, localCard);
        }
    }
}

async function syncCurrentChatIfOpen() {
    if (!state.currentChatId) return;
    if (state.ws && state.ws.readyState === WebSocket.OPEN) return;
    const prev = qs("messages");
    if (!prev) return;
    const atBottom =
        prev.scrollHeight - prev.scrollTop - prev.clientHeight < 60;
    try {
        const data = await api(`/api/chats/${state.currentChatId}/messages`);
        const existingIds = new Set(
            Array.from(prev.querySelectorAll("[data-mid]")).map((el) =>
                Number(el.dataset.mid),
            ),
        );
        let added = 0;
        data.forEach((m) => {
            if (!existingIds.has(m.id)) {
                appendMessage(m);
                added++;
            }
        });
        if (added > 0 && atBottom) prev.scrollTop = prev.scrollHeight;
    } catch (_) {}
}

function startFallbackSync() {
    if (state.syncTimer) clearInterval(state.syncTimer);
    state.syncTimer = setInterval(async () => {
        try {
            await Promise.all([
                loadChats(),
                loadFriends(),
                loadFriendRequests(),
            ]);
            await loadGroupInvites();
            await syncCurrentChatIfOpen();
        } catch (_) {}
    }, 12000);
}

function connectWs() {
    if (!state.token) return;
    if (
        state.ws &&
        (state.ws.readyState === WebSocket.OPEN ||
            state.ws.readyState === WebSocket.CONNECTING)
    )
        return;
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(
        `${proto}://${location.host}/ws?token=${encodeURIComponent(state.token)}`,
    );
    state.ws = ws;

    ws.onopen = async () => {
        state.wsMeta.retry = 0;
        stopWsHeartbeat();
        startWsHeartbeat();
        try {
            await Promise.all([
                loadChats(),
                loadFriends(),
                loadFriendRequests(),
            ]);
            await syncCurrentChatIfOpen();
        } catch (_) {}
        if (state.call.active && state.call.chatId) {
            resetCallPeersForRejoin();
            ws.send(
                JSON.stringify({
                    type: "call:join",
                    chat_id: state.call.chatId,
                    mic: state.call.mic,
                    cam: state.call.cam,
                    screen: state.call.screen,
                }),
            );
        }
    };

    ws.onmessage = async (ev) => {
        let msg;
        try {
            msg = JSON.parse(ev.data);
        } catch {
            return;
        }

        if (msg.type === "pong") {
            if (state.wsMeta.pongTimer) {
                clearTimeout(state.wsMeta.pongTimer);
                state.wsMeta.pongTimer = null;
            }
            return;
        }

        if (msg.type === "message:new") {
            if (msg.payload.chat_id === state.currentChatId) {
                appendMessage(msg.payload);
                if (msg.payload.user_id !== state.me?.id)
                    markChatRead(state.currentChatId);
            }
            loadChats();
        }
        if (msg.type === "message:read") {
            const { chat_id, reader_id, up_to_id } = msg.payload;
            if (reader_id !== state.me?.id && chat_id === state.currentChatId)
                updateReadStatusUpTo(up_to_id);
        }
        if (msg.type === "message:deleted_all") {
            if (msg.payload.chat_id === state.currentChatId)
                removeMessageById(msg.payload.message_id);
            loadChats();
        }
        if (msg.type === "message:deleted_me") {
            if (msg.payload.chat_id === state.currentChatId)
                removeMessageById(msg.payload.message_id);
            loadChats();
        }
        if (
            msg.type === "friend:request" ||
            msg.type === "friend:accepted" ||
            msg.type === "chat:added"
        )
            refreshSide();
        if (msg.type === "group:invite" || msg.type === "group:invite_answer")
            loadGroupInvites();
        if (msg.type === "group:deleted") {
            if (state.currentChatId === msg.payload.chat_id) {
                state.currentChatId = null;
                state.currentChat = null;
                const msgEl = qs("messages");
                const memEl = qs("chatMembers");
                if (msgEl) msgEl.innerHTML = "";
                if (memEl) memEl.innerHTML = "";
                setChatHeader(null);
                setChatOpen(false);
            }
            refreshSide();
        }
        if (msg.type === "group:member_removed") {
            if (
                msg.payload.user_id === state.me?.id &&
                state.currentChatId === msg.payload.chat_id
            ) {
                state.currentChatId = null;
                state.currentChat = null;
                const msgEl = qs("messages");
                const memEl = qs("chatMembers");
                if (msgEl) msgEl.innerHTML = "";
                if (memEl) memEl.innerHTML = "";
                setChatHeader(null);
                setChatOpen(false);
            }
            if (state.currentChatId === msg.payload.chat_id)
                loadMembers(msg.payload.chat_id);
            refreshSide();
        }
        if (msg.type === "group:member_role") {
            if (state.currentChatId === msg.payload.chat_id)
                loadMembers(msg.payload.chat_id);
            loadChats();
        }
        if (msg.type === "user:blocked") refreshSide();

        // Звонки
        handleCallWebSocketMessage(msg);
    };

    ws.onclose = () => {
        stopWsHeartbeat();
        scheduleWsReconnect();
    };
    ws.onerror = () => {
        try {
            ws.close();
        } catch (_) {}
    };
}

// ─── Сессия и настройки ──────────────────────────────────────────────────────

async function ensureSession() {
    if (!state.token) return false;
    try {
        state.me = await api("/api/me");
        return true;
    } catch (_) {
        localStorage.removeItem("token");
        state.token = "";
        return false;
    }
}

async function loadSettings() {
    state.settings = await api("/api/settings");
    const setFriendReq = qs("setFriendReq");
    const setCalls = qs("setCalls");
    const setInvites = qs("setInvites");
    const setLastSeen = qs("setLastSeen");
    if (setFriendReq) setFriendReq.value = state.settings.allow_friend_requests;
    if (setCalls) setCalls.value = state.settings.allow_calls_from;
    if (setInvites) setInvites.value = state.settings.allow_group_invites;
    if (setLastSeen) setLastSeen.value = state.settings.show_last_seen;
}

async function onAuthorized() {
    hide(qs("gateScreen"));
    hide(qs("authScreen"));
    show(qs("app"));
    renderProfileMini();
    await Promise.all([
        loadChats(),
        loadFriends(),
        loadFriendRequests(),
        loadGroupInvites(),
        loadSettings(),
    ]);
    connectWs();
    startFallbackSync();
}

function resetToAuthUi() {
    hide(qs("app"));
    show(qs("authScreen"));
    hide(qs("gateScreen"));
    setError("authError", "");
    setError("gateError", "");
    state.me = null;
    state.settings = null;
    state.chats = [];
    state.friends = [];
    state.currentChat = null;
    state.currentChatId = null;
}

// ─── Привязка UI ─────────────────────────────────────────────────────────────

function bindUi() {
    setMainTab("chats");
    setChatOpen(false);
    setEmptyState(true);
    const gateCode = qs("gateCode");
    if (gateCode)
        gateCode.value = localStorage.getItem("saved_gate_code") || "";

    const tabChats = qs("tabChats");
    const tabRequests = qs("tabRequests");
    const tabSearch = qs("tabSearch");
    const tabFriends = qs("tabFriends");
    const btnBack = qs("btnBackToList");
    const btnMobile = qs("btnMobileMenu");
    const gateBtn = qs("gateBtn");
    const tabLogin = qs("tabLogin");
    const tabRegister = qs("tabRegister");
    const loginBtn = qs("loginBtn");
    const registerBtn = qs("registerBtn");
    const btnLogout = qs("btnLogout");
    const chatSearch = qs("chatSearch");
    const sendBtn = qs("sendBtn");
    const messageInput = qs("messageInput");
    const btnFile = qs("btnFile");
    const fileInput = qs("fileInput");
    const btnAssets = qs("btnAssets");
    const assetsDialog = qs("assetsDialog");
    const assetsClose = qs("assetsClose");
    const btnUploadAsset = qs("btnUploadAsset");
    const btnVoice = qs("btnVoice");
    const btnCircle = qs("btnCircle");
    const btnProfile = qs("btnProfile");
    const profileDialog = qs("profileDialog");
    const profileClose = qs("profileClose");
    const profileSave = qs("profileSave");
    const btnGroup = qs("btnGroup");
    const groupDialog = qs("groupDialog");
    const groupClose = qs("groupClose");
    const groupCreate = qs("groupCreate");
    const btnInviteGroup = qs("btnInviteGroup");
    const btnLeaveChat = qs("btnLeaveChat");
    const btnDeleteGroup = qs("btnDeleteGroup");
    const btnFriends = qs("btnFriends");
    const btnCopyMyId = qs("btnCopyMyId");
    const userSearch = qs("userSearch");
    const btnSettings = qs("btnSettings");
    const settingsDialog = qs("settingsDialog");
    const settingsClose = qs("settingsClose");
    const settingsSave = qs("settingsSave");
    const btnChangePassword = qs("btnChangePassword");
    const btnDeleteAccount = qs("btnDeleteAccount");
    const btnCallStart = qs("btnCallStart");
    const btnShowMembers = qs("btnShowMembers");
    const btnLeaveCall = qs("btnLeaveCall");
    const btnToggleMic = qs("btnToggleMic");
    const btnToggleCam = qs("btnToggleCam");
    const btnRotateCam = qs("btnRotateCam");
    const btnShareScreen = qs("btnShareScreen");
    const btnDevices = qs("btnDevices");
    const devicePanel = qs("devicePanel");
    const selMic = qs("selMic");
    const selCam = qs("selCam");
    const selSpeaker = qs("selSpeaker");
    const selAudioMode = qs("selAudioMode");
    const btnMinimizeCall = qs("btnMinimizeCall");
    const btnCallRestore = qs("btnCallRestore");
    const btnIncomingAccept = qs("btnIncomingAccept");
    const btnIncomingDecline = qs("btnIncomingDecline");
    const incomingCallToast = qs("incomingCallToast");

    if (tabChats) tabChats.onclick = () => setMainTab("chats");
    if (tabRequests) tabRequests.onclick = () => setMainTab("requests");
    if (tabSearch) tabSearch.onclick = () => setMainTab("search");
    if (tabFriends) tabFriends.onclick = () => setMainTab("friends");
    if (btnBack) btnBack.onclick = () => setChatOpen(false);
    if (btnMobile)
        btnMobile.onclick = () => document.body.classList.toggle("menu-open");

    if (gateBtn)
        gateBtn.onclick = async () => {
            setError("gateError", "");
            try {
                const code = qs("gateCode")?.value || "";
                await api("/api/gate", {
                    method: "POST",
                    body: JSON.stringify({ code }),
                });
                if (qs("rememberCode")?.checked)
                    localStorage.setItem("saved_gate_code", code);
                hide(qs("gateScreen"));
                show(qs("authScreen"));
            } catch (e) {
                setError("gateError", e.message);
            }
        };

    if (tabLogin)
        tabLogin.onclick = () => {
            tabLogin.classList.add("active");
            tabRegister?.classList.remove("active");
            show(qs("loginPane"));
            hide(qs("registerPane"));
        };
    if (tabRegister)
        tabRegister.onclick = () => {
            tabRegister.classList.add("active");
            tabLogin?.classList.remove("active");
            hide(qs("loginPane"));
            show(qs("registerPane"));
        };

    if (loginBtn)
        loginBtn.onclick = async () => {
            setError("authError", "");
            try {
                const r = await api("/api/login", {
                    method: "POST",
                    body: JSON.stringify({
                        username: qs("loginUsername")?.value || "",
                        password: qs("loginPassword")?.value || "",
                    }),
                });
                state.token = r.token;
                state.me = r.user;
                localStorage.setItem("token", state.token);
                await onAuthorized();
            } catch (e) {
                setError("authError", e.message);
            }
        };

    if (registerBtn)
        registerBtn.onclick = async () => {
            setError("authError", "");
            try {
                const r = await api("/api/register", {
                    method: "POST",
                    body: JSON.stringify({
                        username: qs("regUsername")?.value || "",
                        password: qs("regPassword")?.value || "",
                        nickname: qs("regNickname")?.value || "",
                    }),
                });
                state.token = r.token;
                state.me = r.user;
                localStorage.setItem("token", state.token);
                await onAuthorized();
            } catch (e) {
                setError("authError", e.message);
            }
        };

    if (btnLogout)
        btnLogout.onclick = async () => {
            try {
                await api("/api/logout", { method: "POST", body: "{}" });
            } catch (_) {}
            leaveCall();
            if (state.syncTimer) clearInterval(state.syncTimer);
            stopWsHeartbeat();
            try {
                state.ws?.close();
            } catch (_) {}
            localStorage.removeItem("token");
            state.token = "";
            resetToAuthUi();
        };

    if (chatSearch) chatSearch.oninput = () => renderChatList(chatSearch.value);
    if (sendBtn)
        sendBtn.onclick = () =>
            sendMessage({ text: messageInput?.value || "" });
    if (messageInput) {
        bindClipboardPaste(messageInput);
        messageInput.addEventListener("keydown", (e) => {
            if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                sendMessage({ text: messageInput.value || "" });
            }
        });
    }
    const messagesEl = qs("messages");
    if (messagesEl) bindMessageContextMenu(messagesEl);

    if (btnFile) btnFile.onclick = () => fileInput?.click();
    if (btnVoice) btnVoice.onclick = () => startVoiceRecord();
    if (btnCircle) btnCircle.onclick = () => startCircleRecord();

    if (btnAssets)
        btnAssets.onclick = async () => {
            await loadAssets();
            assetsDialog?.showModal();
        };
    if (assetsClose) assetsClose.onclick = () => assetsDialog?.close();
    if (btnUploadAsset)
        btnUploadAsset.onclick = async () => {
            const f = qs("assetFile")?.files[0];
            if (!f) {
                alert("Выберите файл");
                return;
            }
            const form = new FormData();
            form.append("kind", qs("assetKind")?.value || "emoji");
            form.append("title", qs("assetTitle")?.value || "");
            form.append("file", f);
            await api("/api/assets", {
                method: "POST",
                body: form,
                headers: {},
            });
            if (qs("assetFile")) qs("assetFile").value = "";
            if (qs("assetTitle")) qs("assetTitle").value = "";
            await loadAssets();
        };

    if (fileInput)
        fileInput.onchange = async () => {
            const f = fileInput.files[0];
            if (!f) return;
            let kind = "file";
            if (f.type.startsWith("image/")) kind = "image";
            if (f.type.startsWith("video/")) kind = "video";
            await sendMessage({ file: f, kind });
            fileInput.value = "";
        };

    if (btnProfile)
        btnProfile.onclick = () => {
            if (qs("profileNickname"))
                qs("profileNickname").value = state.me?.nickname || "";
            if (qs("profileAbout"))
                qs("profileAbout").value = state.me?.about || "";
            profileDialog?.showModal();
        };
    if (profileClose) profileClose.onclick = () => profileDialog?.close();
    if (profileSave)
        profileSave.onclick = async () => {
            state.me = await api("/api/profile", {
                method: "POST",
                body: JSON.stringify({
                    nickname: qs("profileNickname")?.value || "",
                    about: qs("profileAbout")?.value || "",
                }),
            });
            const avatar = qs("profileAvatar")?.files[0];
            if (avatar) {
                const form = new FormData();
                form.append("file", avatar);
                state.me = await api("/api/profile/avatar", {
                    method: "POST",
                    body: form,
                    headers: {},
                });
            }
            renderProfileMini();
            profileDialog?.close();
        };

    if (btnGroup) btnGroup.onclick = () => groupDialog?.showModal();
    if (groupClose) groupClose.onclick = () => groupDialog?.close();
    if (groupCreate)
        groupCreate.onclick = async () => {
            try {
                const members = (qs("groupMembers")?.value || "")
                    .split(",")
                    .map((v) => v.trim().replace(/^@/, "").toLowerCase())
                    .filter(Boolean);
                const out = await api("/api/groups", {
                    method: "POST",
                    body: JSON.stringify({
                        title: qs("groupTitle")?.value || "",
                        members,
                    }),
                });
                if (qs("groupTitle")) qs("groupTitle").value = "";
                if (qs("groupMembers")) qs("groupMembers").value = "";
                groupDialog?.close();
                await loadChats();
                await openChat(out.chat_id);
            } catch (e) {
                alert(e.message);
            }
        };

    if (btnInviteGroup)
        btnInviteGroup.onclick = async () => {
            if (!state.currentChat || state.currentChat.type !== "group")
                return;
            const raw = prompt("Введите @username для инвайта");
            const username = String(raw || "").trim();
            if (!username) return;
            try {
                await api(
                    `/api/groups/${state.currentChatId}/invite/username`,
                    { method: "POST", body: JSON.stringify({ username }) },
                );
                alert("Инвайт отправлен");
            } catch (e) {
                alert(e.message);
            }
        };

    if (btnLeaveChat)
        btnLeaveChat.onclick = async () => {
            if (!state.currentChatId) return;
            const targetName =
                state.currentChat?.type === "group" ? "эту группу" : "этот чат";
            if (!confirm(`Выйти из ${targetName}?`)) return;
            if (state.call.active && state.call.chatId === state.currentChatId)
                leaveCall();
            await api(`/api/chats/${state.currentChatId}/leave`, {
                method: "POST",
                body: "{}",
            });
            state.currentChat = null;
            state.currentChatId = null;
            const msgEl = qs("messages");
            const memEl = qs("chatMembers");
            if (msgEl) msgEl.innerHTML = "";
            if (memEl) memEl.innerHTML = "";
            setChatHeader(null);
            setChatOpen(false);
            await loadChats();
        };

    if (btnDeleteGroup)
        btnDeleteGroup.onclick = async () => {
            if (!state.currentChat || state.currentChat.type !== "group")
                return;
            if (!confirm("Удалить группу?")) return;
            await api(`/api/groups/${state.currentChatId}`, {
                method: "DELETE",
            });
            state.currentChat = null;
            state.currentChatId = null;
            const msgEl = qs("messages");
            const memEl = qs("chatMembers");
            if (msgEl) msgEl.innerHTML = "";
            if (memEl) memEl.innerHTML = "";
            setChatHeader(null);
            setChatOpen(false);
            await loadChats();
        };

    if (btnFriends) btnFriends.onclick = refreshSide;
    if (btnCopyMyId)
        btnCopyMyId.onclick = async () => {
            try {
                await navigator.clipboard.writeText(String(state.me?.id || ""));
                alert(`ID скопирован: ${state.me?.id}`);
            } catch (_) {
                prompt("Скопируйте ваш ID", String(state.me?.id || ""));
            }
        };

    if (userSearch)
        userSearch.oninput = async () => {
            const q = userSearch.value.trim();
            const out = qs("userResults");
            if (!out) return;
            if (q.length < 2) {
                out.innerHTML = "";
                return;
            }
            const users = await api(
                `/api/users/search?q=${encodeURIComponent(q)}`,
            );
            out.innerHTML = "";
            users.forEach((u) => {
                const el = document.createElement("div");
                el.className = "item";
                el.innerHTML = `
                <div class="item-head">
                    ${avatarMarkup({ avatar: u.avatar, label: u.nickname, seed: `search-${u.id}`, className: "avatar-md" })}
                    <div class="item-copy">
                        <div class="item-title-row"><b>${escapeHtml(u.nickname)}</b><span class="item-tag">User</span></div>
                        <small>@${escapeHtml(u.username)} #${u.id}</small>
                        ${u.about ? `<p class="item-preview">${escapeHtml(truncateText(u.about, 90))}</p>` : ""}
                    </div>
                </div>
            `;
                const actions = document.createElement("div");
                actions.className = "actions";
                const add = document.createElement("button");
                add.textContent = "В друзья";
                add.onclick = () =>
                    api("/api/friends/request", {
                        method: "POST",
                        body: JSON.stringify({ username: u.username }),
                    });
                const block = document.createElement("button");
                block.className = "danger";
                block.textContent = "Блок";
                block.onclick = async () => {
                    await api(`/api/users/${u.id}/block`, {
                        method: "POST",
                        body: "{}",
                    });
                    await refreshSide();
                };
                actions.appendChild(add);
                actions.appendChild(block);
                el.appendChild(actions);
                out.appendChild(el);
            });
        };

    if (btnSettings)
        btnSettings.onclick = async () => {
            await loadSettings();
            await loadBlockedList();
            settingsDialog?.showModal();
        };
    if (settingsClose) settingsClose.onclick = () => settingsDialog?.close();
    if (settingsSave)
        settingsSave.onclick = async () => {
            state.settings = await api("/api/settings", {
                method: "POST",
                body: JSON.stringify({
                    allow_friend_requests:
                        qs("setFriendReq")?.value || "everyone",
                    allow_calls_from: qs("setCalls")?.value || "friends",
                    allow_group_invites: qs("setInvites")?.value || "friends",
                    show_last_seen: qs("setLastSeen")?.value || "friends",
                }),
            });
            settingsDialog?.close();
        };

    if (btnChangePassword)
        btnChangePassword.onclick = async () => {
            const oldPassword = qs("oldPassword")?.value || "";
            const newPassword = qs("newPassword")?.value || "";
            if (!oldPassword || !newPassword) {
                alert("Введите старый и новый пароль");
                return;
            }
            await api("/api/account/password", {
                method: "POST",
                body: JSON.stringify({
                    old_password: oldPassword,
                    new_password: newPassword,
                }),
            });
            if (qs("oldPassword")) qs("oldPassword").value = "";
            if (qs("newPassword")) qs("newPassword").value = "";
            alert("Пароль успешно изменён");
        };

    if (btnDeleteAccount)
        btnDeleteAccount.onclick = async () => {
            if (
                !confirm(
                    "Удалить аккаунт безвозвратно? Это удалит ваши сессии и уберёт вас из чатов.",
                )
            )
                return;
            try {
                await api("/api/account", { method: "DELETE" });
            } catch (e) {
                alert(e.message);
                return;
            }
            leaveCall();
            if (state.syncTimer) clearInterval(state.syncTimer);
            stopWsHeartbeat();
            try {
                state.ws?.close();
            } catch (_) {}
            localStorage.removeItem("token");
            state.token = "";
            resetToAuthUi();
        };

    if (btnCallStart) btnCallStart.onclick = startCall;
    if (btnShowMembers) btnShowMembers.onclick = () => openMembersSheet();
    const btnCloseMembers = qs("btnCloseMembers");
    if (btnCloseMembers) btnCloseMembers.onclick = () => closeMembersSheet();
    const membersBackdrop = qs("membersBackdrop");
    if (membersBackdrop) membersBackdrop.onclick = () => closeMembersSheet();

    if (btnLeaveCall) btnLeaveCall.onclick = leaveCall;
    if (btnToggleMic) btnToggleMic.onclick = toggleMic;
    if (btnToggleCam) btnToggleCam.onclick = toggleCam;
    if (btnRotateCam) btnRotateCam.onclick = rotateCamera;
    if (btnShareScreen) btnShareScreen.onclick = toggleScreenShare;

    if (btnDevices)
        btnDevices.onclick = async () => {
            if (!devicePanel) return;
            if (devicePanel.classList.contains("hidden")) {
                await refreshDevicePanel();
                show(devicePanel);
            } else hide(devicePanel);
        };

    if (selMic) selMic.onchange = async () => switchMicDevice(selMic.value);
    if (selCam) selCam.onchange = async () => switchCamDevice(selCam.value);
    if (selSpeaker)
        selSpeaker.onchange = async () => {
            state.devicePrefs.speakerId = selSpeaker.value || "";
            await applySpeakerToAllTiles();
        };
    if (selAudioMode)
        selAudioMode.onchange = async () => applyAudioMode(selAudioMode.value);

    if (btnMinimizeCall)
        btnMinimizeCall.onclick = () => {
            hide(qs("callOverlay"));
            state.ui.callMinimized = true;
            show(qs("btnCallRestore"));
        };
    if (btnCallRestore)
        btnCallRestore.onclick = () => {
            if (!state.call.active) return;
            show(qs("callOverlay"));
            hide(qs("btnCallRestore"));
            state.ui.callMinimized = false;
        };

    if (btnIncomingAccept)
        btnIncomingAccept.onclick = async () => {
            await unlockAudioContext();
            const incoming = state.ui.incomingCall;
            hide(incomingCallToast);
            if (!incoming) return;
            const chat = state.chats.find((c) => c.id === incoming.chatId);
            if (chat) {
                await openChat(chat.id);
                await startCall();
            }
            state.ui.incomingCall = null;
        };
    if (btnIncomingDecline)
        btnIncomingDecline.onclick = () => {
            hide(incomingCallToast);
            state.ui.incomingCall = null;
        };
}

// ─── Boot ─────────────────────────────────────────────────────────────────────

async function boot() {
    if (window.__lanMessengerBooted) return;
    window.__lanMessengerBooted = true;
    installResponsiveEnvironment();
    bindUi();
    const sessionOk = await ensureSession();
    if (sessionOk) {
        await onAuthorized();
        return;
    }
    hide(qs("app"));
    hide(qs("gateScreen"));
    show(qs("authScreen"));
}

boot();
