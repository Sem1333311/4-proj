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
    },
  };
  
  const isLikelyIOS = /iPad|iPhone|iPod/.test(navigator.userAgent) || (navigator.platform === "MacIntel" && navigator.maxTouchPoints > 1);
  const isLikelyAndroid = /Android/.test(navigator.userAgent);
  const isMobile = isLikelyIOS || isLikelyAndroid;
  const isLikelySafari = /^((?!chrome|android|crios|fxios).)*safari/i.test(navigator.userAgent);
  const isLocalDevHost = ["localhost", "127.0.0.1", "[::1]"].includes(location.hostname);
  
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
    if (state.ui.chatOpen) show(back); else hide(back);
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
    const height = Math.round(window.visualViewport?.height || window.innerHeight || document.documentElement.clientHeight || 0);
    if (height > 0) {
        document.documentElement.style.setProperty("--app-height", `${height}px`);
    }
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
        return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    }
    return date.toLocaleDateString([], { day: "2-digit", month: "2-digit" });
  }
  
  function fmtDuration(sec) {
    const m = Math.floor(sec / 60);
    const s = sec % 60;
    return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
  }
  
  function pickRecorderMime(kind) {
    const options = kind === "audio"
        ? ["audio/mp4", "audio/webm;codecs=opus", "audio/webm"]
        : ["video/mp4", "video/webm;codecs=vp8,opus", "video/webm"];
    for (const m of options) {
        if (window.MediaRecorder && MediaRecorder.isTypeSupported && MediaRecorder.isTypeSupported(m)) return m;
    }
    return "";
  }
  
  function extForMime(mime, fallback) {
    if (!mime) return fallback;
    if (mime.includes("mp4")) return "mp4";
    if (mime.includes("webm")) return "webm";
    return fallback;
  }
  
  async function safePlay(mediaEl) {
    if (!mediaEl) return;
    // Запоминаем нужный muted-статус (local=true, remote=false)
    const targetMuted = mediaEl.muted;
    // Принудительно глушим — браузер разрешает autoplay только muted-видео
    mediaEl.muted = true;
    mediaEl.autoplay = true;
    mediaEl.playsInline = true;
    try {
        await mediaEl.play();
        // Восстанавливаем: для local остаётся muted, для remote — звук включается
        mediaEl.muted = targetMuted;
    } catch (e) {
        // Последний шанс: уже muted, пробуем ещё раз
        try { await mediaEl.play(); } catch (_) {}
        console.warn("Autoplay blocked:", e);
    }
  }
  
  async function api(path, opts = {}) {
    const headers = opts.headers || {};
    if (!(opts.body instanceof FormData)) {
        headers["Content-Type"] = "application/json";
    }
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
    if (!url) return url;
    if (!state.token) return url;
    return `${url}${url.includes("?") ? "&" : "?"}token=${encodeURIComponent(state.token)}`;
  }
  
  function avatarMediaUrl(avatar) {
    if (!avatar) return "";
    const fileName = String(avatar);
    return withMediaToken(fileName.startsWith("/media/") ? fileName : `/media/${encodeURIComponent(fileName)}`);
  }
  
  function truncateText(text, limit = 96) {
    const clean = String(text || "").trim();
    if (!clean) return "";
    return clean.length > limit ? `${clean.slice(0, limit).trimEnd()}...` : clean;
  }
  
  function toMultilineHtml(text) {
    return escapeHtml(text || "").replace(/\n/g, "<br>");
  }
  
  function seedHue(seed) {
    const source = String(seed || "lm");
    let hash = 0;
    for (let i = 0; i < source.length; i += 1) {
        hash = (hash * 31 + source.charCodeAt(i)) % 360;
    }
    return Math.abs(hash);
  }
  
  function initialsFromLabel(label) {
    const clean = String(label || "").trim();
    if (!clean) return "LM";
    const parts = clean.split(/\s+/).filter(Boolean);
    const letters = parts.slice(0, 2).map((part) => part[0]).join("");
    return (letters || clean.slice(0, 2)).toUpperCase();
  }
  
  function avatarMarkup({ avatar, label, seed, className = "avatar-md" }) {
    const classes = ["avatar", className].filter(Boolean).join(" ");
    const title = escapeHtml(label || "LM");
    const url = avatarMediaUrl(avatar);
    if (url) {
        return `<img class="${classes}" src="${url}" alt="${title}" loading="lazy">`;
    }
    return `<div class="${classes}" style="--avatar-hue:${seedHue(seed || label || "lm")}">${escapeHtml(initialsFromLabel(label))}</div>`;
  }
  
  function describeMemberRole(role) {
    const labels = {
        owner: "Владелец",
        admin: "Админ",
        member: "Участник",
    };
    return labels[role] || role || "Участник";
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
    return chat ? (chat.title || chat.peer?.nickname || "Чат") : "Выберите чат";
  }
  
  function chatMetaText(chat) {
    if (!chat) return "Откройте диалог слева или создайте новую группу.";
    if (chat.type === "group") {
        const count = state.membersById.size || chat.member_count || 0;
        return count ? `${count} участников` : "Групповой чат";
    }
    if (chat.peer?.username) {
        return `@${chat.peer.username}`;
    }
    return "Личный чат";
  }
  
  function setComposerEnabled(enabled) {
    const input = qs("messageInput");
    if (input) {
        input.disabled = !enabled;
        input.placeholder = enabled ? "Напишите сообщение..." : "Выберите чат, чтобы начать переписку";
    }
    ["sendBtn", "btnAssets", "btnFile", "btnVoice", "btnCircle"].forEach((id) => {
        const btn = qs(id);
        if (btn) btn.disabled = !enabled;
    });
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
    if (text) {
        parts.push(`<div class="message-text">${text}</div>`);
    }
    if (!m.file_url) return parts.join("");
    const fileUrl = withMediaToken(m.file_url);
    if (m.kind === "image" || m.kind === "sticker" || m.kind === "emoji") {
        parts.push(`<img src="${fileUrl}" alt="${escapeHtml(m.file_name || "Вложение")}" loading="lazy">`);
        return parts.join("");
    }
    if (m.kind === "video" || m.kind === "circle") {
        parts.push(`<video src="${fileUrl}" controls playsinline webkit-playsinline preload="metadata"></video>`);
        return parts.join("");
    }
    if (m.kind === "voice") {
        parts.push(`<audio src="${fileUrl}" controls preload="metadata"></audio>`);
        return parts.join("");
    }
    return `${text ? `${text}<br>` : ""}<a href="${fileUrl}" target="_blank">${escapeHtml(m.file_name || "Файл")}</a>`;
  }
  
  function appendMessage(m) {
    const mine = m.user_id === state.me?.id;
    const isRead = mine && (m.read_count > 0);
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
    const target = qs("messages")?.querySelector(`[data-mid="${messageId}"]`);
    if (target) target.remove();
  }
  
  function renderProfileMini() {
    if (!state.me) return;
    const el = qs("profileMini");
    if (el) {
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
  }
  
  function renderChatList(filter = "") {
    const q = filter.trim().toLowerCase();
    const list = qs("chatList");
    if (!list) return;
    list.innerHTML = "";
    state.chats
        .filter((c) => {
            const haystack = [
                c.title,
                c.peer?.nickname,
                c.peer?.username,
                c.last_text,
            ].filter(Boolean).join(" ").toLowerCase();
            return !q || haystack.includes(q);
        })
        .forEach((chat) => {
            const title = chatTitleText(chat);
            const preview = truncateText(chat.last_text || (chat.type === "group" ? "Групповой чат" : "Начните новый диалог"), 88);
            const subtitle = chat.type === "group"
                ? "Группа"
                : (chat.peer?.username ? `@${chat.peer.username}` : "Личный чат");
            const el = document.createElement("div");
            el.className = `item ${chat.id === state.currentChatId ? "selected" : ""}`;
            el.innerHTML = `
                <div class="item-head">
                    ${avatarMarkup({
                        avatar: chat.peer?.avatar,
                        label: title,
                        seed: `chat-${chat.id}-${title}`,
                        className: "avatar-md",
                    })}
                    <div class="item-copy">
                        <div class="item-title-row">
                            <b>${escapeHtml(title)}</b>
                            <span class="item-time">${escapeHtml(formatListTime(chat.last_at) || "")}</span>
                        </div>
                        <small>${escapeHtml(subtitle)}</small>
                        <p class="item-preview">${escapeHtml(preview)}</p>
                    </div>
                </div>
                <div class="item-tags">
                    <span class="item-tag">${chat.type === "group" ? "Group" : "DM"}</span>
                </div>
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
    if (titleEl) titleEl.textContent = titleText;
    if (metaEl) metaEl.textContent = chatMetaText(chat);
    if (avatarEl) {
        avatarEl.innerHTML = avatarMarkup({
            avatar: chat?.peer?.avatar,
            label: titleText,
            seed: chat ? `chat-${chat.id}-${titleText}` : "empty-chat",
            className: "avatar-xl avatar-placeholder",
        });
    }
    if (!chat) {
        state.membersById = new Map();
    }
    const btnShowMembers = qs("btnShowMembers");
    if (canCall) show(btnCall); else hide(btnCall);
    if (group) show(btnInvite); else hide(btnInvite);
    if (group) show(btnShowMembers); else hide(btnShowMembers);
    if (btnInvite) btnInvite.title = group ? "Пригласить по username" : "Пригласить";
    if (chat) show(btnLeave); else hide(btnLeave);
    if (btnLeave) {
        btnLeave.textContent = "🚪";
        btnLeave.title = group ? "Выйти из группы" : "Выйти из чата";
    }
    if (canDelete) show(btnDelete); else hide(btnDelete);
    if (btnDelete) {
        btnDelete.textContent = "🗑️";
        btnDelete.title = "Удалить группу";
    }
    if (group) show(membersPanel); else hide(membersPanel);
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
                const out = await api("/api/chats/direct", { method: "POST", body: JSON.stringify({ user_id: m.id }) });
                await loadChats();
                await openChat(out.chat_id);
            };
            const call = document.createElement("button");
            call.textContent = "📞";
            call.title = "Позвонить";
            call.onclick = async () => {
                const out = await api("/api/chats/direct", { method: "POST", body: JSON.stringify({ user_id: m.id }) });
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
            promote.title = m.role === "admin" ? "Сделать участником" : "Выдать админку";
            promote.onclick = async () => {
                try {
                    await api(`/api/groups/${chatId}/members/${m.id}/role`, {
                        method: "POST",
                        body: JSON.stringify({ role: m.role === "admin" ? "member" : "admin" }),
                    });
                    await Promise.all([loadMembers(chatId), loadChats()]);
                } catch (e) {
                    alert(e.message);
                }
            };
            actions.appendChild(promote);
            const transfer = document.createElement("button");
            transfer.className = "ghost";
            transfer.textContent = "👑";
            transfer.title = "Передать owner";
            transfer.onclick = async () => {
                if (!confirm(`Передать роль owner пользователю @${m.username}?`)) return;
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
            actions.appendChild(transfer);
        }
        if ((currentRole === "owner" && m.id !== state.me.id) || (currentRole === "admin" && m.role === "member" && m.id !== state.me.id)) {
            const kick = document.createElement("button");
            kick.className = "danger";
            kick.textContent = "⛔";
            kick.title = "Исключить из группы";
            kick.onclick = async () => {
                if (!confirm(`Исключить @${m.username} из группы?`)) return;
                try {
                    await api(`/api/groups/${chatId}/members/${m.id}`, { method: "DELETE" });
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
    if (state.currentChatId === chatId) {
        setChatHeader(state.currentChat);
    }
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
    // Участников загружаем только для групп
    if (chat && chat.type === "group") {
        await loadMembers(chatId);
    } else {
        state.membersById = new Map();
    }
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
                    <div class="item-title-row">
                        <b>${escapeHtml(f.nickname)}</b>
                        <span class="item-tag">Friend</span>
                    </div>
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
            const out = await api("/api/chats/direct", { method: "POST", body: JSON.stringify({ user_id: f.id }) });
            await loadChats();
            await openChat(out.chat_id);
        };
        const block = document.createElement("button");
        block.className = "danger";
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
            await api(`/api/friends/request/${r.id}/accept`, { method: "POST", body: "{}" });
            await refreshSide();
        };
        const no = document.createElement("button");
        no.className = "danger";
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
            await api(`/api/groups/invites/${r.id}/accept`, { method: "POST", body: "{}" });
            await Promise.all([loadGroupInvites(), loadChats()]);
        };
        const no = document.createElement("button");
        no.className = "danger";
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
    await Promise.all([loadFriends(), loadFriendRequests(), loadGroupInvites(), loadChats()]);
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
        const el = document.createElement("div");
        const preview = a.kind === "emoji" || a.kind === "sticker"
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
        const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
        const chunks = [];
        const mimeType = pickRecorderMime("audio");
        const rec = mimeType ? new MediaRecorder(stream, { mimeType }) : new MediaRecorder(stream);
        state.mediaRecorder = rec;
        const btn = qs("btnVoice");
        if (btn) btn.textContent = "Стоп";
        rec.ondataavailable = (e) => chunks.push(e.data);
        rec.onstop = async () => {
            const outMime = rec.mimeType || mimeType || "audio/webm";
            const ext = extForMime(outMime, "webm");
            const blob = new Blob(chunks, { type: outMime });
            const file = new File([blob], `voice_${Date.now()}.${ext}`, { type: outMime });
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
        const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
        const chunks = [];
        const mimeType = pickRecorderMime("video");
        const rec = mimeType ? new MediaRecorder(stream, { mimeType }) : new MediaRecorder(stream);
        state.circleRecorder = rec;
        const btn = qs("btnCircle");
        if (btn) btn.textContent = "Стоп";
        rec.ondataavailable = (e) => chunks.push(e.data);
        rec.onstop = async () => {
            const outMime = rec.mimeType || mimeType || "video/webm";
            const ext = extForMime(outMime, "webm");
            const blob = new Blob(chunks, { type: outMime });
            const file = new File([blob], `circle_${Date.now()}.${ext}`, { type: outMime });
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
  
  function updateCallButtons() {
    const btnMic = qs("btnToggleMic");
    const btnCam = qs("btnToggleCam");
    const btnScreen = qs("btnShareScreen");
    const btnRotate = qs("btnRotateCam");
    const btnDevices = qs("btnDevices");
    if (btnMic) {
        btnMic.textContent = state.call.mic ? "🎤" : "🔇";
        btnMic.title = state.call.mic ? "Выключить микрофон" : "Включить микрофон";
    }
    if (btnCam) {
        btnCam.textContent = state.call.cam ? (state.call.screen ? "🖥️" : "📷") : "🚫";
        btnCam.title = state.call.cam ? "Выключить камеру" : "Включить камеру";
    }
    if (btnScreen) {
        const canShareScreen = !isMobile && !!(navigator.mediaDevices && "getDisplayMedia" in navigator.mediaDevices);
        btnScreen.textContent = state.call.screen ? "🖥️" : "🪟";
        btnScreen.title = state.call.screen ? "Остановить демонстрацию" : "Поделиться экраном";
        btnScreen.disabled = !canShareScreen;
        if (canShareScreen) show(btnScreen); else hide(btnScreen);
    }
    if (btnRotate) {
        btnRotate.textContent = "🔄";
        btnRotate.title = "Сменить камеру";
        btnRotate.disabled = !state.call.active || state.call.screen;
        if (state.call.active) show(btnRotate); else hide(btnRotate);
    }
    if (btnDevices) {
        btnDevices.textContent = "🎧";
        btnDevices.title = "Устройства и звук";
    }
  }

  function updateCallTimer() {
    if (!state.call.startedAt) {
        const el = qs("callTimer");
        if (el) el.textContent = "00:00";
        return;
    }
    const sec = Math.max(0, Math.floor((Date.now() - state.call.startedAt) / 1000));
    const el = qs("callTimer");
    if (el) el.textContent = fmtDuration(sec);
  }
  
  function sendCallState() {
    if (!state.ws || state.ws.readyState !== WebSocket.OPEN || !state.call.active) return;
    state.ws.send(JSON.stringify({
        type: "call:state",
        chat_id: state.call.chatId,
        mic: state.call.mic,
        cam: state.call.cam,
        screen: state.call.screen,
    }));
  }
  
  // === ИСПРАВЛЕННАЯ ФУНКЦИЯ (ЗАЩИТА ОТ ДУБЛИКАТОВ И IOS) ===
  function ensureCallTile(userId, isLocal = false) {
    userId = Number(userId);
    if (isNaN(userId)) return null;
  
    if (state.call.tiles.has(userId)) return state.call.tiles.get(userId);
  
    const wrap = qs("callGrid");
    if (!wrap) return null;
  
    // Жестко удаляем зависшие копии карточки этого юзера в DOM
    const rogueTiles = wrap.querySelectorAll(`.call-tile[data-uid="${userId}"]`);
    rogueTiles.forEach(t => t.remove());
  
    const tile = document.createElement("div");
    tile.className = `call-tile ${isLocal ? "local" : ""}`;
    tile.dataset.uid = userId;
    
    const video = document.createElement("video");
    video.autoplay = true;
    video.playsInline = true; // Важно для iOS
    video.setAttribute("playsinline", "");
    video.setAttribute("webkit-playsinline", "");
    // Всегда стартуем muted — safePlay снимет mute после успешного play()
    video.muted = true;
    video.style.transform = isLocal ? "scaleX(-1)" : "none";
    
    const who = document.createElement("div");
    who.className = "who";
    const left = document.createElement("span");
    left.textContent = isLocal ? "Вы" : peerNameById(userId);
    const right = document.createElement("span");
    right.textContent = "mic:on cam:off";
    who.appendChild(left);
    who.appendChild(right);
    
    tile.appendChild(video);
    tile.appendChild(who);
    wrap.appendChild(tile);
    
    const card = { tile, video, right };
    state.call.tiles.set(userId, card);
    return card;
  }
  
  function setTileState(userId, statePayload = {}) {
    const card = ensureCallTile(userId, userId === state.me?.id);
    if (!card) return;
    const mic = statePayload.mic ? "on" : "off";
    const cam = statePayload.cam ? (statePayload.screen ? "screen" : "on") : "off";
    card.right.textContent = `mic:${mic} cam:${cam}`;
    // Для удалённых участников: если камера появилась и видео на паузе — запустить
    if (userId !== state.me?.id && cam !== "off" && card.video?.srcObject && card.video.paused) {
        safePlay(card.video);
    }
  }
  
  function removePeer(userId) {
    userId = Number(userId);
    const pc = state.call.peers.get(userId);
    if (pc) pc.close();
    state.call.peers.delete(userId);
    state.call.remoteStreams.delete(userId);
    const card = state.call.tiles.get(userId);
    if (card) card.tile.remove();
    state.call.tiles.delete(userId);
    
    // Дополнительная очистка мусора в DOM
    const wrap = qs("callGrid");
    if (wrap) {
        wrap.querySelectorAll(`.call-tile[data-uid="${userId}"]`).forEach(t => t.remove());
    }
  }
  
  function attachStreamToPeer(userId, stream) {
    const card = ensureCallTile(userId, false);
    if (!card || !card.video) return;
    card.video.srcObject = stream;
    card.video.muted = false;
    applySpeakerToMedia(card.video);
    safePlay(card.video);
  }
  
  async function createLocalAudioIfMissing() {
    if (!state.call.localStream) {
        state.call.localStream = new MediaStream();
    }
    const hasAudio = state.call.localStream.getAudioTracks().length > 0;
    if (hasAudio) return;
    try {
        const micStream = await navigator.mediaDevices.getUserMedia({
            audio: state.devicePrefs.micId ? { deviceId: { exact: state.devicePrefs.micId } } : true,
            video: false,
        });
        const track = micStream.getAudioTracks()[0];
        state.call.localStream.addTrack(track);
        track.enabled = state.call.mic;
    } catch (e) {
        console.error("Failed to get audio:", e);
    }
  }
  
  async function listMediaDevices() {
    try {
        const devices = await navigator.mediaDevices.enumerateDevices();
        const mics = devices.filter((d) => d.kind === "audioinput");
        const cams = devices.filter((d) => d.kind === "videoinput");
        const speakers = devices.filter((d) => d.kind === "audiooutput");
        return { mics, cams, speakers };
    } catch (e) {
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
    fillSelect(qs("selMic"), mics, state.devicePrefs.micId, "Системный микрофон");
    fillSelect(qs("selCam"), cams, state.devicePrefs.camId, "Системная камера");
    fillSelect(qs("selSpeaker"), speakers, state.devicePrefs.speakerId, "Системный вывод");
    const speakerSelect = qs("selSpeaker");
    if (speakerSelect) {
        if (!("setSinkId" in HTMLMediaElement.prototype)) {
            speakerSelect.disabled = true;
            speakerSelect.title = "Safari/iOS ограничивает выбор аудиовыхода в браузере";
        } else {
            speakerSelect.disabled = false;
            speakerSelect.title = "";
        }
    }
    const modeSelect = qs("selAudioMode");
    if (modeSelect) modeSelect.value = state.devicePrefs.audioMode || "speaker";
  }
  
  async function applySpeakerToMedia(mediaEl) {
    if (!mediaEl) return;
    if (typeof mediaEl.setSinkId !== "function") return;
    const sink = state.devicePrefs.speakerId || "";
    try {
        await mediaEl.setSinkId(sink);
    } catch (_) {}
  }
  
  async function applySpeakerToAllTiles() {
    for (const [uid, card] of state.call.tiles.entries()) {
        if (uid === state.me?.id) continue;
        await applySpeakerToMedia(card.video);
    }
  }
  
  async function switchMicDevice(deviceId) {
    state.devicePrefs.micId = deviceId || "";
    if (!state.call.active) return;
    try {
        const stream = await navigator.mediaDevices.getUserMedia({
            audio: state.devicePrefs.micId ? { deviceId: { exact: state.devicePrefs.micId } } : true,
            video: false,
        });
        const track = stream.getAudioTracks()[0];
        if (!track) return;
        track.enabled = state.call.mic;
        state.call.localStream.getAudioTracks().forEach((t) => {
            t.stop();
            state.call.localStream.removeTrack(t);
        });
        state.call.localStream.addTrack(track);
        for (const pc of state.call.peers.values()) {
            const audioSender = getPeerSender(pc, "audio");
            if (audioSender) {
                await audioSender.replaceTrack(track);
            }
        }
    } catch (e) {
        console.error("Failed to switch mic:", e);
    }
  }
  
  async function switchCamDevice(deviceId) {
    state.devicePrefs.camId = deviceId || "";
    if (!state.call.active || !state.call.cam) return;
    try {
        const stream = await navigator.mediaDevices.getUserMedia({
            video: getRequestedVideoConstraints(),
            audio: false,
        });
        const track = stream.getVideoTracks()[0];
        if (!track) return;
        state.call.localStream.getVideoTracks().forEach((t) => {
            t.stop();
            state.call.localStream.removeTrack(t);
        });
        state.call.localStream.addTrack(track);
        for (const pc of state.call.peers.values()) {
            const videoSender = getPeerSender(pc, "video");
            if (videoSender) {
                await videoSender.replaceTrack(track);
            }
        }
        const localCard = state.call.tiles.get(state.me?.id);
        if (localCard) {
            localCard.video.srcObject = new MediaStream(state.call.localStream.getTracks());
            await safePlay(localCard.video);
        }
        await renegotiateAllPeers();
        updateCallButtons();
    } catch (e) {
        console.error("Failed to switch cam:", e);
    }
  }

  async function rotateCamera() {
    if (isMobile) {
        // На iOS/Android надёжнее всего переключать через facingMode, а не deviceId
        state.devicePrefs.camFacing = state.devicePrefs.camFacing === "environment" ? "user" : "environment";
        state.devicePrefs.camId = "";
    } else {
        const { cams } = await listMediaDevices();
        if (cams.length > 1) {
            const currentIndex = Math.max(0, cams.findIndex((cam) => cam.deviceId === state.devicePrefs.camId));
            const nextCam = cams[(currentIndex + 1) % cams.length];
            state.devicePrefs.camId = nextCam?.deviceId || "";
        }
    }
    if (state.call.active && state.call.cam && !state.call.screen) {
        await switchCamDevice(state.devicePrefs.camId);
    }
  }
  
  async function applyAudioMode(mode) {
    state.devicePrefs.audioMode = mode || "speaker";
    const audioSession = navigator.audioSession;
    if (audioSession && typeof audioSession === "object" && "type" in audioSession) {
        try {
            audioSession.type = state.devicePrefs.audioMode === "phone" ? "play-and-record" : "playback";
        } catch (_) {}
    }
    if (!state.call.active) return;
    if (isLikelyIOS && isLikelySafari && !("setSinkId" in HTMLMediaElement.prototype)) {
        await applySpeakerToAllTiles();
        return;
    }
    if (!state.devicePrefs.speakerId) {
        try {
            const { speakers } = await listMediaDevices();
            if (speakers.length) {
                const modeNeedle = state.devicePrefs.audioMode === "phone" ? ["head", "ear", "phone"] : ["speaker", "spk", "loud"];
                const preferred = speakers.find((s) => {
                    const label = (s.label || "").toLowerCase();
                    return modeNeedle.some((n) => label.includes(n));
                });
                if (preferred) state.devicePrefs.speakerId = preferred.deviceId;
            }
        } catch (_) {}
    }
    await applySpeakerToAllTiles();
  }

  // === ИСПРАВЛЕННАЯ ФУНКЦИЯ (ИДЕАЛЬНЫЙ ПАТТЕРН ПЕРЕГОВОРОВ) ===
  function getPeerSender(pc, kind) {
    if (!pc) return null;
    const cached = kind === "audio" ? pc._audioSender : pc._videoSender;
    if (cached) return cached;
    const fallback = pc.getSenders().find((sender) => sender.track?.kind === kind) || null;
    if (kind === "audio") pc._audioSender = fallback;
    if (kind === "video") pc._videoSender = fallback;
    return fallback;
  }

  async function renegotiatePeer(userId) {
    const uid = Number(userId);
    const pc = state.call.peers.get(uid);
    if (!pc || pc.signalingState !== "stable" || pc._makingOffer) return;
    try {
        pc._makingOffer = true;
        const offer = await pc.createOffer({
            offerToReceiveAudio: true,
            offerToReceiveVideo: true,
        });
        await pc.setLocalDescription(offer);
        if (state.ws && state.ws.readyState === WebSocket.OPEN) {
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

  function getRequestedVideoConstraints() {
    if (state.devicePrefs.camId) {
        return { deviceId: { exact: state.devicePrefs.camId } };
    }
    const facing = state.devicePrefs.camFacing || "user";
    if (isMobile) {
        // iOS требует exact для надёжного переключения; Android - ideal
        return { facingMode: isLikelyIOS ? { exact: facing } : { ideal: facing } };
    }
    return true;
  }
  
  function attachTrackToPeer(userId, track, streamHint = null) {
    const card = ensureCallTile(userId, false);
    if (!card || !card.video || !track) return;
    let remoteStream = state.call.remoteStreams.get(userId);
    if (!remoteStream) {
        remoteStream = new MediaStream();
        state.call.remoteStreams.set(userId, remoteStream);
    }
    const toAdd = (streamHint && streamHint.getTracks().length > 0) ? streamHint.getTracks() : [track];
    toAdd.forEach((t) => {
        if (!remoteStream.getTracks().some((e) => e.id === t.id)) {
            remoteStream.addTrack(t);
        }
    });
    card.video.srcObject = remoteStream;
    card.video.style.transform = "none";
    // Явно указываем что удалённый тайл должен звучать
    card.video.muted = false;
    applySpeakerToMedia(card.video);
    safePlay(card.video);
  }

  async function ensurePeer(userId, createOffer) {
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
    pc._audioSender = null;
    pc._videoSender = null;
    pc._pendingCandidates = []; // буфер ICE до setRemoteDescription
  
    // Транзиверы добавляем ТОЛЬКО когда мы инициатор (offerer).
    // Answerer получит транзиверы из offer через setRemoteDescription.
    if (createOffer) {
        const localAudio = state.call.localStream?.getAudioTracks()[0] || null;
        const localVideo = state.call.localStream?.getVideoTracks()[0] || null;
        pc._audioSender = pc.addTransceiver("audio", { direction: "sendrecv" }).sender;
        pc._videoSender = pc.addTransceiver("video", { direction: "sendrecv" }).sender;
        if (localAudio) await pc._audioSender.replaceTrack(localAudio);
        if (localVideo) await pc._videoSender.replaceTrack(localVideo);
    }

    pc.onnegotiationneeded = async () => {
        if (!state.call.active || !createOffer) return;
        await renegotiatePeer(userId);
    };

    pc.onicecandidate = (ev) => {
        if (!ev.candidate) return;
        if (!state.ws || state.ws.readyState !== WebSocket.OPEN) return;
        state.ws.send(JSON.stringify({
            type: "call:signal",
            chat_id: state.call.chatId,
            to_user: userId,
            signal: { type: "candidate", candidate: ev.candidate },
        }));
    };
  
    pc.ontrack = (ev) => {
        const remoteTrack = ev.track;
        const remoteStream = ev.streams?.[0] || null;
        attachTrackToPeer(userId, remoteTrack, remoteStream);
        remoteTrack.onunmute = () => attachTrackToPeer(userId, remoteTrack, remoteStream);
        remoteTrack.onended = () => {
            const stored = state.call.remoteStreams.get(userId);
            if (!stored) return;
            stored.getTracks().forEach((existing) => {
                if (existing.id === remoteTrack.id) stored.removeTrack(existing);
            });
        };
        // iOS/Android: повторная попытка воспроизведения
        setTimeout(() => {
            if (!state.call.active) return;
            const card = state.call.tiles.get(userId);
            if (card?.video?.paused && card.video.srcObject) safePlay(card.video);
        }, 900);
    };
  
    pc.onconnectionstatechange = () => {
        console.log(`Peer ${userId}: ${pc.connectionState}`);
        if (pc.connectionState === "failed") {
            if (state.call.peers.get(userId) === pc) {
                try { pc.restartIce(); } catch (e) {}
            }
        }
    };
  
    state.call.peers.set(userId, pc);
  
    if (createOffer) {
        try {
            pc._makingOffer = true;
            const offer = await pc.createOffer({
                offerToReceiveAudio: true,
                offerToReceiveVideo: true,
            });
            await pc.setLocalDescription(offer);
            if (state.ws && state.ws.readyState === WebSocket.OPEN) {
                state.ws.send(JSON.stringify({
                    type: "call:signal",
                    chat_id: state.call.chatId,
                    to_user: userId,
                    signal: { type: "offer", sdp: pc.localDescription },
                }));
            }
        } catch (e) {
            console.error("Failed to create offer:", e);
        } finally {
            pc._makingOffer = false;
        }
    }
    return pc;
  }
  
  async function handleSignal(fromUser, signal) {
    if (!state.call.active) return;
    fromUser = Number(fromUser);
    
    const isPolite = state.me?.id < fromUser;
    const pc = await ensurePeer(fromUser, false);
  
    try {
        if (signal.type === "offer") {
            const offerCollision = pc._makingOffer || pc.signalingState !== "stable";
            pc._ignoreOffer = !isPolite && offerCollision;
            if (pc._ignoreOffer) return;
  
            await pc.setRemoteDescription(new RTCSessionDescription(signal.sdp));

            // Answerer: прописываем локальные треки в транзиверы из offer
            const localAudio = state.call.localStream?.getAudioTracks()[0] || null;
            const localVideo = state.call.localStream?.getVideoTracks()[0] || null;
            for (const t of pc.getTransceivers()) {
                if (!pc._audioSender && t.receiver?.track?.kind === "audio" && localAudio) {
                    pc._audioSender = t.sender;
                    try { await t.sender.replaceTrack(localAudio); } catch(e) {}
                } else if (!pc._videoSender && t.receiver?.track?.kind === "video") {
                    pc._videoSender = t.sender;
                    if (localVideo) try { await t.sender.replaceTrack(localVideo); } catch(e) {}
                }
            }

            const answer = await pc.createAnswer();
            await pc.setLocalDescription(answer);
            if (state.ws && state.ws.readyState === WebSocket.OPEN) {
                state.ws.send(JSON.stringify({
                    type: "call:signal",
                    chat_id: state.call.chatId,
                    to_user: fromUser,
                    signal: { type: "answer", sdp: pc.localDescription },
                }));
            }
            // Сбрасываем буферизованные ICE-кандидаты
            for (const c of pc._pendingCandidates) {
                try { await pc.addIceCandidate(new RTCIceCandidate(c)); } catch(e) {}
            }
            pc._pendingCandidates = [];
        } else if (signal.type === "answer") {
            if (pc.signalingState !== "have-local-offer") return;
            await pc.setRemoteDescription(new RTCSessionDescription(signal.sdp));
            // Сбрасываем буферизованные ICE-кандидаты
            for (const c of pc._pendingCandidates) {
                try { await pc.addIceCandidate(new RTCIceCandidate(c)); } catch(e) {}
            }
            pc._pendingCandidates = [];
        } else if (signal.type === "candidate") {
            // Если remote description ещё не установлен — буферизуем
            if (!pc.remoteDescription) {
                pc._pendingCandidates.push(signal.candidate);
            } else {
                try { await pc.addIceCandidate(new RTCIceCandidate(signal.candidate)); } catch(e) {}
            }
        }
    } catch (e) {
        console.error("Signal handling error:", e);
    }
  }
  
  async function startCall() {
    if (!state.currentChat) return;
    if (state.call.active) return;
    if (!state.ws || state.ws.readyState !== WebSocket.OPEN) {
        connectWs();
        alert("Соединение восстанавливается. Попробуйте начать звонок через 1-2 секунды.");
        return;
    }
    
    let iceServers = isLocalDevHost ? [] : [
        { urls: "stun:stun.l.google.com:19302" },
        { urls: "stun:stun1.l.google.com:19302" },
    ];
    try {
        const config = await api("/api/rtc-config");
        if (!isLocalDevHost && config.ice_servers && config.ice_servers.length > 0) {
            iceServers = config.ice_servers;
        }
    } catch (_) {}

    state.call.active = true;
    state.call.chatId = state.currentChatId;
    state.call.startedAt = Date.now();
    state.call.mic = true;
    state.call.cam = false;
    state.call.screen = false;
    state.call.iceServers = iceServers;
    state.call.peers.clear();
    state.call.tiles.clear();
    state.call.remoteStreams.clear();
    
    const callGrid = qs("callGrid");
    if (callGrid) callGrid.innerHTML = "";
    
    state.call.localStream = new MediaStream();
    await createLocalAudioIfMissing();
    
    const localCard = ensureCallTile(state.me?.id, true);
    if (localCard) {
        localCard.video.srcObject = new MediaStream(state.call.localStream.getTracks());
        localCard.video.muted = true; // себя не слышим никогда
        await safePlay(localCard.video);
    }
    
    setTileState(state.me?.id, { mic: state.call.mic, cam: state.call.cam, screen: state.call.screen });
    await applyAudioMode(state.devicePrefs.audioMode || "speaker");
    
    const callTitleLabel = qs("callTitleLabel");
    if (callTitleLabel) {
        callTitleLabel.textContent = state.currentChat
            ? `Звонок: ${state.currentChat.title || state.currentChat.peer?.nickname || "чат"}`
            : "Звонок";
    }
    
    show(qs("callOverlay"));
    hide(qs("btnCallRestore"));
    updateCallButtons();
    updateCallTimer();
    state.call.timer = setInterval(updateCallTimer, 1000);
    
    state.ws.send(JSON.stringify({
        type: "call:join",
        chat_id: state.call.chatId,
        mic: state.call.mic,
        cam: state.call.cam,
        screen: state.call.screen,
    }));
  }

  function stopCallTimer() {
    if (state.call.timer) {
        clearInterval(state.call.timer);
        state.call.timer = null;
    }
  }
  
  function stopAllLocalTracks() {
    if (!state.call.localStream) return;
    state.call.localStream.getTracks().forEach((t) => t.stop());
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
    state.call.remoteStreams.clear();
    state.call.screen = false;
    state.call.cam = false;
    state.call.mic = true;
    state.call.iceServers = null;
    stopCallTimer();
    hide(qs("callOverlay"));
    hide(qs("btnCallRestore"));
    const callTitleLabel = qs("callTitleLabel");
    if (callTitleLabel) callTitleLabel.textContent = "Звонок";
    const callGrid = qs("callGrid");
    if (callGrid) callGrid.innerHTML = "";
    updateCallButtons();
    state.ui.callMinimized = false;
  }

  function leaveCall() {
    if (!state.call.active) return;
    if (state.ws && state.ws.readyState === WebSocket.OPEN) {
        state.ws.send(JSON.stringify({ type: "call:leave", chat_id: state.call.chatId }));
    }
    resetCallState();
  }
  
  async function toggleMic() {
    if (!state.call.active) return;
    state.call.mic = !state.call.mic;
    (state.call.localStream?.getAudioTracks() || []).forEach((t) => {
        t.enabled = state.call.mic;
    });
    setTileState(state.me?.id, { mic: state.call.mic, cam: state.call.cam, screen: state.call.screen });
    updateCallButtons();
    sendCallState();
  }
  
  async function toggleCam() {
    if (!state.call.active) return;
    if (state.call.cam) {
        state.call.cam = false;
        state.call.screen = false;
        const oldTracks = state.call.localStream?.getVideoTracks() || [];
        oldTracks.forEach((t) => {
            t.stop();
            state.call.localStream?.removeTrack(t);
        });
        for (const pc of state.call.peers.values()) {
            const videoSender = getPeerSender(pc, "video");
            if (videoSender) {
                await videoSender.replaceTrack(null);
            }
        }
        await renegotiateAllPeers();
    } else {
        try {
            const camStream = await navigator.mediaDevices.getUserMedia({
                video: getRequestedVideoConstraints(),
            });
            const track = camStream.getVideoTracks()[0];
            if (!track) return;
            const oldTracks = state.call.localStream?.getVideoTracks() || [];
            oldTracks.forEach((t) => {
                t.stop();
                state.call.localStream?.removeTrack(t);
            });
            state.call.localStream?.addTrack(track);
            state.call.cam = true;
            state.call.screen = false;
            for (const pc of state.call.peers.values()) {
                const videoSender = getPeerSender(pc, "video");
                if (videoSender) {
                    await videoSender.replaceTrack(track);
                }
            }
            await renegotiateAllPeers();
        } catch (e) {
            alert("Ошибка доступа к камере: " + e.message);
            return;
        }
    }
    const localCard = state.call.tiles.get(state.me?.id);
    if (localCard) {
        localCard.video.srcObject = new MediaStream(state.call.localStream?.getTracks() || []);
        await safePlay(localCard.video);
    }
    setTileState(state.me?.id, { mic: state.call.mic, cam: state.call.cam, screen: state.call.screen });
    updateCallButtons();
    sendCallState();
  }
  
  // === ИСПРАВЛЕННАЯ ФУНКЦИЯ (БЛОК ДЛЯ МОБИЛЬНЫХ) ===
  async function toggleScreenShare() {
    if (!state.call.active) return;
    
    if (!("getDisplayMedia" in navigator.mediaDevices)) {
        alert("Демонстрация экрана недоступна на мобильных устройствах.");
        return;
    }
    
    if (state.call.screen) {
        state.call.screen = false;
        state.call.cam = false;
        const oldTracks = state.call.localStream?.getVideoTracks() || [];
        oldTracks.forEach((t) => {
            t.stop();
            state.call.localStream?.removeTrack(t);
        });
        for (const pc of state.call.peers.values()) {
            const videoSender = getPeerSender(pc, "video");
            if (videoSender) {
                await videoSender.replaceTrack(null);
            }
        }
        await renegotiateAllPeers();
        const localCard = state.call.tiles.get(state.me?.id);
        if (localCard) {
            localCard.video.srcObject = new MediaStream(state.call.localStream?.getTracks() || []);
            await safePlay(localCard.video);
        }
        setTileState(state.me?.id, { mic: state.call.mic, cam: state.call.cam, screen: state.call.screen });
        updateCallButtons();
        sendCallState();
        return;
    }
    
    try {
        const display = await navigator.mediaDevices.getDisplayMedia({ 
            video: true,
            audio: false  
        });
        const track = display.getVideoTracks()[0];
        if (!track) return;
        
        track.onended = async () => {
            if (state.call.screen) {
                await toggleScreenShare();
            }
        };
        
        const oldTracks = state.call.localStream?.getVideoTracks() || [];
        oldTracks.forEach((t) => {
            t.stop();
            state.call.localStream?.removeTrack(t);
        });
        state.call.localStream?.addTrack(track);
        state.call.cam = true;
        state.call.screen = true;
        
        for (const pc of state.call.peers.values()) {
            const videoSender = getPeerSender(pc, "video");
            if (videoSender) {
                await videoSender.replaceTrack(track);
            }
        }
        await renegotiateAllPeers();
        
        const localCard = state.call.tiles.get(state.me?.id);
        if (localCard) {
            localCard.video.srcObject = new MediaStream(state.call.localStream?.getTracks() || []);
            await safePlay(localCard.video);
        }
        setTileState(state.me?.id, { mic: state.call.mic, cam: state.call.cam, screen: state.call.screen });
        updateCallButtons();
        sendCallState();
    } catch (e) {
        console.error("Screen share error:", e);
        alert("Не удалось начать демонстрацию экрана: " + e.message);
    }
  }

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
    for (const uid of Array.from(state.call.peers.keys())) {
        removePeer(uid);
    }
    const localCard = state.call.tiles.get(state.me?.id);
    const callGrid = qs("callGrid");
    if (callGrid) callGrid.innerHTML = "";
    state.call.tiles.clear();
    if (localCard) {
        const card = ensureCallTile(state.me?.id, true);
        if (card) {
            card.video.srcObject = localCard.video.srcObject || new MediaStream(state.call.localStream?.getTracks() || []);
            card.video.muted = true;
            safePlay(card.video);
            setTileState(state.me?.id, { mic: state.call.mic, cam: state.call.cam, screen: state.call.screen });
        }
    }
  }
  
  async function syncCurrentChatIfOpen() {
    if (!state.currentChatId) return;
    // Если WS подключён, он уже доставляет сообщения — не нужно стирать DOM
    if (state.ws && state.ws.readyState === WebSocket.OPEN) return;
    const prev = qs("messages");
    if (!prev) return;
    const atBottom = prev.scrollHeight - prev.scrollTop - prev.clientHeight < 60;
    try {
        const data = await api(`/api/chats/${state.currentChatId}/messages`);
        // Добавляем только новые сообщения, не стирая старые
        const existingIds = new Set(
            Array.from(prev.querySelectorAll("[data-mid]")).map(el => Number(el.dataset.mid))
        );
        let added = 0;
        data.forEach(m => {
            if (!existingIds.has(m.id)) { appendMessage(m); added++; }
        });
        if (added > 0 && atBottom) prev.scrollTop = prev.scrollHeight;
    } catch (_) {}
  }
  
  function startFallbackSync() {
    if (state.syncTimer) clearInterval(state.syncTimer);
    state.syncTimer = setInterval(async () => {
        try {
            await Promise.all([loadChats(), loadFriends(), loadFriendRequests()]);
            await loadGroupInvites();
            await syncCurrentChatIfOpen();
        } catch (_) {}
    }, 12000);
  }
  
  function connectWs() {
    if (!state.token) return;
    if (state.ws && (state.ws.readyState === WebSocket.OPEN || state.ws.readyState === WebSocket.CONNECTING)) {
        return;
    }
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${proto}://${location.host}/ws?token=${encodeURIComponent(state.token)}`);
    state.ws = ws;
    ws.onopen = async () => {
        state.wsMeta.retry = 0;
        stopWsHeartbeat();
        startWsHeartbeat();
        try {
            await Promise.all([loadChats(), loadFriends(), loadFriendRequests()]);
            await syncCurrentChatIfOpen();
        } catch (_) {}
        if (state.call.active && state.call.chatId) {
            resetCallPeersForRejoin();
            ws.send(JSON.stringify({
                type: "call:join",
                chat_id: state.call.chatId,
                mic: state.call.mic,
                cam: state.call.cam,
                screen: state.call.screen,
            }));
        }
    };
    ws.onmessage = async (ev) => {
        let msg;
        try { msg = JSON.parse(ev.data); } catch(e) { return; }
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
                // Если это чужое сообщение — отмечаем прочитанным
                if (msg.payload.user_id !== state.me?.id) markChatRead(state.currentChatId);
            }
            loadChats();
        }
        if (msg.type === "message:read") {
            const { chat_id, reader_id, up_to_id } = msg.payload;
            if (reader_id !== state.me?.id && chat_id === state.currentChatId) {
                updateReadStatusUpTo(up_to_id);
            }
        }
        if (msg.type === "message:deleted_all") {
            if (msg.payload.chat_id === state.currentChatId) removeMessageById(msg.payload.message_id);
            loadChats();
        }
        if (msg.type === "message:deleted_me") {
            if (msg.payload.chat_id === state.currentChatId) removeMessageById(msg.payload.message_id);
            loadChats();
        }
        if (msg.type === "friend:request" || msg.type === "friend:accepted" || msg.type === "chat:added") {
            refreshSide();
        }
        if (msg.type === "group:invite" || msg.type === "group:invite_answer") {
            loadGroupInvites();
        }
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
            if (msg.payload.user_id === state.me?.id && state.currentChatId === msg.payload.chat_id) {
                state.currentChatId = null;
                state.currentChat = null;
                const msgEl = qs("messages");
                const memEl = qs("chatMembers");
                if (msgEl) msgEl.innerHTML = "";
                if (memEl) memEl.innerHTML = "";
                setChatHeader(null);
                setChatOpen(false);
            }
            if (state.currentChatId === msg.payload.chat_id) {
                loadMembers(msg.payload.chat_id);
            }
            refreshSide();
        }
        if (msg.type === "group:member_role") {
            if (state.currentChatId === msg.payload.chat_id) {
                loadMembers(msg.payload.chat_id);
            }
            loadChats();
        }
        if (msg.type === "user:blocked") {
            refreshSide();
        }
        if (msg.type === "call:participants") {
            try {
                const list = msg.payload.users || [];
                const states = msg.payload.states || {};
                Object.keys(states).forEach((uid) => setTileState(Number(uid), states[uid]));
                for (const uid of list) {
                    // Инициатор - тот у кого ID больше; при равных - новый участник
                    if (Number(state.me?.id) >= Number(uid)) await ensurePeer(Number(uid), true);
                }
            } catch(e) { console.error("call:participants error:", e); }
        }
        if (msg.type === "call:ring") {
            const chat = state.chats.find((c) => c.id === msg.payload.chat_id);
            state.ui.incomingCall = { chatId: msg.payload.chat_id, title: chat?.title || "Входящий звонок" };
            const incomingText = qs("incomingCallText");
            if (incomingText) incomingText.textContent = `Входящий звонок: ${state.ui.incomingCall.title}`;
            show(qs("incomingCallToast"));
        }
        if (msg.type === "call:user_joined") {
            try {
                const uid = Number(msg.payload.user_id);
                setTileState(uid, msg.payload.state || {});
                // Существующий участник создаёт offer новому - если у него ID больше или равен
                if (state.call.active && state.call.chatId === msg.payload.chat_id && Number(state.me?.id) >= uid) {
                    await ensurePeer(uid, true);
                }
            } catch(e) { console.error("call:user_joined error:", e); }
        }
        if (msg.type === "call:user_left") {
            removePeer(msg.payload.user_id);
        }
        if (msg.type === "call:user_state") {
            setTileState(msg.payload.user_id, msg.payload.state || {});
        }
        if (msg.type === "call:signal") {
            await handleSignal(msg.payload.from_user, msg.payload.signal);
        }
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
    await Promise.all([loadChats(), loadFriends(), loadFriendRequests(), loadGroupInvites(), loadSettings()]);
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
  
  function openMembersSheet() {
    const panel = qs("groupMembersPanel");
    const backdrop = qs("membersBackdrop");
    if (!panel) return;
    panel.classList.add("members-modal-open");
    show(panel);
    if (backdrop) {
        show(backdrop);
        // Небольшая задержка чтобы transition сработал
        requestAnimationFrame(() => {
            backdrop.classList.add("open");
            requestAnimationFrame(() => panel.classList.add("sheet-visible"));
        });
    }
    // Закрытие по Escape
    document.addEventListener("keydown", _membersEscHandler);
  }

  function closeMembersSheet() {
    const panel = qs("groupMembersPanel");
    const backdrop = qs("membersBackdrop");
    if (!panel) return;
    panel.classList.remove("sheet-visible");
    if (backdrop) backdrop.classList.remove("open");
    // Ждём окончания transition потом скрываем
    const onEnd = () => {
        panel.classList.remove("members-modal-open");
        hide(panel);
        if (backdrop) hide(backdrop);
        panel.removeEventListener("transitionend", onEnd);
    };
    panel.addEventListener("transitionend", onEnd);
    document.removeEventListener("keydown", _membersEscHandler);
    // fallback если transition не сработал
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


  // ─── Контекстное меню для сообщений ───
  let _ctxMenuEl = null;
  let _ctxLongPressTimer = null;

  function hideContextMenu() {
    if (_ctxMenuEl) { _ctxMenuEl.remove(); _ctxMenuEl = null; }
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
        } catch(e) { alert(e.message); }
    };
    menu.appendChild(btnDelMe);

    if (isMine) {
        const btnDelAll = document.createElement("button");
        btnDelAll.textContent = "🗑️ Удалить у всех";
        btnDelAll.onclick = async () => {
            hideContextMenu();
            if (!confirm("Удалить сообщение у всех?")) return;
            try { await api(`/api/messages/${mid}?mode=all`, { method: "DELETE" }); }
            catch(e) { alert(e.message); }
        };
        menu.appendChild(btnDelAll);
    }

    document.body.appendChild(menu);

    // Не выходить за правый край экрана
    const rect = menu.getBoundingClientRect();
    if (rect.right > window.innerWidth - 8) {
        menu.style.left = `${window.innerWidth - rect.width - 8}px`;
    }
    if (rect.bottom > window.innerHeight - 8) {
        menu.style.top = `${y - rect.height}px`;
    }

    setTimeout(() => document.addEventListener("click", hideContextMenu, { once: true }), 0);
  }

  function bindMessageContextMenu(container) {
    // ПКМ на десктопе
    container.addEventListener("contextmenu", (e) => {
        const msgEl = e.target.closest(".message[data-mid]");
        if (!msgEl) return;
        e.preventDefault();
        showMessageContextMenu(e.clientX, e.clientY, msgEl);
    });
    // Долгое нажатие на мобильных
    container.addEventListener("touchstart", (e) => {
        const msgEl = e.target.closest(".message[data-mid]");
        if (!msgEl) return;
        const touch = e.touches[0];
        _ctxLongPressTimer = setTimeout(() => {
            if (navigator.vibrate) navigator.vibrate(30);
            showMessageContextMenu(touch.clientX, touch.clientY, msgEl);
        }, 550);
    }, { passive: true });
    container.addEventListener("touchend", () => {
        clearTimeout(_ctxLongPressTimer);
    }, { passive: true });
    container.addEventListener("touchmove", () => {
        clearTimeout(_ctxLongPressTimer);
    }, { passive: true });
  }

  // ─── Вставка изображения из буфера обмена ───
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
                const named = new File([file], `paste_${Date.now()}.${ext}`, { type: item.type });
                try { await sendMessage({ file: named, kind: "image" }); }
                catch(err) { console.error("Paste send error:", err); }
                return;
            }
        }
    });
  }

  function bindUi() {
    setMainTab("chats");
    setChatOpen(false);
    setEmptyState(true);
    const gateCode = qs("gateCode");
    if (gateCode) gateCode.value = localStorage.getItem("saved_gate_code") || "";
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
    if (btnMobile) btnMobile.onclick = () => document.body.classList.toggle("menu-open");
    if (gateBtn) gateBtn.onclick = async () => {
        setError("gateError", "");
        try {
            const code = qs("gateCode")?.value || "";
            await api("/api/gate", { method: "POST", body: JSON.stringify({ code }) });
            if (qs("rememberCode")?.checked) localStorage.setItem("saved_gate_code", code);
            hide(qs("gateScreen"));
            show(qs("authScreen"));
        } catch (e) {
            setError("gateError", e.message);
        }
    };
    if (tabLogin) tabLogin.onclick = () => {
        tabLogin.classList.add("active");
        tabRegister?.classList.remove("active");
        show(qs("loginPane"));
        hide(qs("registerPane"));
    };
    if (tabRegister) tabRegister.onclick = () => {
        tabRegister.classList.add("active");
        tabLogin?.classList.remove("active");
        hide(qs("loginPane"));
        show(qs("registerPane"));
    };
    if (loginBtn) loginBtn.onclick = async () => {
        setError("authError", "");
        try {
            const r = await api("/api/login", {
                method: "POST",
                body: JSON.stringify({ 
                    username: qs("loginUsername")?.value || "", 
                    password: qs("loginPassword")?.value || "" 
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
    if (registerBtn) registerBtn.onclick = async () => {
        setError("authError", "");
        try {
            const r = await api("/api/register", {
                method: "POST",
                body: JSON.stringify({ 
                    username: qs("regUsername")?.value || "", 
                    password: qs("regPassword")?.value || "", 
                    nickname: qs("regNickname")?.value || "" 
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
    if (btnLogout) btnLogout.onclick = async () => {
        try {
            await api("/api/logout", { method: "POST", body: "{}" });
        } catch (err) {
            console.error("Logout error:", err);
        }
  
        leaveCall();
  
        if (state.syncTimer) {
            clearInterval(state.syncTimer);
        }
  
        stopWsHeartbeat();
  
        if (state.ws) {
            try {
                state.ws.close();
            } catch (err) {
                console.error("WebSocket close error:", err);
            }
        }
  
        localStorage.removeItem("token");
        state.token = "";
        resetToAuthUi();
    };
    if (chatSearch) chatSearch.oninput = () => renderChatList(chatSearch.value);
    if (sendBtn) sendBtn.onclick = () => sendMessage({ text: messageInput?.value || "" });
    // Вставка из буфера обмена
    if (messageInput) bindClipboardPaste(messageInput);
    // Контекстное меню на сообщениях
    const messagesEl = qs("messages");
    if (messagesEl) bindMessageContextMenu(messagesEl);
    if (messageInput) messageInput.addEventListener("keydown", (e) => {
        if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            sendMessage({ text: messageInput.value || "" });
        }
    });
    if (btnFile) btnFile.onclick = () => fileInput?.click();
    if (btnAssets) btnAssets.onclick = async () => {
        await loadAssets();
        assetsDialog?.showModal();
    };
    if (assetsClose) assetsClose.onclick = () => assetsDialog?.close();
    if (btnUploadAsset) btnUploadAsset.onclick = async () => {
        const f = qs("assetFile")?.files[0];
        if (!f) {
            alert("Выберите файл");
            return;
        }
        const form = new FormData();
        form.append("kind", qs("assetKind")?.value || "emoji");
        form.append("title", qs("assetTitle")?.value || "");
        form.append("file", f);
        await api("/api/assets", { method: "POST", body: form, headers: {} });
        const assetFile = qs("assetFile");
        const assetTitle = qs("assetTitle");
        if (assetFile) assetFile.value = "";
        if (assetTitle) assetTitle.value = "";
        await loadAssets();
    };
    if (fileInput) fileInput.onchange = async () => {
        const f = fileInput.files[0];
        if (!f) return;
        let kind = "file";
        if (f.type.startsWith("image/")) kind = "image";
        if (f.type.startsWith("video/")) kind = "video";
        await sendMessage({ file: f, kind });
        fileInput.value = "";
    };
    if (btnVoice) btnVoice.onclick = () => startVoiceRecord();
    if (btnCircle) btnCircle.onclick = () => startCircleRecord();
    if (btnProfile) btnProfile.onclick = () => {
        const profileNickname = qs("profileNickname");
        const profileAbout = qs("profileAbout");
        if (profileNickname) profileNickname.value = state.me?.nickname || "";
        if (profileAbout) profileAbout.value = state.me?.about || "";
        profileDialog?.showModal();
    };
    if (profileClose) profileClose.onclick = () => profileDialog?.close();
    if (profileSave) profileSave.onclick = async () => {
        state.me = await api("/api/profile", {
            method: "POST",
            body: JSON.stringify({ 
                nickname: qs("profileNickname")?.value || "", 
                about: qs("profileAbout")?.value || "" 
            }),
        });
        const avatar = qs("profileAvatar")?.files[0];
        if (avatar) {
            const form = new FormData();
            form.append("file", avatar);
            state.me = await api("/api/profile/avatar", { method: "POST", body: form, headers: {} });
        }
        renderProfileMini();
        profileDialog?.close();
    };
    if (btnGroup) btnGroup.onclick = () => groupDialog?.showModal();
    if (groupClose) groupClose.onclick = () => groupDialog?.close();
    if (groupCreate) groupCreate.onclick = async () => {
        try {
            const members = (qs("groupMembers")?.value || "")
                .split(",")
                .map((v) => v.trim().replace(/^@/, "").toLowerCase())
                .filter(Boolean);
            const out = await api("/api/groups", {
                method: "POST",
                body: JSON.stringify({ title: qs("groupTitle")?.value || "", members }),
            });
            const groupTitle = qs("groupTitle");
            const groupMembers = qs("groupMembers");
            if (groupTitle) groupTitle.value = "";
            if (groupMembers) groupMembers.value = "";
            groupDialog?.close();
            await loadChats();
            await openChat(out.chat_id);
        } catch (e) {
            alert(e.message);
        }
    };
    if (btnInviteGroup) btnInviteGroup.onclick = async () => {
        if (!state.currentChat || state.currentChat.type !== "group") return;
        const raw = prompt("Введите @username для инвайта");
        const username = String(raw || "").trim();
        if (!username) return;
        try {
            await api(`/api/groups/${state.currentChatId}/invite/username`, {
                method: "POST",
                body: JSON.stringify({ username }),
            });
            alert("Инвайт отправлен");
        } catch (e) {
            alert(e.message);
        }
    };
    if (btnLeaveChat) btnLeaveChat.onclick = async () => {
        if (!state.currentChatId) return;
        const targetName = state.currentChat?.type === "group" ? "эту группу" : "этот чат";
        if (!confirm(`Выйти из ${targetName}?`)) return;
        if (state.call.active && state.call.chatId === state.currentChatId) leaveCall();
        await api(`/api/chats/${state.currentChatId}/leave`, { method: "POST", body: "{}" });
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
    if (btnDeleteGroup) btnDeleteGroup.onclick = async () => {
        if (!state.currentChat || state.currentChat.type !== "group") return;
        if (!confirm("Удалить группу?")) return;
        await api(`/api/groups/${state.currentChatId}`, { method: "DELETE" });
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
    if (btnCopyMyId) btnCopyMyId.onclick = async () => {
        try {
            await navigator.clipboard.writeText(String(state.me?.id || ""));
            alert(`ID скопирован: ${state.me?.id}`);
        } catch (_) {
            prompt("Скопируйте ваш ID", String(state.me?.id || ""));
        }
    };
    if (userSearch) userSearch.oninput = async () => {
        const q = userSearch.value.trim();
        const out = qs("userResults");
        if (!out) return;
        if (q.length < 2) {
            out.innerHTML = "";
            return;
        }
        const users = await api(`/api/users/search?q=${encodeURIComponent(q)}`);
        out.innerHTML = "";
        users.forEach((u) => {
            const el = document.createElement("div");
            el.className = "item";
            el.innerHTML = `
                <div class="item-head">
                    ${avatarMarkup({ avatar: u.avatar, label: u.nickname, seed: `search-${u.id}`, className: "avatar-md" })}
                    <div class="item-copy">
                        <div class="item-title-row">
                            <b>${escapeHtml(u.nickname)}</b>
                            <span class="item-tag">User</span>
                        </div>
                        <small>@${escapeHtml(u.username)} #${u.id}</small>
                        ${u.about ? `<p class="item-preview">${escapeHtml(truncateText(u.about, 90))}</p>` : ""}
                    </div>
                </div>
            `;
            const actions = document.createElement("div");
            actions.className = "actions";
            const add = document.createElement("button");
            add.textContent = "В друзья";
            add.onclick = () => api("/api/friends/request", { method: "POST", body: JSON.stringify({ username: u.username }) });
            const block = document.createElement("button");
            block.className = "danger";
            block.textContent = "Блок";
            block.onclick = async () => {
                await api(`/api/users/${u.id}/block`, { method: "POST", body: "{}" });
                await refreshSide();
            };
            actions.appendChild(add);
            actions.appendChild(block);
            el.appendChild(actions);
            out.appendChild(el);
        });
    };
    if (btnSettings) btnSettings.onclick = async () => {
        await loadSettings();
        await loadBlockedList();
        settingsDialog?.showModal();
    };
    if (settingsClose) settingsClose.onclick = () => settingsDialog?.close();
    if (settingsSave) settingsSave.onclick = async () => {
        state.settings = await api("/api/settings", {
            method: "POST",
            body: JSON.stringify({
                allow_friend_requests: qs("setFriendReq")?.value || "everyone",
                allow_calls_from: qs("setCalls")?.value || "friends",
                allow_group_invites: qs("setInvites")?.value || "friends",
                show_last_seen: qs("setLastSeen")?.value || "friends",
            }),
        });
        settingsDialog?.close();
    };
    if (btnChangePassword) btnChangePassword.onclick = async () => {
        const oldPassword = qs("oldPassword")?.value || "";
        const newPassword = qs("newPassword")?.value || "";
        if (!oldPassword || !newPassword) {
            alert("Введите старый и новый пароль");
            return;
        }
        await api("/api/account/password", {
            method: "POST",
            body: JSON.stringify({ old_password: oldPassword, new_password: newPassword }),
        });
        const oldPass = qs("oldPassword");
        const newPass = qs("newPassword");
        if (oldPass) oldPass.value = "";
        if (newPass) newPass.value = "";
        alert("Пароль успешно изменен");
    };
    if (btnDeleteAccount) btnDeleteAccount.onclick = async () => {
        const ok = confirm("Удалить аккаунт безвозвратно? Это удалит ваши сессии и уберет вас из чатов.");
        if (!ok) return;
        try {
            await api("/api/account", { method: "DELETE" });
        } catch (e) {
            alert(e.message);
            return;
        }
        leaveCall();
        if (state.syncTimer) clearInterval(state.syncTimer);
        stopWsHeartbeat();
        if (state.ws) {
            try {
                state.ws.close();
            } catch (_) {}
        }
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
    if (btnDevices) btnDevices.onclick = async () => {
        if (devicePanel) {
            if (devicePanel.classList.contains("hidden")) {
                await refreshDevicePanel();
                show(devicePanel);
            } else {
                hide(devicePanel);
            }
        }
    };
    if (selMic) selMic.onchange = async () => switchMicDevice(selMic.value);
    if (selCam) selCam.onchange = async () => switchCamDevice(selCam.value);
    if (selSpeaker) selSpeaker.onchange = async () => {
        state.devicePrefs.speakerId = selSpeaker.value || "";
        await applySpeakerToAllTiles();
    };
    if (selAudioMode) selAudioMode.onchange = async () => applyAudioMode(selAudioMode.value);
    if (btnMinimizeCall) btnMinimizeCall.onclick = () => {
        hide(qs("callOverlay"));
        state.ui.callMinimized = true;
        show(qs("btnCallRestore"));
    };
    if (btnCallRestore) btnCallRestore.onclick = () => {
        if (!state.call.active) return;
        show(qs("callOverlay"));
        hide(qs("btnCallRestore"));
        state.ui.callMinimized = false;
    };
    if (btnIncomingAccept) btnIncomingAccept.onclick = async () => {
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
    if (btnIncomingDecline) btnIncomingDecline.onclick = () => {
        hide(incomingCallToast);
        state.ui.incomingCall = null;
    };
  }
  
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

