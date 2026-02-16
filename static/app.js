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
  },
  assets: [],
  ui: {
      currentTab: "chats",
      chatOpen: false,
      callMinimized: false,
      incomingCall: null,
  },
};

// FIXED: Detect mobile platforms for screen sharing limitation
const isLikelyIOS = /iPad|iPhone|iPod/.test(navigator.userAgent) || (navigator.platform === "MacIntel" && navigator.maxTouchPoints > 1);
const isLikelyAndroid = /Android/.test(navigator.userAgent);
const isMobile = isLikelyIOS || isLikelyAndroid;
const isLikelySafari = /^((?!chrome|android|crios|fxios).)*safari/i.test(navigator.userAgent);

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

function formatTime(iso) {
  return new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
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

// FIXED: Proper async play with error handling for autoplay policy
async function safePlay(mediaEl) {
  if (!mediaEl) return;
  try {
      await mediaEl.play();
  } catch (e) {
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

function messageBody(m) {
  const text = escapeHtml(m.text || "");
  if (!m.file_url) return text;
  const fileUrl = withMediaToken(m.file_url);
  if (m.kind === "image" || m.kind === "sticker" || m.kind === "emoji") return `${text}<br><img src="${fileUrl}" />`;
  if (m.kind === "video" || m.kind === "circle") return `${text}<br><video src="${fileUrl}" controls playsinline webkit-playsinline></video>`;
  if (m.kind === "voice") return `${text}<br><audio src="${fileUrl}" controls></audio>`;
  return `${text}<br><a href="${fileUrl}" target="_blank">${escapeHtml(m.file_name || "Файл")}</a>`;
}

function appendMessage(m) {
  const item = document.createElement("div");
  item.dataset.mid = String(m.id);
  item.className = `message ${m.user_id === state.me?.id ? "mine" : ""}`;
  item.innerHTML = `<div class="meta">${escapeHtml(m.nickname)} @${escapeHtml(m.username)} ${formatTime(m.created_at)}</div><div>${messageBody(m)}</div>`;
  const actions = document.createElement("div");
  actions.className = "message-actions";
  const delMe = document.createElement("button");
  delMe.className = "ghost";
  delMe.textContent = "Удалить у себя";
  delMe.onclick = async () => {
      try {
          await api(`/api/messages/${m.id}?mode=me`, { method: "DELETE" });
          item.remove();
      } catch (e) {
          alert(e.message);
      }
  };
  actions.appendChild(delMe);
  if (m.user_id === state.me?.id) {
      const delAll = document.createElement("button");
      delAll.className = "danger";
      delAll.textContent = "Удалить у всех";
      delAll.onclick = async () => {
          if (!confirm("Удалить сообщение у всех?")) return;
          try {
              await api(`/api/messages/${m.id}?mode=all`, { method: "DELETE" });
          } catch (e) {
              alert(e.message);
          }
      };
      actions.appendChild(delAll);
  }
  item.appendChild(actions);
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
  if (el) el.innerHTML = `${escapeHtml(state.me.nickname)} <span class="small">@${escapeHtml(state.me.username)} #${state.me.id}</span>`;
}

function renderChatList(filter = "") {
  const q = filter.trim().toLowerCase();
  const list = qs("chatList");
  if (!list) return;
  list.innerHTML = "";
  state.chats
      .filter((c) => !q || (c.title || "").toLowerCase().includes(q))
      .forEach((chat) => {
          const el = document.createElement("div");
          el.className = "item";
          el.innerHTML = `<b>${escapeHtml(chat.title || "Без названия")}</b><small>${escapeHtml(chat.last_text || "")}</small>`;
          el.onclick = () => openChat(chat.id);
          list.appendChild(el);
      });
}

function setChatHeader(chat) {
  const title = chat ? escapeHtml(chat.title || "Чат") : "Выберите чат";
  const titleEl = qs("chatTitle");
  if (titleEl) titleEl.innerHTML = title;
  const canCall = !!chat && !!chat.can_call;
  const group = !!chat && chat.type === "group";
  const canDelete = !!chat && chat.type === "group" && !!chat.can_delete;
  const stack = document.querySelector(".chat-stack");
  const btnCall = qs("btnCallStart");
  const btnInvite = qs("btnInviteGroup");
  const btnDelete = qs("btnDeleteGroup");
  const membersPanel = qs("groupMembersPanel");
  if (canCall) show(btnCall); else hide(btnCall);
  if (group) show(btnInvite); else hide(btnInvite);
  if (canDelete) show(btnDelete); else hide(btnDelete);
  if (group) show(membersPanel); else hide(membersPanel);
  if (stack) stack.classList.toggle("has-members", group);
  
  // FIXED: Hide screen share button on mobile (iOS/Android don't support getDisplayMedia)
  const btnScreen = qs("btnShareScreen");
  if (btnScreen) {
      if (isMobile || !('getDisplayMedia' in navigator.mediaDevices)) {
          hide(btnScreen);
          btnScreen.title = "Демонстрация экрана не поддерживается на мобильных устройствах";
      } else {
          show(btnScreen);
      }
  }
}

async function loadMembers(chatId) {
  if (!chatId) return;
  const members = await api(`/api/chats/${chatId}/members`);
  state.membersById = new Map(members.map((m) => [m.id, m]));
  const wrap = qs("chatMembers");
  if (!wrap) return;
  wrap.innerHTML = "";
  members.forEach((m) => {
      const el = document.createElement("div");
      el.className = "item";
      el.innerHTML = `<b>${escapeHtml(m.nickname)}</b><small>@${escapeHtml(m.username)} (${m.role}) #${m.id}</small>`;
      const actions = document.createElement("div");
      actions.className = "actions";
      const dm = document.createElement("button");
      dm.textContent = "ЛС";
      dm.onclick = async () => {
          if (m.id === state.me.id) return;
          const out = await api("/api/chats/direct", { method: "POST", body: JSON.stringify({ user_id: m.id }) });
          await loadChats();
          await openChat(out.chat_id);
      };
      const call = document.createElement("button");
      call.textContent = "Позвонить";
      call.onclick = async () => {
          if (m.id === state.me.id) return;
          const out = await api("/api/chats/direct", { method: "POST", body: JSON.stringify({ user_id: m.id }) });
          await loadChats();
          await openChat(out.chat_id);
          await startCall();
      };
      actions.appendChild(dm);
      actions.appendChild(call);
      el.appendChild(actions);
      wrap.appendChild(el);
  });
}

async function openChat(chatId) {
  const chat = state.chats.find((c) => c.id === chatId) || null;
  state.currentChat = chat;
  state.currentChatId = chatId;
  setChatHeader(chat);
  setChatOpen(true);
  if (window.innerWidth <= 820) document.body.classList.remove("menu-open");
  const messages = qs("messages");
  if (messages) messages.innerHTML = "";
  const data = await api(`/api/chats/${chatId}/messages`);
  data.forEach(appendMessage);
  await loadMembers(chatId);
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
      el.innerHTML = `<b>${escapeHtml(f.nickname)}</b><small>@${escapeHtml(f.username)} #${f.id}</small>`;
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
      el.innerHTML = `<b>${escapeHtml(r.nickname)}</b><small>@${escapeHtml(r.username)}</small>`;
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
      el.innerHTML = `<b>${escapeHtml(r.chat_title || "Группа")}</b><small>от ${escapeHtml(r.inviter_nickname)} @${escapeHtml(r.inviter_username)}</small>`;
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
      el.innerHTML = `<b>${escapeHtml(u.nickname)}</b><small>@${escapeHtml(u.username)} #${u.id}</small>`;
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
      el.className = "item";
      el.innerHTML = `<b>${escapeHtml(a.title || a.kind)}</b><small>${a.kind}</small><br><img src="${withMediaToken(a.file_url)}" style="max-width:72px;max-height:72px;border-radius:8px;" />`;
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
  if (btnMic) btnMic.textContent = `Микрофон: ${state.call.mic ? "вкл" : "выкл"}`;
  if (btnCam) btnCam.textContent = `Камера: ${state.call.cam ? "вкл" : "выкл"}`;
  if (btnScreen) btnScreen.textContent = `Демонстрация: ${state.call.screen ? "вкл" : "выкл"}`;
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

function ensureCallTile(userId, isLocal = false) {
  if (state.call.tiles.has(userId)) return state.call.tiles.get(userId);
  const wrap = qs("callGrid");
  if (!wrap) return null;
  const tile = document.createElement("div");
  tile.className = "call-tile";
  tile.dataset.uid = userId;
  const video = document.createElement("video");
  video.autoplay = true;
  video.playsInline = true;
  video.setAttribute("playsinline", "");
  video.setAttribute("webkit-playsinline", "");
  video.muted = isLocal;
  const who = document.createElement("div");
  who.className = "who";
  const left = document.createElement("span");
  left.textContent = isLocal ? "Вы" : peerNameById(userId);
  const right = document.createElement("span");
  right.textContent = "mic:on cam:off";
  who.appendChild(left);
  who.appendChild(right);
  const vol = document.createElement("input");
  vol.type = "range";
  vol.min = "0";
  vol.max = "1";
  vol.step = "0.05";
  vol.value = "1";
  vol.disabled = isLocal;
  if (!isLocal) vol.classList.add("hidden");
  vol.oninput = () => {
      video.volume = Number(vol.value);
  };
  const volBtn = document.createElement("button");
  volBtn.className = "ghost";
  volBtn.textContent = "🔊";
  volBtn.style.width = "auto";
  volBtn.style.marginTop = "6px";
  volBtn.onclick = () => {
      if (isLocal) return;
      vol.classList.toggle("hidden");
  };
  tile.appendChild(video);
  tile.appendChild(who);
  tile.appendChild(volBtn);
  tile.appendChild(vol);
  wrap.appendChild(tile);
  const card = { tile, video, right, vol };
  state.call.tiles.set(userId, card);
  return card;
}

function setTileState(userId, statePayload = {}) {
  const card = ensureCallTile(userId, userId === state.me?.id);
  if (!card) return;
  const mic = statePayload.mic ? "on" : "off";
  const cam = statePayload.cam ? (statePayload.screen ? "screen" : "on") : "off";
  card.right.textContent = `mic:${mic} cam:${cam}`;
}

function removePeer(userId) {
  const pc = state.call.peers.get(userId);
  if (pc) pc.close();
  state.call.peers.delete(userId);
  const card = state.call.tiles.get(userId);
  if (card) card.tile.remove();
  state.call.tiles.delete(userId);
}

function attachStreamToPeer(userId, stream) {
  const card = ensureCallTile(userId, false);
  if (!card || !card.video) return;
  card.video.srcObject = stream;
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
          const senders = pc.getSenders();
          const audioSender = senders.find((s) => s.track && s.track.kind === "audio");
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
          video: state.devicePrefs.camId ? { deviceId: { exact: state.devicePrefs.camId } } : true,
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
          const senders = pc.getSenders();
          const videoSender = senders.find((s) => s.track && s.track.kind === "video");
          if (videoSender) {
              await videoSender.replaceTrack(track);
          }
      }
      const localCard = state.call.tiles.get(state.me?.id);
      if (localCard) {
          localCard.video.srcObject = new MediaStream(state.call.localStream.getTracks());
          await safePlay(localCard.video);
      }
  } catch (e) {
      console.error("Failed to switch cam:", e);
  }
}

async function applyAudioMode(mode) {
  state.devicePrefs.audioMode = mode || "speaker";
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

// FIXED: WebRTC peer connection with proper bidirectional setup for group calls
async function ensurePeer(userId, createOffer) {
  if (state.call.peers.has(userId)) return state.call.peers.get(userId);
  
  // Get ICE servers from API
  let iceServers = [
      { urls: "stun:stun.l.google.com:19302" },
      { urls: "stun:stun1.l.google.com:19302" }
  ];
  try {
      const config = await api("/api/rtc-config");
      if (config.ice_servers && config.ice_servers.length > 0) {
          iceServers = config.ice_servers;
      }
  } catch (_) {}
  
  const pc = new RTCPeerConnection({ iceServers });
  
  // Add all tracks from local stream to peer connection
  if (state.call.localStream) {
      state.call.localStream.getTracks().forEach((track) => {
          pc.addTrack(track, state.call.localStream);
      });
  }
  
  pc.onicecandidate = (ev) => {
      if (!ev.candidate) return;
      state.ws.send(JSON.stringify({
          type: "call:signal",
          chat_id: state.call.chatId,
          to_user: userId,
          signal: { type: "candidate", candidate: ev.candidate },
      }));
  };
  
  pc.ontrack = (ev) => {
      const stream = ev.streams[0];
      if (stream) {
          attachStreamToPeer(userId, stream);
      }
  };
  
  pc.onconnectionstatechange = () => {
      console.log(`Peer ${userId} connection state:`, pc.connectionState);
  };
  
  state.call.peers.set(userId, pc);
  
  if (createOffer) {
      try {
          const offer = await pc.createOffer();
          await pc.setLocalDescription(offer);
          state.ws.send(JSON.stringify({
              type: "call:signal",
              chat_id: state.call.chatId,
              to_user: userId,
              signal: { type: "offer", sdp: pc.localDescription },
          }));
      } catch (e) {
          console.error("Failed to create offer:", e);
      }
  }
  return pc;
}

async function handleSignal(fromUser, signal) {
  if (!state.call.active) return;
  const pc = await ensurePeer(fromUser, false);
  try {
      if (signal.type === "offer") {
          await pc.setRemoteDescription(new RTCSessionDescription(signal.sdp));
          const answer = await pc.createAnswer();
          await pc.setLocalDescription(answer);
          state.ws.send(JSON.stringify({
              type: "call:signal",
              chat_id: state.call.chatId,
              to_user: fromUser,
              signal: { type: "answer", sdp: pc.localDescription },
          }));
      }
      if (signal.type === "answer") {
          await pc.setRemoteDescription(new RTCSessionDescription(signal.sdp));
      }
      if (signal.type === "candidate") {
          await pc.addIceCandidate(new RTCIceCandidate(signal.candidate));
      }
  } catch (e) {
      console.error("Signal handling error:", e);
  }
}

// FIXED: Start call with proper user gesture handling for audio autoplay
async function startCall() {
  if (!state.currentChat) return;
  if (state.call.active) return;
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN) {
      connectWs();
      alert("Соединение восстанавливается. Попробуйте начать звонок через 1-2 секунды.");
      return;
  }
  
  state.call.active = true;
  state.call.chatId = state.currentChatId;
  state.call.startedAt = Date.now();
  state.call.mic = true;
  state.call.cam = false;
  state.call.screen = false;
  state.call.peers.clear();
  state.call.tiles.clear();
  
  const callGrid = qs("callGrid");
  if (callGrid) callGrid.innerHTML = "";
  
  state.call.localStream = new MediaStream();
  await createLocalAudioIfMissing();
  
  const localCard = ensureCallTile(state.me?.id, true);
  if (localCard) {
      localCard.video.srcObject = new MediaStream(state.call.localStream.getTracks());
      await safePlay(localCard.video);
  }
  
  setTileState(state.me?.id, { mic: state.call.mic, cam: state.call.cam, screen: state.call.screen });
  await applyAudioMode(state.devicePrefs.audioMode || "speaker");
  
  const callTitle = qs("callTitle");
  if (callTitle && state.currentChat) callTitle.textContent = `Звонок: ${state.currentChat.title}`;
  
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
  state.call.screen = false;
  state.call.cam = false;
  state.call.mic = true;
  stopCallTimer();
  hide(qs("callOverlay"));
  hide(qs("btnCallRestore"));
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
          const senders = pc.getSenders();
          const videoSender = senders.find((s) => s.track && s.track.kind === "video");
          if (videoSender) {
              await videoSender.replaceTrack(null);
          }
      }
  } else {
      try {
          const camStream = await navigator.mediaDevices.getUserMedia({
              video: state.devicePrefs.camId ? { deviceId: { exact: state.devicePrefs.camId } } : true,
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
              const senders = pc.getSenders();
              const videoSender = senders.find((s) => s.track && s.track.kind === "video");
              if (videoSender) {
                  await videoSender.replaceTrack(track);
              } else {
                  pc.addTrack(track, state.call.localStream);
              }
          }
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

// FIXED: Screen share with mobile detection and proper error handling
async function toggleScreenShare() {
  if (!state.call.active) return;
  
  // FIXED: Check if getDisplayMedia is supported (not available on iOS/Android)
  if (!('getDisplayMedia' in navigator.mediaDevices)) {
      alert("Демонстрация экрана не поддерживается на вашем устройстве. Эта функция доступна только на десктопных браузерах (Chrome, Edge, Safari 17+).");
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
          const senders = pc.getSenders();
          const videoSender = senders.find((s) => s.track && s.track.kind === "video");
          if (videoSender) {
              await videoSender.replaceTrack(null);
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
      return;
  }
  
  try {
      // FIXED: Request screen share without audio (audio not reliably supported)
      const display = await navigator.mediaDevices.getDisplayMedia({ 
          video: true,
          audio: false  // Audio from screen share is unreliable across browsers
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
          const senders = pc.getSenders();
          const videoSender = senders.find((s) => s.track && s.track.kind === "video");
          if (videoSender) {
              await videoSender.replaceTrack(track);
          } else {
              pc.addTrack(track, state.call.localStream);
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
          safePlay(card.video);
          setTileState(state.me?.id, { mic: state.call.mic, cam: state.call.cam, screen: state.call.screen });
      }
  }
}

async function syncCurrentChatIfOpen() {
  if (!state.currentChatId) return;
  const prev = qs("messages");
  if (!prev) return;
  const atBottom = prev.scrollHeight - prev.scrollTop - prev.clientHeight < 60;
  const data = await api(`/api/chats/${state.currentChatId}/messages`);
  prev.innerHTML = "";
  data.forEach(appendMessage);
  if (atBottom) prev.scrollTop = prev.scrollHeight;
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
      const msg = JSON.parse(ev.data);
      if (msg.type === "pong") {
          if (state.wsMeta.pongTimer) {
              clearTimeout(state.wsMeta.pongTimer);
              state.wsMeta.pongTimer = null;
          }
          return;
      }
      if (msg.type === "message:new") {
          if (msg.payload.chat_id === state.currentChatId) appendMessage(msg.payload);
          loadChats();
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
      if (msg.type === "user:blocked") {
          refreshSide();
      }
      if (msg.type === "call:participants") {
          const list = msg.payload.users || [];
          const states = msg.payload.states || {};
          Object.keys(states).forEach((uid) => setTileState(Number(uid), states[uid]));
          for (const uid of list) {
              if (state.me?.id > uid) await ensurePeer(uid, true);
          }
      }
      if (msg.type === "call:ring") {
          const chat = state.chats.find((c) => c.id === msg.payload.chat_id);
          state.ui.incomingCall = { chatId: msg.payload.chat_id, title: chat?.title || "Входящий звонок" };
          const incomingText = qs("incomingCallText");
          if (incomingText) incomingText.textContent = `Входящий звонок: ${state.ui.incomingCall.title}`;
          show(qs("incomingCallToast"));
      }
      if (msg.type === "call:user_joined") {
          const uid = msg.payload.user_id;
          setTileState(uid, msg.payload.state || {});
          if (state.call.active && state.call.chatId === msg.payload.chat_id && state.me?.id > uid) {
              await ensurePeer(uid, true);
          }
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

function bindUi() {
  setMainTab("chats");
  setChatOpen(false);
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
  const btnLeaveCall = qs("btnLeaveCall");
  const btnToggleMic = qs("btnToggleMic");
  const btnToggleCam = qs("btnToggleCam");
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
  
      }
      localStorage.removeItem("token");
      location.reload();
  };
  if (chatSearch) chatSearch.oninput = () => renderChatList(chatSearch.value);
  if (sendBtn) sendBtn.onclick = () => sendMessage({ text: messageInput?.value || "" });
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
      const members = (qs("groupMembers")?.value || "").split(",").map((v) => parseInt(v.trim(), 10)).filter((n) => Number.isInteger(n));
      const out = await api("/api/groups", {
          method: "POST",
          body: JSON.stringify({ title: qs("groupTitle")?.value || "", members }),
      });
      groupDialog?.close();
      await loadChats();
      await openChat(out.chat_id);
  };
  if (btnInviteGroup) btnInviteGroup.onclick = async () => {
      if (!state.currentChat || state.currentChat.type !== "group") return;
      const raw = prompt("Введите @username для инвайта");
      const username = String(raw || "").trim();
      if (!username) return;
      await api(`/api/groups/${state.currentChatId}/invite/username`, {
          method: "POST",
          body: JSON.stringify({ username }),
      });
      alert("Инвайт отправлен");
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
          el.innerHTML = `<b>${escapeHtml(u.nickname)}</b><small>@${escapeHtml(u.username)} #${u.id}</small>`;
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
      location.reload();
  };
  if (btnCallStart) btnCallStart.onclick = startCall;
  if (btnLeaveCall) btnLeaveCall.onclick = leaveCall;
  if (btnToggleMic) btnToggleMic.onclick = toggleMic;
  if (btnToggleCam) btnToggleCam.onclick = toggleCam;
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

async function boot() {
  bindUi();
  const gate = await api("/api/gate/status");
  const sessionOk = await ensureSession();
  if (sessionOk) {
      await onAuthorized();
      return;
  }
  hide(qs("app"));
  if (gate.ok) {
      hide(qs("gateScreen"));
      show(qs("authScreen"));
  } else {
      show(qs("gateScreen"));
      hide(qs("authScreen"));
  }
}

boot();