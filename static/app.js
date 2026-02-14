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
};

function qs(id) {
  return document.getElementById(id);
}

function show(el) {
  el.classList.remove("hidden");
}

function hide(el) {
  el.classList.add("hidden");
}

function escapeHtml(v) {
  return (v || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;");
}

function setError(id, text) {
  qs(id).textContent = text || "";
}

function formatTime(iso) {
  return new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function fmtDuration(sec) {
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

const isLikelyIOS = /iPad|iPhone|iPod/.test(navigator.userAgent) || (navigator.platform === "MacIntel" && navigator.maxTouchPoints > 1);
const isLikelySafari = /^((?!chrome|android|crios|fxios).)*safari/i.test(navigator.userAgent);

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
  try {
    await mediaEl.play();
  } catch (_) {}
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

function messageBody(m) {
  const text = escapeHtml(m.text || "");
  if (!m.file_url) return text;
  if (m.kind === "image" || m.kind === "sticker" || m.kind === "emoji") return `${text}<br><img src="${m.file_url}" />`;
  if (m.kind === "video" || m.kind === "circle") return `${text}<br><video src="${m.file_url}" controls playsinline></video>`;
  if (m.kind === "voice") return `${text}<br><audio src="${m.file_url}" controls></audio>`;
  return `${text}<br><a href="${m.file_url}" target="_blank">${escapeHtml(m.file_name || "Файл")}</a>`;
}

function appendMessage(m) {
  const item = document.createElement("div");
  item.dataset.mid = String(m.id);
  item.className = `message ${m.user_id === state.me?.id ? "mine" : ""}`;
  item.innerHTML = `
    <div class="meta">${escapeHtml(m.nickname)} @${escapeHtml(m.username)} ${formatTime(m.created_at)}</div>
    <div>${messageBody(m)}</div>
  `;

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
  messages.appendChild(item);
  messages.scrollTop = messages.scrollHeight;
}

function removeMessageById(messageId) {
  const target = qs("messages").querySelector(`[data-mid="${messageId}"]`);
  if (target) target.remove();
}

function renderProfileMini() {
  if (!state.me) return;
  qs("profileMini").innerHTML = `${escapeHtml(state.me.nickname)} <span class="small">@${escapeHtml(state.me.username)} #${state.me.id}</span>`;
}

function renderChatList(filter = "") {
  const q = filter.trim().toLowerCase();
  const list = qs("chatList");
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
  qs("chatTitle").innerHTML = title;
  const canCall = !!chat && !!chat.can_call;
  const group = !!chat && chat.type === "group";
  const canDelete = !!chat && chat.type === "group" && !!chat.can_delete;

  if (canCall) show(qs("btnCallStart")); else hide(qs("btnCallStart"));
  if (group) show(qs("btnInviteGroup")); else hide(qs("btnInviteGroup"));
  if (canDelete) show(qs("btnDeleteGroup")); else hide(qs("btnDeleteGroup"));
}

async function loadMembers(chatId) {
  if (!chatId) return;
  const members = await api(`/api/chats/${chatId}/members`);
  state.membersById = new Map(members.map((m) => [m.id, m]));

  const wrap = qs("chatMembers");
  wrap.innerHTML = "";
  members.forEach((m) => {
    const el = document.createElement("div");
    el.className = "item";
    el.innerHTML = `<b>${escapeHtml(m.nickname)}</b><small>@${escapeHtml(m.username)} (${m.role}) #${m.id}</small>`;
    wrap.appendChild(el);
  });
}

async function openChat(chatId) {
  const chat = state.chats.find((c) => c.id === chatId) || null;
  state.currentChat = chat;
  state.currentChatId = chatId;
  setChatHeader(chat);

  const messages = qs("messages");
  messages.innerHTML = "";
  const data = await api(`/api/chats/${chatId}/messages`);
  data.forEach(appendMessage);
  await loadMembers(chatId);
}

async function loadChats() {
  state.chats = await api("/api/chats");
  renderChatList(qs("chatSearch").value);
  if (state.currentChatId) {
    const still = state.chats.find((c) => c.id === state.currentChatId);
    if (!still) {
      state.currentChat = null;
      state.currentChatId = null;
      setChatHeader(null);
      qs("messages").innerHTML = "";
      qs("chatMembers").innerHTML = "";
    } else {
      state.currentChat = still;
      setChatHeader(still);
    }
  }
}

async function loadFriends() {
  state.friends = await api("/api/friends");
  const list = qs("friendsList");
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

async function loadBlockedList() {
  const rows = await api("/api/blocks");
  const list = qs("blockedList");
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
  await Promise.all([loadFriends(), loadFriendRequests(), loadChats()]);
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
  qs("messageInput").value = "";
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
  list.innerHTML = "";
  state.assets.forEach((a) => {
    const el = document.createElement("div");
    el.className = "item";
    el.innerHTML = `<b>${escapeHtml(a.title || a.kind)}</b><small>${a.kind}</small><br><img src="${a.file_url}" style="max-width:72px;max-height:72px;border-radius:8px;" />`;
    const actions = document.createElement("div");
    actions.className = "actions";
    const send = document.createElement("button");
    send.textContent = "Отправить";
    send.onclick = async () => {
      await sendAssetMessage(a.id);
      qs("assetsDialog").close();
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
  const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
  const chunks = [];
  const mimeType = pickRecorderMime("audio");
  const rec = mimeType ? new MediaRecorder(stream, { mimeType }) : new MediaRecorder(stream);
  state.mediaRecorder = rec;
  qs("btnVoice").textContent = "Стоп";

  rec.ondataavailable = (e) => chunks.push(e.data);
  rec.onstop = async () => {
    const outMime = rec.mimeType || mimeType || "audio/webm";
    const ext = extForMime(outMime, "webm");
    const blob = new Blob(chunks, { type: outMime });
    const file = new File([blob], `voice_${Date.now()}.${ext}`, { type: outMime });
    await sendMessage({ file, kind: "voice" });
    stream.getTracks().forEach((t) => t.stop());
    state.mediaRecorder = null;
    qs("btnVoice").textContent = "Голос";
  };
  rec.start();
}

async function startCircleRecord() {
  if (state.circleRecorder) {
    state.circleRecorder.stop();
    return;
  }
  const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
  const chunks = [];
  const mimeType = pickRecorderMime("video");
  const rec = mimeType ? new MediaRecorder(stream, { mimeType }) : new MediaRecorder(stream);
  state.circleRecorder = rec;
  qs("btnCircle").textContent = "Стоп";

  rec.ondataavailable = (e) => chunks.push(e.data);
  rec.onstop = async () => {
    const outMime = rec.mimeType || mimeType || "video/webm";
    const ext = extForMime(outMime, "webm");
    const blob = new Blob(chunks, { type: outMime });
    const file = new File([blob], `circle_${Date.now()}.${ext}`, { type: outMime });
    await sendMessage({ file, kind: "circle" });
    stream.getTracks().forEach((t) => t.stop());
    state.circleRecorder = null;
    qs("btnCircle").textContent = "Кружок";
  };
  rec.start();
}

function updateCallButtons() {
  qs("btnToggleMic").textContent = `Микрофон: ${state.call.mic ? "вкл" : "выкл"}`;
  qs("btnToggleCam").textContent = `Камера: ${state.call.cam ? "вкл" : "выкл"}`;
  qs("btnShareScreen").textContent = `Демонстрация: ${state.call.screen ? "вкл" : "выкл"}`;
}

function updateCallTimer() {
  if (!state.call.startedAt) {
    qs("callTimer").textContent = "00:00";
    return;
  }
  const sec = Math.max(0, Math.floor((Date.now() - state.call.startedAt) / 1000));
  qs("callTimer").textContent = fmtDuration(sec);
}

function sendCallState() {
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN || !state.call.active) return;
  state.ws.send(
    JSON.stringify({
      type: "call:state",
      chat_id: state.call.chatId,
      mic: state.call.mic,
      cam: state.call.cam,
      screen: state.call.screen,
    })
  );
}

function ensureCallTile(userId, isLocal = false) {
  if (state.call.tiles.has(userId)) return state.call.tiles.get(userId);
  const wrap = qs("callGrid");

  const tile = document.createElement("div");
  tile.className = "call-tile";
  tile.dataset.uid = userId;

  const video = document.createElement("video");
  video.autoplay = true;
  video.playsInline = true;
  video.setAttribute("playsinline", "");
  video.setAttribute("webkit-playsinline", "");
  video.muted = isLocal; // Локальное видео всегда muted

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

  vol.oninput = () => {
    video.volume = Number(vol.value);
  };

  tile.appendChild(video);
  tile.appendChild(who);
  tile.appendChild(vol);
  wrap.appendChild(tile);

  const card = { tile, video, right, vol };
  state.call.tiles.set(userId, card);
  return card;
}

function setTileState(userId, statePayload = {}) {
  const card = ensureCallTile(userId, userId === state.me.id);
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

  const micStream = await navigator.mediaDevices.getUserMedia({
    audio: state.devicePrefs.micId ? { deviceId: { exact: state.devicePrefs.micId } } : true,
    video: false,
  });
  const track = micStream.getAudioTracks()[0];
  state.call.localStream.addTrack(track);
  track.enabled = state.call.mic;
}

function localVideoSender(pc) {
  return pc.getSenders().find((s) => s.track && s.track.kind === "video");
}

function localAudioSender(pc) {
  return pc.getSenders().find((s) => s.track && s.track.kind === "audio");
}

async function listMediaDevices() {
  const devices = await navigator.mediaDevices.enumerateDevices();
  const mics = devices.filter((d) => d.kind === "audioinput");
  const cams = devices.filter((d) => d.kind === "videoinput");
  const speakers = devices.filter((d) => d.kind === "audiooutput");
  return { mics, cams, speakers };
}

function fillSelect(selectEl, devices, selectedId, fallbackLabel) {
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
  if (!("setSinkId" in HTMLMediaElement.prototype)) {
    speakerSelect.disabled = true;
    speakerSelect.title = "Safari/iOS ограничивает выбор аудиовыхода в браузере";
  } else {
    speakerSelect.disabled = false;
    speakerSelect.title = "";
  }
  qs("selAudioMode").value = state.devicePrefs.audioMode || "speaker";
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
    const sender = localAudioSender(pc);
    if (sender) await sender.replaceTrack(track);
    else pc.addTrack(track, state.call.localStream);
  }
}

async function switchCamDevice(deviceId) {
  state.devicePrefs.camId = deviceId || "";
  if (!state.call.active || !state.call.cam) return;

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
    const sender = localVideoSender(pc);
    if (sender) await sender.replaceTrack(track);
    else pc.addTrack(track, state.call.localStream);
  }
  
  // ИСПРАВЛЕНИЕ: Обновляем локальное видео
  const localCard = state.call.tiles.get(state.me.id);
  if (localCard) {
    localCard.video.srcObject = new MediaStream(state.call.localStream.getTracks());
    await safePlay(localCard.video);
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

async function ensurePeer(userId, createOffer) {
  if (state.call.peers.has(userId)) return state.call.peers.get(userId);

  const pc = new RTCPeerConnection({
    iceServers: [{ urls: "stun:stun.l.google.com:19302" }],
  });

  // ИСПРАВЛЕНИЕ: Добавляем все треки из localStream
  if (state.call.localStream) {
    state.call.localStream.getTracks().forEach((track) => {
      pc.addTrack(track, state.call.localStream);
    });
  }

  pc.onicecandidate = (ev) => {
    if (!ev.candidate) return;
    state.ws.send(
      JSON.stringify({
        type: "call:signal",
        chat_id: state.call.chatId,
        to_user: userId,
        signal: { type: "candidate", candidate: ev.candidate },
      })
    );
  };

  pc.ontrack = (ev) => {
    const stream = ev.streams[0];
    if (stream) attachStreamToPeer(userId, stream);
  };

  state.call.peers.set(userId, pc);

  if (createOffer) {
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    state.ws.send(
      JSON.stringify({
        type: "call:signal",
        chat_id: state.call.chatId,
        to_user: userId,
        signal: { type: "offer", sdp: pc.localDescription },
      })
    );
  }

  return pc;
}

async function handleSignal(fromUser, signal) {
  if (!state.call.active) return;
  const pc = await ensurePeer(fromUser, false);

  if (signal.type === "offer") {
    await pc.setRemoteDescription(new RTCSessionDescription(signal.sdp));
    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
    state.ws.send(
      JSON.stringify({
        type: "call:signal",
        chat_id: state.call.chatId,
        to_user: fromUser,
        signal: { type: "answer", sdp: pc.localDescription },
      })
    );
  }

  if (signal.type === "answer") {
    await pc.setRemoteDescription(new RTCSessionDescription(signal.sdp));
  }

  if (signal.type === "candidate") {
    await pc.addIceCandidate(new RTCIceCandidate(signal.candidate));
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

  state.call.active = true;
  state.call.chatId = state.currentChatId;
  state.call.startedAt = Date.now();
  state.call.mic = true;
  state.call.cam = false;
  state.call.screen = false;
  state.call.peers.clear();
  state.call.tiles.clear();
  qs("callGrid").innerHTML = "";

  // ИСПРАВЛЕНИЕ: Создаем локальный стрим правильно
  state.call.localStream = new MediaStream();
  await createLocalAudioIfMissing();

  const localCard = ensureCallTile(state.me.id, true);
  localCard.video.srcObject = new MediaStream(state.call.localStream.getTracks());
  await safePlay(localCard.video);
  setTileState(state.me.id, { mic: state.call.mic, cam: state.call.cam, screen: state.call.screen });
  await applyAudioMode(state.devicePrefs.audioMode || "speaker");

  qs("callTitle").textContent = `Звонок: ${state.currentChat.title}`;
  show(qs("callOverlay"));
  updateCallButtons();
  updateCallTimer();
  state.call.timer = setInterval(updateCallTimer, 1000);

  state.ws.send(
    JSON.stringify({
      type: "call:join",
      chat_id: state.call.chatId,
      mic: state.call.mic,
      cam: state.call.cam,
      screen: state.call.screen,
    })
  );
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
  qs("callGrid").innerHTML = "";
  updateCallButtons();
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
  (state.call.localStream.getAudioTracks() || []).forEach((t) => {
    t.enabled = state.call.mic;
  });
  setTileState(state.me.id, { mic: state.call.mic, cam: state.call.cam, screen: state.call.screen });
  updateCallButtons();
  sendCallState();
}

async function toggleCam() {
  if (!state.call.active) return;

  if (state.call.cam) {
    // Выключаем камеру
    state.call.cam = false;
    state.call.screen = false;
    state.call.localStream.getVideoTracks().forEach((t) => {
      t.stop();
      state.call.localStream.removeTrack(t);
    });
    for (const pc of state.call.peers.values()) {
      const sender = localVideoSender(pc);
      if (sender) await sender.replaceTrack(null);
    }
  } else {
    // Включаем камеру
    const camStream = await navigator.mediaDevices.getUserMedia({
      video: state.devicePrefs.camId ? { deviceId: { exact: state.devicePrefs.camId } } : true,
    });
    const track = camStream.getVideoTracks()[0];
    state.call.localStream.getVideoTracks().forEach((t) => {
      t.stop();
      state.call.localStream.removeTrack(t);
    });
    state.call.localStream.addTrack(track);
    state.call.cam = true;
    state.call.screen = false;

    // ИСПРАВЛЕНИЕ: Добавляем трек ко всем существующим соединениям
    for (const pc of state.call.peers.values()) {
      const sender = localVideoSender(pc);
      if (sender) {
        await sender.replaceTrack(track);
      } else {
        pc.addTrack(track, state.call.localStream);
      }
    }
  }

  // ИСПРАВЛЕНИЕ: Обновляем локальный тайл
  const localCard = state.call.tiles.get(state.me.id);
  if (localCard) {
    localCard.video.srcObject = new MediaStream(state.call.localStream.getTracks());
    await safePlay(localCard.video);
  }
  
  setTileState(state.me.id, { mic: state.call.mic, cam: state.call.cam, screen: state.call.screen });
  updateCallButtons();
  sendCallState();
}

async function toggleScreenShare() {
  if (!state.call.active) return;

  if (state.call.screen) {
    await toggleCam();
    return;
  }

  const display = await navigator.mediaDevices.getDisplayMedia({ video: true });
  const track = display.getVideoTracks()[0];

  track.onended = async () => {
    if (state.call.screen) {
      state.call.screen = false;
      state.call.cam = false;
      state.call.localStream.getVideoTracks().forEach((t) => {
        t.stop();
        state.call.localStream.removeTrack(t);
      });
      for (const pc of state.call.peers.values()) {
        const sender = localVideoSender(pc);
        if (sender) await sender.replaceTrack(null);
      }
      const localCard = state.call.tiles.get(state.me.id);
      if (localCard) {
        localCard.video.srcObject = new MediaStream(state.call.localStream.getTracks());
        await safePlay(localCard.video);
      }
      setTileState(state.me.id, { mic: state.call.mic, cam: state.call.cam, screen: state.call.screen });
      updateCallButtons();
      sendCallState();
    }
  };

  state.call.localStream.getVideoTracks().forEach((t) => {
    t.stop();
    state.call.localStream.removeTrack(t);
  });
  state.call.localStream.addTrack(track);
  state.call.cam = true;
  state.call.screen = true;

  for (const pc of state.call.peers.values()) {
    const sender = localVideoSender(pc);
    if (sender) await sender.replaceTrack(track);
    else pc.addTrack(track, state.call.localStream);
  }

  // ИСПРАВЛЕНИЕ: Обновляем локальный тайл
  const localCard = state.call.tiles.get(state.me.id);
  if (localCard) {
    localCard.video.srcObject = new MediaStream(state.call.localStream.getTracks());
    await safePlay(localCard.video);
  }
  
  setTileState(state.me.id, { mic: state.call.mic, cam: state.call.cam, screen: state.call.screen });
  updateCallButtons();
  sendCallState();
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
  const localCard = state.call.tiles.get(state.me.id);
  qs("callGrid").innerHTML = "";
  state.call.tiles.clear();
  if (localCard) {
    const card = ensureCallTile(state.me.id, true);
    card.video.srcObject = localCard.video.srcObject || new MediaStream(state.call.localStream?.getTracks() || []);
    safePlay(card.video);
    setTileState(state.me.id, { mic: state.call.mic, cam: state.call.cam, screen: state.call.screen });
  }
}

async function syncCurrentChatIfOpen() {
  if (!state.currentChatId) return;
  const prev = qs("messages");
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
      ws.send(
        JSON.stringify({
          type: "call:join",
          chat_id: state.call.chatId,
          mic: state.call.mic,
          cam: state.call.cam,
          screen: state.call.screen,
        })
      );
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

    if (msg.type === "group:deleted") {
      if (state.currentChatId === msg.payload.chat_id) {
        state.currentChatId = null;
        state.currentChat = null;
        qs("messages").innerHTML = "";
        qs("chatMembers").innerHTML = "";
        setChatHeader(null);
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
        if (state.me.id > uid) await ensurePeer(uid, true);
      }
    }

    if (msg.type === "call:user_joined") {
      const uid = msg.payload.user_id;
      setTileState(uid, msg.payload.state || {});
      if (state.call.active && state.call.chatId === msg.payload.chat_id && state.me.id > uid) {
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
  qs("setFriendReq").value = state.settings.allow_friend_requests;
  qs("setCalls").value = state.settings.allow_calls_from;
  qs("setInvites").value = state.settings.allow_group_invites;
  qs("setLastSeen").value = state.settings.show_last_seen;
}

async function onAuthorized() {
  hide(qs("gateScreen"));
  hide(qs("authScreen"));
  show(qs("app"));

  renderProfileMini();
  await Promise.all([loadChats(), loadFriends(), loadFriendRequests(), loadSettings()]);
  connectWs();
  startFallbackSync();
}

function bindUi() {
  qs("gateCode").value = localStorage.getItem("saved_gate_code") || "";

  qs("gateBtn").onclick = async () => {
    setError("gateError", "");
    try {
      const code = qs("gateCode").value;
      await api("/api/gate", { method: "POST", body: JSON.stringify({ code }) });
      if (qs("rememberCode").checked) localStorage.setItem("saved_gate_code", code);
      hide(qs("gateScreen"));
      show(qs("authScreen"));
    } catch (e) {
      setError("gateError", e.message);
    }
  };

  qs("tabLogin").onclick = () => {
    qs("tabLogin").classList.add("active");
    qs("tabRegister").classList.remove("active");
    show(qs("loginPane"));
    hide(qs("registerPane"));
  };

  qs("tabRegister").onclick = () => {
    qs("tabRegister").classList.add("active");
    qs("tabLogin").classList.remove("active");
    hide(qs("loginPane"));
    show(qs("registerPane"));
  };

  qs("loginBtn").onclick = async () => {
    setError("authError", "");
    try {
      const r = await api("/api/login", {
        method: "POST",
        body: JSON.stringify({ username: qs("loginUsername").value, password: qs("loginPassword").value }),
      });
      state.token = r.token;
      state.me = r.user;
      localStorage.setItem("token", state.token);
      await onAuthorized();
    } catch (e) {
      setError("authError", e.message);
    }
  };

  qs("registerBtn").onclick = async () => {
    setError("authError", "");
    try {
      const r = await api("/api/register", {
        method: "POST",
        body: JSON.stringify({ username: qs("regUsername").value, password: qs("regPassword").value, nickname: qs("regNickname").value }),
      });
      state.token = r.token;
      state.me = r.user;
      localStorage.setItem("token", state.token);
      await onAuthorized();
    } catch (e) {
      setError("authError", e.message);
    }
  };

  qs("btnLogout").onclick = async () => {
    try {
      await api("/api/logout", { method: "POST", body: "{}" });
    } catch (_) {}
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

  qs("chatSearch").oninput = () => renderChatList(qs("chatSearch").value);

  qs("sendBtn").onclick = () => sendMessage({ text: qs("messageInput").value });
  qs("messageInput").addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage({ text: qs("messageInput").value });
    }
  });

  qs("btnFile").onclick = () => qs("fileInput").click();
  qs("btnAssets").onclick = async () => {
    await loadAssets();
    qs("assetsDialog").showModal();
  };
  qs("assetsClose").onclick = () => qs("assetsDialog").close();
  qs("btnUploadAsset").onclick = async () => {
    const f = qs("assetFile").files[0];
    if (!f) {
      alert("Выберите файл");
      return;
    }
    const form = new FormData();
    form.append("kind", qs("assetKind").value);
    form.append("title", qs("assetTitle").value || "");
    form.append("file", f);
    await api("/api/assets", { method: "POST", body: form, headers: {} });
    qs("assetFile").value = "";
    qs("assetTitle").value = "";
    await loadAssets();
  };
  qs("fileInput").onchange = async () => {
    const f = qs("fileInput").files[0];
    if (!f) return;
    let kind = "file";
    if (f.type.startsWith("image/")) kind = "image";
    if (f.type.startsWith("video/")) kind = "video";
    await sendMessage({ file: f, kind });
    qs("fileInput").value = "";
  };

  qs("btnVoice").onclick = () => startVoiceRecord();
  qs("btnCircle").onclick = () => startCircleRecord();

  qs("btnProfile").onclick = () => {
    qs("profileNickname").value = state.me.nickname || "";
    qs("profileAbout").value = state.me.about || "";
    qs("profileDialog").showModal();
  };

  qs("profileClose").onclick = () => qs("profileDialog").close();
  qs("profileSave").onclick = async () => {
    state.me = await api("/api/profile", {
      method: "POST",
      body: JSON.stringify({ nickname: qs("profileNickname").value, about: qs("profileAbout").value }),
    });
    const avatar = qs("profileAvatar").files[0];
    if (avatar) {
      const form = new FormData();
      form.append("file", avatar);
      state.me = await api("/api/profile/avatar", { method: "POST", body: form, headers: {} });
    }
    renderProfileMini();
    qs("profileDialog").close();
  };

  qs("btnGroup").onclick = () => qs("groupDialog").showModal();
  qs("groupClose").onclick = () => qs("groupDialog").close();
  qs("groupCreate").onclick = async () => {
    const members = qs("groupMembers").value.split(",").map((v) => parseInt(v.trim(), 10)).filter((n) => Number.isInteger(n));
    const out = await api("/api/groups", {
      method: "POST",
      body: JSON.stringify({ title: qs("groupTitle").value, members }),
    });
    qs("groupDialog").close();
    await loadChats();
    await openChat(out.chat_id);
  };

  qs("btnInviteGroup").onclick = async () => {
    if (!state.currentChat || state.currentChat.type !== "group") return;
    const raw = prompt("ID пользователя для приглашения в группу");
    const id = parseInt(raw || "", 10);
    if (!Number.isInteger(id)) return;
    await api(`/api/groups/${state.currentChatId}/invite`, { method: "POST", body: JSON.stringify({ user_id: id }) });
    await loadMembers(state.currentChatId);
  };

  qs("btnDeleteGroup").onclick = async () => {
    if (!state.currentChat || state.currentChat.type !== "group") return;
    if (!confirm("Удалить группу?")) return;
    await api(`/api/groups/${state.currentChatId}`, { method: "DELETE" });
    state.currentChat = null;
    state.currentChatId = null;
    qs("messages").innerHTML = "";
    qs("chatMembers").innerHTML = "";
    setChatHeader(null);
    await loadChats();
  };

  qs("btnFriends").onclick = refreshSide;
  qs("btnCopyMyId").onclick = async () => {
    try {
      await navigator.clipboard.writeText(String(state.me.id));
      alert(`ID скопирован: ${state.me.id}`);
    } catch (_) {
      prompt("Скопируйте ваш ID", String(state.me.id));
    }
  };

  qs("userSearch").oninput = async () => {
    const q = qs("userSearch").value.trim();
    const out = qs("userResults");
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

  qs("btnSettings").onclick = async () => {
    await loadSettings();
    await loadBlockedList();
    qs("settingsDialog").showModal();
  };

  qs("settingsClose").onclick = () => qs("settingsDialog").close();
  qs("settingsSave").onclick = async () => {
    state.settings = await api("/api/settings", {
      method: "POST",
      body: JSON.stringify({
        allow_friend_requests: qs("setFriendReq").value,
        allow_calls_from: qs("setCalls").value,
        allow_group_invites: qs("setInvites").value,
        show_last_seen: qs("setLastSeen").value,
      }),
    });
    qs("settingsDialog").close();
  };
  qs("btnChangePassword").onclick = async () => {
    const oldPassword = qs("oldPassword").value;
    const newPassword = qs("newPassword").value;
    if (!oldPassword || !newPassword) {
      alert("Введите старый и новый пароль");
      return;
    }
    await api("/api/account/password", {
      method: "POST",
      body: JSON.stringify({ old_password: oldPassword, new_password: newPassword }),
    });
    qs("oldPassword").value = "";
    qs("newPassword").value = "";
    alert("Пароль успешно изменен");
  };

  qs("btnDeleteAccount").onclick = async () => {
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

  qs("btnCallStart").onclick = startCall;
  qs("btnLeaveCall").onclick = leaveCall;
  qs("btnToggleMic").onclick = toggleMic;
  qs("btnToggleCam").onclick = toggleCam;
  qs("btnShareScreen").onclick = toggleScreenShare;
  qs("btnDevices").onclick = async () => {
    const panel = qs("devicePanel");
    if (panel.classList.contains("hidden")) {
      await refreshDevicePanel();
      show(panel);
    } else {
      hide(panel);
    }
  };
  qs("selMic").onchange = async () => switchMicDevice(qs("selMic").value);
  qs("selCam").onchange = async () => switchCamDevice(qs("selCam").value);
  qs("selSpeaker").onchange = async () => {
    state.devicePrefs.speakerId = qs("selSpeaker").value || "";
    await applySpeakerToAllTiles();
  };
  qs("selAudioMode").onchange = async () => applyAudioMode(qs("selAudioMode").value);
}

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
