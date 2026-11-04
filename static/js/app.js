"use strict";

(() => {
  const MAX_MESSAGES = 200;
  const MAX_CHART_SERIES = 8;
  const MAX_CHART_POINTS = 50;
  const MQTT_STATES = ["connecting", "connected", "disconnected", "error"];
  const CHART_COLOR_TOKENS = Array.from(
    { length: MAX_CHART_SERIES },
    (_, index) => `--chart-series-${index + 1}`
  );
  const THEME_KEY = "mqtt-dashboard-theme";
  const DEFAULT_STATUS = {
    connecting: "Đang kết nối tới broker...",
    connected: "Đã kết nối tới broker.",
    disconnected: "Chưa kết nối tới broker.",
    error: "Đã xảy ra lỗi kết nối.",
  };
  const QoS_LABELS = {
    0: "At most once",
    1: "At least once",
    2: "Exactly once",
  };
  const MAX_LAN_AGENTS = 20;
  const MAX_LAN_DEVICES = 200;
  const MAX_LAN_EVENTS = 100;
  const LAN_EVENT_TYPES = new Set([
    "agent_started",
    "device_down",
    "device_up",
    "new_device",
    "port_opened",
    "port_closed",
  ]);
  const LAN_SEVERITIES = new Set(["info", "warning", "critical"]);
  const LAN_EVENT_LABELS = {
    agent_started: "Agent khởi động",
    device_down: "Mất kết nối",
    device_up: "Kết nối lại",
    new_device: "Thiết bị lạ",
    port_opened: "Cổng mở",
    port_closed: "Cổng đóng",
  };
  const LAN_SEVERITY_LABELS = {
    info: "THÔNG TIN",
    warning: "CẢNH BÁO",
    critical: "NGHIÊM TRỌNG",
  };
  const LAN_EVENT_TOAST_TYPES = {
    device_down: "error",
    new_device: "warning",
    port_opened: "warning",
    device_up: "success",
  };

  const els = {
    themeToggle: document.getElementById("theme-toggle"),
    connectionForm: document.getElementById("connection-form"),
    host: document.getElementById("broker-host"),
    port: document.getElementById("broker-port"),
    username: document.getElementById("broker-username"),
    password: document.getElementById("broker-password"),
    togglePassword: document.getElementById("toggle-password"),
    useTls: document.getElementById("use-tls"),
    tlsHint: document.getElementById("tls-hint"),
    connectBtn: document.getElementById("connect-btn"),
    disconnectBtn: document.getElementById("disconnect-btn"),
    statusPanel: document.getElementById("status-panel"),
    statusLabel: document.getElementById("status-label"),
    statusMessage: document.getElementById("status-message"),
    subscriptionForm: document.getElementById("subscription-form"),
    topicInput: document.getElementById("subscription-topic"),
    subscribeBtn: document.getElementById("subscribe-btn"),
    topicList: document.getElementById("topic-list"),
    topicEmpty: document.getElementById("topic-empty"),
    topicCount: document.getElementById("topic-count"),
    publishForm: document.getElementById("publish-form"),
    publishTopic: document.getElementById("publish-topic"),
    publishPayload: document.getElementById("publish-payload"),
    publishQos: document.getElementById("publish-qos"),
    publishRetain: document.getElementById("publish-retain"),
    publishBtn: document.getElementById("publish-btn"),
    messagesScroll: document.getElementById("messages-scroll"),
    messagesBody: document.getElementById("messages-body"),
    messagesEmpty: document.getElementById("messages-empty"),
    messagesEmptyTitle: document.getElementById("messages-empty-title"),
    messagesEmptyText: document.getElementById("messages-empty-text"),
    messageCount: document.getElementById("message-count"),
    statsTotal: document.getElementById("stats-total"),
    statsRate: document.getElementById("stats-rate"),
    statsTopics: document.getElementById("stats-topics"),
    chartCanvas: document.getElementById("realtime-chart"),
    chartEmpty: document.getElementById("chart-empty"),
    chartLegend: document.getElementById("chart-legend"),
    chartLimit: document.getElementById("chart-limit"),
    chartUnavailable: document.getElementById("chart-unavailable"),
    topicFilter: document.getElementById("topic-filter"),
    exportCsv: document.getElementById("export-csv"),
    clearMessages: document.getElementById("clear-messages"),
    toastContainer: document.getElementById("toast-container"),
    lanmonPrefix: document.getElementById("lanmon-prefix"),
    lanmonSubscribe: document.getElementById("lanmon-subscribe"),
    lanBrokerWarning: document.getElementById("lanmon-broker-warning"),
    devTotal: document.getElementById("dev-total"),
    devOnline: document.getElementById("dev-online"),
    devOffline: document.getElementById("dev-offline"),
    devStale: document.getElementById("dev-stale"),
    devLatencyAvg: document.getElementById("dev-latency-avg"),
    agentList: document.getElementById("agent-list"),
    devicesBody: document.getElementById("devices-body"),
    devicesEmpty: document.getElementById("devices-empty"),
    eventList: document.getElementById("event-list"),
    eventCount: document.getElementById("event-count"),
    eventEmpty: document.getElementById("event-empty"),
    clearEvents: document.getElementById("clear-events"),
  };

  const state = {
    mqtt: "disconnected",
    reconnecting: false,
    socketReady: false,
    hadSocketConnection: false,
    syncInProgress: false,
    seq: 0,
    totalMessages: 0,
    lastToast: { key: "", at: 0 },
    socketErrorShown: false,
  };

  const topics = new Set();
  const receivedTopics = new Set();
  const messages = [];
  const recentMessageTimes = [];
  const chartSeries = new Map();
  let chart = null;
  let chartUpdateScheduled = false;
  let chartLimitReached = false;
  const timeFormatter = new Intl.DateTimeFormat("vi-VN", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  const fullTimeFormatter = new Intl.DateTimeFormat("vi-VN", {
    dateStyle: "short",
    timeStyle: "medium",
  });

  let socket = null;
  let filterQuery = "";
  let renderScheduled = false;
  let highlightLast = false;

  const lanAgents = new Map();
  const lanDevices = new Map();
  const lanEvents = [];
  const lanState = {
    rows: new Map(),
    order: "",
    deviceLimitToasted: false,
    agentLimitToasted: false,
    hadBrokerConnection: false,
  };

  initChart();
  initTheme();
  bindConnectionForm();
  bindSubscriptions();
  bindPublish();
  bindMessages();
  lanBindMonitor();
  updateStatistics();
  window.setInterval(updateMessageRate, 1000);
  window.setInterval(lanTick, 1000);
  connectSocket();
  applyConnectionState("disconnected", DEFAULT_STATUS.disconnected, false);
  renderTopics();
  renderMessages(false);
  lanRenderAgents();
  lanRenderDevices();
  lanRenderEvents();

  // -------------------------------------------------------------------------
  // Theme
  // -------------------------------------------------------------------------
  function initTheme() {
    const current = document.documentElement.dataset.theme === "dark" ? "dark" : "light";
    syncTheme(current);

    els.themeToggle.addEventListener("click", () => {
      const next = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
      document.documentElement.dataset.theme = next;
      try {
        localStorage.setItem(THEME_KEY, next);
      } catch (error) {
        // localStorage can be unavailable in private browsing.
      }
      syncTheme(next);
    });
  }

  function syncTheme(theme) {
    const dark = theme === "dark";
    els.themeToggle.setAttribute("aria-pressed", String(dark));
    const label = dark ? "Chuyển sang chế độ sáng" : "Chuyển sang chế độ tối";
    els.themeToggle.setAttribute("aria-label", label);
    els.themeToggle.title = label;
    const meta = document.querySelector('meta[name="theme-color"]');
    if (meta) {
      meta.setAttribute("content", dark ? "#070b14" : "#f4f6fb");
    }
    updateChartTheme();
  }

  // -------------------------------------------------------------------------
  // Connection form
  // -------------------------------------------------------------------------
  function bindConnectionForm() {
    els.connectionForm.addEventListener("submit", handleConnectSubmit);
    els.disconnectBtn.addEventListener("click", handleDisconnect);
    els.useTls.addEventListener("change", () => {
      const tls = els.useTls.checked;
      els.port.placeholder = tls ? "8883" : "1883";
      els.tlsHint.textContent = tls ? "(mặc định cổng 8883)" : "(mặc định cổng 1883)";
    });
    els.togglePassword.addEventListener("click", togglePasswordVisibility);
  }

  function togglePasswordVisibility() {
    const show = els.password.type === "password";
    els.password.type = show ? "text" : "password";
    els.togglePassword.classList.toggle("is-visible", show);
    const label = show ? "Ẩn password" : "Hiện password";
    els.togglePassword.setAttribute("aria-label", label);
    els.togglePassword.title = label;
  }

  function handleConnectSubmit(event) {
    event.preventDefault();
    if (state.mqtt === "connected" || state.mqtt === "connecting") {
      return;
    }

    const host = els.host.value.trim();
    if (!host) {
      toast("Vui lòng nhập địa chỉ broker.", "warning");
      els.host.focus();
      return;
    }

    const data = { host, use_tls: els.useTls.checked };
    const rawPort = els.port.value.trim();
    if (rawPort !== "") {
      if (!/^\d+$/.test(rawPort)) {
        toast("Port phải là số nguyên từ 1 đến 65535.", "warning");
        els.port.focus();
        return;
      }
      const port = Number(rawPort);
      if (port < 1 || port > 65535) {
        toast("Port phải nằm trong khoảng 1-65535.", "warning");
        els.port.focus();
        return;
      }
      data.port = port;
    }

    const username = els.username.value.trim();
    const password = els.password.value;
    if (username) {
      data.username = username;
      if (password !== "") {
        data.password = password;
      }
    } else if (password !== "") {
      toast("Vui lòng nhập Username khi sử dụng Password.", "warning");
      els.username.focus();
      return;
    }

    applyConnectionState("connecting", `Đang kết nối tới ${host}...`, false);
    sendEvent("connect_broker", data)
      .then((response) => {
        if (!response.ok) {
          const message = response.error || "Không thể kết nối broker.";
          applyConnectionState("error", message, false);
          toast(message, "error");
        }
      })
      .catch((error) => {
        applyConnectionState("error", error.message, false);
        toast(error.message, "error");
      });
  }

  function handleDisconnect() {
    if (els.disconnectBtn.disabled) {
      return;
    }
    sendEvent("disconnect_broker", {})
      .then((response) => {
        if (!response.ok) {
          toast(response.error || "Không thể ngắt kết nối.", "error");
          return;
        }
        applyConnectionState(
          "disconnected",
          response.message || "Đã ngắt kết nối MQTT broker.",
          false
        );
        toast("Đã ngắt kết nối broker.", "info");
      })
      .catch((error) => toast(error.message, "error"));
  }

  function applyConnectionState(nextState, message, reconnecting) {
    state.mqtt = MQTT_STATES.includes(nextState) ? nextState : "error";
    state.reconnecting = Boolean(reconnecting);
    if (state.mqtt !== "connected") {
      recentMessageTimes.length = 0;
    }
    updateStatistics();

    const visualState =
      state.mqtt === "disconnected" && state.reconnecting ? "connecting" : state.mqtt;
    els.statusPanel.dataset.state = visualState;
    els.statusLabel.textContent = statusLabel(state.mqtt, state.reconnecting);
    els.statusMessage.textContent = message || DEFAULT_STATUS[state.mqtt];

    const connected = state.mqtt === "connected";
    const busy = connected || state.mqtt === "connecting";

    els.connectBtn.disabled = busy || !state.socketReady;
    els.disconnectBtn.disabled =
      !state.socketReady || (state.mqtt === "disconnected" && !state.reconnecting);
    els.subscribeBtn.disabled = !connected;
    els.publishBtn.disabled = !connected;
    els.lanmonSubscribe.disabled = !connected;

    if (connected) {
      lanState.hadBrokerConnection = true;
      els.lanBrokerWarning.hidden = true;
    } else {
      els.lanBrokerWarning.hidden = !lanState.hadBrokerConnection;
    }

    [els.host, els.port, els.username, els.password, els.useTls, els.togglePassword].forEach(
      (input) => {
        input.disabled = busy;
      }
    );

    els.topicList.dataset.connected = String(connected);
    renderTopics();
  }

  function statusLabel(mqttState, reconnecting) {
    if (mqttState === "connected") {
      return "Đã kết nối";
    }
    if (mqttState === "connecting") {
      return "Đang kết nối";
    }
    if (mqttState === "error") {
      return reconnecting ? "Đang thử lại" : "Lỗi kết nối";
    }
    return reconnecting ? "Đang kết nối lại" : "Chưa kết nối";
  }

  // -------------------------------------------------------------------------
  // Subscriptions
  // -------------------------------------------------------------------------
  function bindSubscriptions() {
    els.subscriptionForm.addEventListener("submit", (event) => {
      event.preventDefault();
      subscribeCurrentTopic();
    });
  }

  function subscribeCurrentTopic() {
    if (state.mqtt !== "connected") {
      toast("Chỉ có thể subscribe khi đã kết nối broker.", "warning");
      return;
    }
    const topic = els.topicInput.value.trim();
    if (!topic) {
      toast("Vui lòng nhập topic cần subscribe.", "warning");
      els.topicInput.focus();
      return;
    }
    if (topics.has(topic)) {
      toast("Topic này đã có trong danh sách.", "info");
      return;
    }

    sendEvent("subscribe", { topic })
      .then((response) => {
        if (!response.ok) {
          toast(response.error || "Subscribe thất bại.", "error");
          return;
        }
        topics.add(topic);
        els.topicInput.value = "";
        renderTopics();
        toast(`Đã subscribe ${topic}`, "success");
      })
      .catch((error) => toast(error.message, "error"));
  }

  function unsubscribeTopic(topic) {
    if (state.mqtt !== "connected") {
      toast("Chỉ có thể bỏ subscribe khi đã kết nối broker.", "warning");
      return;
    }
    sendEvent("unsubscribe", { topic })
      .then((response) => {
        if (!response.ok) {
          toast(response.error || "Unsubscribe thất bại.", "error");
          return;
        }
        topics.delete(topic);
        renderTopics();
        toast(`Đã bỏ subscribe ${topic}`, "info");
      })
      .catch((error) => toast(error.message, "error"));
  }

  async function syncSubscriptions() {
    if (state.syncInProgress || state.mqtt !== "connected" || topics.size === 0) {
      return;
    }
    state.syncInProgress = true;
    const snapshot = Array.from(topics);
    try {
      for (const topic of snapshot) {
        if (state.mqtt !== "connected") {
          break;
        }
        if (!topics.has(topic)) {
          continue;
        }
        try {
          const response = await sendEvent("subscribe", { topic }, 10000);
          if (!response.ok) {
            toast(
              `Không thể khôi phục ${topic}: ${response.error || "lỗi không xác định"}`,
              "error"
            );
          }
        } catch (error) {
          toast(`Không thể khôi phục ${topic}: ${error.message}`, "error");
        }
      }
    } finally {
      state.syncInProgress = false;
    }
  }

  function renderTopics() {
    els.topicCount.textContent = String(topics.size);
    els.topicCount.title = `${topics.size} topic đang subscribe`;
    els.topicEmpty.hidden = topics.size > 0;
    els.topicList.replaceChildren();

    const connected = state.mqtt === "connected";
    topics.forEach((topic) => {
      const item = document.createElement("li");
      item.className = "topic-item";

      const name = document.createElement("span");
      name.className = "topic-name";
      name.textContent = topic;

      const remove = document.createElement("button");
      remove.type = "button";
      remove.className = "topic-remove";
      remove.textContent = "×";
      remove.setAttribute("aria-label", `Bỏ subscribe ${topic}`);
      remove.title = connected
        ? "Bỏ subscribe"
        : "Chỉ có thể bỏ topic khi đã kết nối broker";
      remove.disabled = !connected;
      remove.addEventListener("click", () => unsubscribeTopic(topic));

      item.append(name, remove);
      els.topicList.append(item);
    });
  }

  // -------------------------------------------------------------------------
  // Publish
  // -------------------------------------------------------------------------
  function bindPublish() {
    els.publishForm.addEventListener("submit", (event) => {
      event.preventDefault();
      publishMessage();
    });
    els.publishPayload.addEventListener("keydown", (event) => {
      if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
        event.preventDefault();
        publishMessage();
      }
    });
  }

  function publishMessage() {
    if (state.mqtt !== "connected") {
      toast("Chỉ có thể publish khi đã kết nối broker.", "warning");
      return;
    }
    const topic = els.publishTopic.value.trim();
    if (!topic) {
      toast("Vui lòng nhập topic cần publish.", "warning");
      els.publishTopic.focus();
      return;
    }
    if (topic.includes("+") || topic.includes("#")) {
      toast("Topic publish không được chứa wildcard + hoặc #.", "warning");
      els.publishTopic.focus();
      return;
    }

    const data = {
      topic,
      payload: els.publishPayload.value,
      qos: Number(els.publishQos.value),
      retain: els.publishRetain.checked,
    };

    sendEvent("publish", data)
      .then((response) => {
        if (!response.ok) {
          toast(response.error || "Publish thất bại.", "error");
          return;
        }
        toast(`Đã publish tới ${topic}`, "success");
      })
      .catch((error) => toast(error.message, "error"));
  }

  // -------------------------------------------------------------------------
  // Messages
  // -------------------------------------------------------------------------
  function bindMessages() {
    els.topicFilter.addEventListener("input", () => {
      filterQuery = els.topicFilter.value.trim();
      renderMessages(false);
    });
    els.exportCsv.addEventListener("click", exportCsv);
    els.clearMessages.addEventListener("click", clearDashboardData);
  }

  function handleMqttMessage(data) {
    if (!data || typeof data !== "object" || typeof data.topic !== "string") {
      return;
    }
    const qos = Number(data.qos);
    const message = {
      id: ++state.seq,
      topic: data.topic,
      payload: data.payload == null ? "" : String(data.payload),
      qos: [0, 1, 2].includes(qos) ? qos : 0,
      retain: Boolean(data.retain),
      timestamp:
        typeof data.timestamp === "string" ? data.timestamp : new Date().toISOString(),
    };

    const receivedAt = Date.now();
    state.totalMessages += 1;
    receivedTopics.add(message.topic);
    recentMessageTimes.push(receivedAt);
    pruneRecentMessageTimes(receivedAt);
    updateStatistics(receivedAt);

    const chartTimestamp = Date.parse(message.timestamp);
    addMessageToChart(
      message.topic,
      message.payload,
      Number.isFinite(chartTimestamp) ? chartTimestamp : receivedAt
    );

    messages.push(message);
    if (messages.length > MAX_MESSAGES) {
      messages.splice(0, messages.length - MAX_MESSAGES);
    }
    scheduleRender(true);

    // Keep the LAN Monitor state independent from the 200-message table.
    lanHandleMessage(message, receivedAt);
  }

  function scheduleRender(animateLast) {
    if (animateLast) {
      highlightLast = true;
    }
    if (renderScheduled) {
      return;
    }
    renderScheduled = true;
    window.requestAnimationFrame(() => {
      renderScheduled = false;
      const animate = highlightLast;
      highlightLast = false;
      renderMessages(animate);
    });
  }

  function renderMessages(animateLast) {
    const visible = messages.filter((message) =>
      matchesTopicFilter(message.topic, filterQuery)
    );

    els.messageCount.textContent = String(messages.length);
    els.messageCount.title = `Đang lưu ${messages.length}/${MAX_MESSAGES} tin nhắn`;
    els.exportCsv.disabled = visible.length === 0;

    const newestId = messages.length > 0 ? messages[messages.length - 1].id : null;
    const lastVisibleId = visible.length > 0 ? visible[visible.length - 1].id : null;
    const animateLastRow = animateLast && newestId !== null && newestId === lastVisibleId;

    const fragment = document.createDocumentFragment();
    visible.forEach((message, index) => {
      const isLast = animateLastRow && index === visible.length - 1;
      fragment.append(createMessageRow(message, isLast));
    });
    els.messagesBody.replaceChildren(fragment);

    const empty = visible.length === 0;
    els.messagesEmpty.hidden = !empty;
    if (empty) {
      if (messages.length === 0) {
        els.messagesEmptyTitle.textContent = "Chưa có tin nhắn";
        els.messagesEmptyText.textContent = "Subscribe một topic để bắt đầu nhận dữ liệu.";
      } else {
        els.messagesEmptyTitle.textContent = "Không có tin nhắn khớp bộ lọc";
        els.messagesEmptyText.textContent = "Thử topic khác hoặc xóa bộ lọc.";
      }
    }

    window.requestAnimationFrame(() => {
      els.messagesScroll.scrollTop = els.messagesScroll.scrollHeight;
    });
  }

  function createMessageRow(message, isNew) {
    const row = document.createElement("tr");
    row.className = "msg-row";
    if (isNew) {
      row.classList.add("msg-row--new");
      row.addEventListener(
        "animationend",
        () => row.classList.remove("msg-row--new"),
        { once: true }
      );
    }

    const date = new Date(message.timestamp);
    const validDate = !Number.isNaN(date.getTime());

    const timeCell = document.createElement("td");
    timeCell.className = "cell-time";
    const time = document.createElement("span");
    time.className = "msg-time";
    time.textContent = validDate ? timeFormatter.format(date) : "--:--:--";
    if (validDate) {
      time.title = fullTimeFormatter.format(date);
    }
    timeCell.append(time);

    const topicCell = document.createElement("td");
    topicCell.className = "cell-topic";
    const topic = document.createElement("span");
    topic.className = "msg-topic";
    topic.textContent = message.topic;
    const mobileMeta = document.createElement("span");
    mobileMeta.className = "msg-meta-mobile";
    mobileMeta.append(
      makeBadge(
        `QoS ${message.qos}`,
        `badge badge-qos-${message.qos}`,
        QoS_LABELS[message.qos]
      ),
      makeBadge(
        message.retain ? "Retain: Có" : "Retain: Không",
        message.retain ? "badge badge-retain" : "badge",
        message.retain
          ? "Tin nhắn được broker giữ lại"
          : "Tin nhắn không được giữ lại"
      )
    );
    topicCell.append(topic, mobileMeta);

    const payloadCell = document.createElement("td");
    payloadCell.className = "cell-payload";
    const payload = document.createElement("div");
    payload.className = "msg-payload";
    if (message.payload === "") {
      payload.classList.add("msg-payload--empty");
      payload.textContent = "(payload rỗng)";
    } else {
      payload.textContent = message.payload;
    }
    payloadCell.append(payload);

    const qosCell = document.createElement("td");
    qosCell.className = "cell-qos";
    qosCell.append(
      makeBadge(
        String(message.qos),
        `badge badge-qos-${message.qos}`,
        QoS_LABELS[message.qos]
      )
    );

    const retainCell = document.createElement("td");
    retainCell.className = "cell-retain";
    if (message.retain) {
      retainCell.append(
        makeBadge("Có", "badge badge-retain", "Tin nhắn được broker giữ lại")
      );
    } else {
      const muted = document.createElement("span");
      muted.className = "muted";
      muted.textContent = "—";
      retainCell.append(muted);
    }

    row.append(timeCell, topicCell, payloadCell, qosCell, retainCell);
    return row;
  }

  function makeBadge(text, className, title) {
    const badge = document.createElement("span");
    badge.className = className;
    badge.textContent = text;
    if (title) {
      badge.title = title;
    }
    return badge;
  }

  function matchesTopicFilter(topic, query) {
    if (!query) {
      return true;
    }
    const normalizedTopic = topic.toLowerCase();
    const normalizedQuery = query.toLowerCase();
    if (!normalizedQuery.includes("+") && !normalizedQuery.includes("#")) {
      return normalizedTopic.includes(normalizedQuery);
    }

    const filterParts = normalizedQuery.split("/");
    const topicParts = normalizedTopic.split("/");
    for (let index = 0; index < filterParts.length; index += 1) {
      if (filterParts[index] === "#") {
        return index === filterParts.length - 1;
      }
      if (index >= topicParts.length) {
        return false;
      }
      if (filterParts[index] !== "+" && filterParts[index] !== topicParts[index]) {
        return false;
      }
    }
    return filterParts.length === topicParts.length;
  }

  // -------------------------------------------------------------------------
  // LAN Monitor
  // -------------------------------------------------------------------------
  function lanBindMonitor() {
    els.lanmonSubscribe.addEventListener("click", lanSubscribe);
    els.lanmonPrefix.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        lanSubscribe();
      }
    });
    els.clearEvents.addEventListener("click", lanClearEvents);
  }

  function lanSubscribe() {
    if (state.mqtt !== "connected") {
      toast("Chỉ có thể theo dõi LAN Monitor khi đã kết nối broker.", "warning");
      return;
    }

    const prefix = els.lanmonPrefix.value.trim();
    if (!prefix) {
      toast("Vui lòng nhập prefix LAN Monitor.", "warning");
      els.lanmonPrefix.focus();
      return;
    }
    if (/[+#/]/.test(prefix)) {
      toast("Prefix không được chứa ký tự +, # hoặc /.", "warning");
      els.lanmonPrefix.focus();
      return;
    }

    const topic = `${prefix}/#`;
    if (topics.has(topic)) {
      toast(`Đã theo dõi prefix ${prefix}.`, "info");
      return;
    }

    // Reuse the existing subscribe path so the topic also appears in the
    // "Topic đang theo dõi" list and is restored after a reconnect.
    els.topicInput.value = topic;
    subscribeCurrentTopic();
  }

  function lanGetPrefix() {
    const prefix = els.lanmonPrefix.value.trim();
    if (!prefix || /[+#/]/.test(prefix)) {
      return "";
    }
    return prefix;
  }

  function lanEscapeRegex(value) {
    return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }

  function lanHandleMessage(message, receivedAt) {
    if (
      !message ||
      typeof message.topic !== "string" ||
      typeof message.payload !== "string"
    ) {
      return;
    }

    try {
      const prefix = lanGetPrefix();
      if (!prefix) {
        return;
      }
      const pattern = new RegExp(
        `^${lanEscapeRegex(prefix)}/([^/]+)/(status|device/([^/]+)|event)$`
      );
      const match = pattern.exec(message.topic);
      if (!match) {
        return;
      }

      let payload;
      try {
        payload = JSON.parse(message.payload);
      } catch (error) {
        return;
      }
      if (payload === null || typeof payload !== "object" || Array.isArray(payload)) {
        return;
      }

      const agent = match[1];
      if (payload.agent !== agent) {
        return;
      }

      if (match[2] === "status") {
        lanApplyStatus(agent, payload, receivedAt);
      } else if (match[2] === "event") {
        lanApplyEvent(agent, payload, receivedAt);
      } else {
        lanApplyDevice(agent, match[3], payload, receivedAt);
      }
    } catch (error) {
      // Malformed external payloads must never break the dashboard.
    }
  }

  function lanApplyStatus(agent, payload, receivedAt) {
    if (payload.state !== "online" && payload.state !== "offline") {
      return;
    }
    if (
      payload.interval_s !== undefined &&
      (!lanIsFiniteNumber(payload.interval_s) || payload.interval_s <= 0)
    ) {
      return;
    }
    if (
      payload.targets !== undefined &&
      (!lanIsFiniteNumber(payload.targets) || payload.targets < 0)
    ) {
      return;
    }
    if (payload.version !== undefined && !lanIsFiniteNumber(payload.version)) {
      return;
    }
    if (payload.ts !== undefined && typeof payload.ts !== "string") {
      return;
    }

    const existing = lanAgents.get(agent);
    if (!existing && lanAgents.size >= MAX_LAN_AGENTS) {
      if (!lanState.agentLimitToasted) {
        lanState.agentLimitToasted = true;
        toast(`Đã đạt giới hạn ${MAX_LAN_AGENTS} agent; bỏ qua agent mới.`, "warning");
      }
      return;
    }

    const intervalS =
      lanIsFiniteNumber(payload.interval_s) && payload.interval_s > 0
        ? payload.interval_s
        : existing
          ? existing.intervalS
          : null;

    lanAgents.set(agent, {
      agent,
      state: payload.state,
      intervalS,
      targets: lanIsFiniteNumber(payload.targets)
        ? payload.targets
        : existing
          ? existing.targets
          : null,
      version: lanIsFiniteNumber(payload.version)
        ? payload.version
        : existing
          ? existing.version
          : null,
      updatedMs: lanParseTimestamp(payload.ts, receivedAt),
    });

    lanRenderAgents();
    lanRenderDevices();
  }

  function lanApplyDevice(agent, topicDeviceId, payload, receivedAt) {
    if (typeof payload.id !== "string" || !payload.id || payload.id !== topicDeviceId) {
      return;
    }
    if (typeof payload.host !== "string" || typeof payload.name !== "string") {
      return;
    }
    if (typeof payload.online !== "boolean") {
      return;
    }
    if (
      !(
        payload.latency_ms === null ||
        (lanIsFiniteNumber(payload.latency_ms) && payload.latency_ms >= 0)
      )
    ) {
      return;
    }
    if (
      !lanIsFiniteNumber(payload.loss_pct) ||
      payload.loss_pct < 0 ||
      payload.loss_pct > 100
    ) {
      return;
    }
    if (
      payload.ports === null ||
      typeof payload.ports !== "object" ||
      Array.isArray(payload.ports)
    ) {
      return;
    }
    if (!(payload.mac === null || typeof payload.mac === "string")) {
      return;
    }
    if (typeof payload.known !== "boolean" || typeof payload.ts !== "string") {
      return;
    }

    const ports = {};
    for (const [port, isOpen] of Object.entries(payload.ports)) {
      if (!/^\d+$/.test(port) || typeof isOpen !== "boolean") {
        return;
      }
      const portNumber = Number(port);
      if (!Number.isInteger(portNumber) || portNumber < 1 || portNumber > 65535) {
        return;
      }
      ports[port] = isOpen;
    }

    const key = `${agent}/${topicDeviceId}`;
    const existing = lanDevices.get(key);
    if (!existing && lanDevices.size >= MAX_LAN_DEVICES) {
      if (!lanState.deviceLimitToasted) {
        lanState.deviceLimitToasted = true;
        toast(
          `Đã đạt giới hạn ${MAX_LAN_DEVICES} thiết bị; bỏ qua thiết bị mới.`,
          "warning"
        );
      }
      return;
    }

    const updatedMs = lanParseTimestamp(payload.ts, receivedAt);
    if (existing && updatedMs < existing.updatedMs) {
      return;
    }

    lanDevices.set(key, {
      key,
      agent,
      id: topicDeviceId,
      host: lanClampText(payload.host, 64),
      name: lanClampText(payload.name, 64),
      online: payload.online,
      latencyMs: payload.latency_ms,
      lossPct: payload.loss_pct,
      ports,
      mac: payload.mac === null ? null : lanClampText(payload.mac, 32),
      known: payload.known,
      updatedMs,
      receivedAt,
    });

    lanRenderDevices();
  }

  function lanApplyEvent(agent, payload, receivedAt) {
    if (typeof payload.type !== "string" || !LAN_EVENT_TYPES.has(payload.type)) {
      return;
    }
    if (
      typeof payload.severity !== "string" ||
      !LAN_SEVERITIES.has(payload.severity)
    ) {
      return;
    }
    if (typeof payload.host !== "string" || typeof payload.name !== "string") {
      return;
    }
    if (typeof payload.message !== "string" || typeof payload.ts !== "string") {
      return;
    }

    const event = {
      agent,
      type: payload.type,
      severity: payload.severity,
      host: lanClampText(payload.host, 64),
      name: lanClampText(payload.name, 64),
      message: lanClampText(payload.message, 200),
      tsMs: lanParseTimestamp(payload.ts, receivedAt),
    };

    lanEvents.unshift(event);
    if (lanEvents.length > MAX_LAN_EVENTS) {
      lanEvents.length = MAX_LAN_EVENTS;
    }
    lanRenderEvents();
    lanToastEvent(event);
  }

  function lanToastEvent(event) {
    const toastType = LAN_EVENT_TOAST_TYPES[event.type];
    if (!toastType) {
      return;
    }
    const label = LAN_EVENT_LABELS[event.type] || event.type;
    const device = event.name || event.host || event.agent;
    const message = event.message ? `: ${event.message}` : "";
    toast(`${label} — ${device} (${event.agent})${message}`, toastType);
  }

  function lanClearEvents() {
    if (lanEvents.length === 0) {
      return;
    }
    lanEvents.length = 0;
    lanRenderEvents();
    toast("Đã xóa nhật ký cảnh báo.", "info");
  }

  function lanRenderEvents() {
    els.eventCount.textContent = String(lanEvents.length);
    els.eventCount.title = `Đang lưu ${lanEvents.length}/${MAX_LAN_EVENTS} cảnh báo`;
    els.eventEmpty.hidden = lanEvents.length > 0;

    const fragment = document.createDocumentFragment();
    lanEvents.forEach((event) => {
      fragment.append(lanCreateEventRow(event));
    });
    els.eventList.replaceChildren(fragment);
  }

  function lanCreateEventRow(event) {
    const item = document.createElement("li");
    item.className = "lanmon-event";

    const time = document.createElement("span");
    time.className = "lanmon-event-time";
    time.textContent = lanFormatClock(event.tsMs);

    const main = document.createElement("div");
    main.className = "lanmon-event-main";

    const severity = document.createElement("span");
    severity.className = `lanmon-event-severity lanmon-event-severity--${event.severity}`;
    severity.textContent =
      LAN_SEVERITY_LABELS[event.severity] || event.severity.toUpperCase();

    const type = document.createElement("span");
    type.className = "lanmon-event-type";
    type.textContent = LAN_EVENT_LABELS[event.type] || event.type;

    const device = document.createElement("span");
    device.className = "lanmon-event-device";
    device.textContent = event.name || event.host || "—";
    if (event.host && event.name && event.host !== event.name) {
      device.title = event.host;
    }

    const agent = document.createElement("span");
    agent.className = "lanmon-event-agent";
    agent.textContent = event.agent;

    const message = document.createElement("p");
    message.className = "lanmon-event-message";
    message.textContent = event.message || "—";

    main.append(severity, type, device, agent);
    item.append(time, main, message);
    return item;
  }

  function lanRenderAgents() {
    const agents = Array.from(lanAgents.values()).sort((a, b) =>
      a.agent.localeCompare(b.agent, "vi", { sensitivity: "base" })
    );
    const fragment = document.createDocumentFragment();

    agents.forEach((agent) => {
      const chip = document.createElement("span");
      chip.className = "lanmon-agent-chip";
      if (agent.state === "offline") {
        chip.classList.add("is-offline");
      }
      chip.dataset.agent = agent.agent;

      const dot = document.createElement("span");
      dot.className = "lanmon-agent-dot";
      dot.setAttribute("aria-hidden", "true");

      const name = document.createElement("span");
      name.className = "lanmon-agent-name";
      name.textContent = agent.agent;

      const stateText = document.createElement("span");
      stateText.className = "lanmon-agent-state";
      stateText.textContent = agent.state === "offline" ? "offline" : "online";

      chip.append(dot, name, stateText);

      if (lanIsFiniteNumber(agent.intervalS) && agent.intervalS > 0) {
        const interval = document.createElement("span");
        interval.className = "lanmon-agent-interval";
        interval.textContent = `mỗi ${lanFormatNumber(agent.intervalS)}s`;
        chip.append(interval);
      }

      const targetsText = lanIsFiniteNumber(agent.targets)
        ? `${agent.targets} thiết bị`
        : "";
      chip.title = `${agent.agent}: ${stateText.textContent}${
        targetsText ? `, ${targetsText}` : ""
      }`;
      fragment.append(chip);
    });

    els.agentList.replaceChildren(fragment);
  }

  function lanTick() {
    lanRenderDevices();
  }

  function lanRenderDevices(now = Date.now()) {
    const items = [];
    lanDevices.forEach((device) => {
      items.push({
        device,
        status: lanDeviceStatus(device, now),
        updatedText: lanRelativeTime(device.updatedMs, now),
      });
    });
    items.sort(lanCompareDevices);

    let online = 0;
    let offline = 0;
    let stale = 0;
    let latencySum = 0;
    let latencyCount = 0;

    items.forEach(({ device, status }) => {
      if (status.key === "online") {
        online += 1;
        if (lanIsFiniteNumber(device.latencyMs)) {
          latencySum += device.latencyMs;
          latencyCount += 1;
        }
      } else if (status.key === "offline") {
        offline += 1;
      } else {
        stale += 1;
      }
    });

    els.devTotal.textContent = String(items.length);
    els.devOnline.textContent = String(online);
    els.devOffline.textContent = String(offline);
    els.devStale.textContent = String(stale);
    els.devLatencyAvg.textContent =
      latencyCount > 0 ? `${(latencySum / latencyCount).toFixed(1)} ms` : "—";
    els.devicesEmpty.hidden = items.length > 0;

    const seen = new Set();
    items.forEach(({ device, status, updatedText }) => {
      let entry = lanState.rows.get(device.key);
      if (!entry) {
        entry = lanCreateDeviceRow(device);
        lanState.rows.set(device.key, entry);
      }
      lanUpdateDeviceRow(entry, device, status, updatedText);
      seen.add(device.key);
    });

    lanState.rows.forEach((entry, key) => {
      if (!seen.has(key)) {
        entry.row.remove();
        lanState.rows.delete(key);
      }
    });

    const order = items.map((item) => item.device.key).join("\u0000");
    if (order !== lanState.order) {
      items.forEach((item) => {
        const entry = lanState.rows.get(item.device.key);
        if (entry) {
          els.devicesBody.append(entry.row);
        }
      });
      lanState.order = order;
    }
  }

  function lanCreateDeviceRow(device) {
    const row = document.createElement("tr");
    row.dataset.key = device.key;

    const statusCell = document.createElement("td");
    const status = document.createElement("span");
    status.className = "lan-status";
    const statusDot = document.createElement("span");
    statusDot.className = "lan-status-dot";
    statusDot.setAttribute("aria-hidden", "true");
    const statusText = document.createElement("span");
    statusText.className = "lan-status-text";
    status.append(statusDot, statusText);
    statusCell.append(status);

    const deviceCell = document.createElement("td");
    const title = document.createElement("div");
    title.className = "lan-device-title";
    const name = document.createElement("span");
    name.className = "lan-device-name";
    const unknown = document.createElement("span");
    unknown.className = "lan-device-unknown";
    unknown.textContent = "LẠ";
    unknown.hidden = device.known;
    title.append(name, unknown);
    const host = document.createElement("span");
    host.className = "lan-device-host";
    const mac = document.createElement("span");
    mac.className = "lan-device-mac";
    deviceCell.append(title, host, mac);

    const agentCell = document.createElement("td");
    const agent = document.createElement("span");
    agent.className = "lan-device-agent";
    agentCell.append(agent);

    const latencyCell = document.createElement("td");
    const latency = document.createElement("span");
    latency.className = "lan-metric";
    latencyCell.append(latency);

    const lossCell = document.createElement("td");
    const loss = document.createElement("span");
    loss.className = "lan-metric";
    lossCell.append(loss);

    const portsCell = document.createElement("td");
    const ports = document.createElement("span");
    ports.className = "lan-ports";
    portsCell.append(ports);

    const updatedCell = document.createElement("td");
    const updated = document.createElement("span");
    updated.className = "lan-updated";
    updatedCell.append(updated);

    row.append(
      statusCell,
      deviceCell,
      agentCell,
      latencyCell,
      lossCell,
      portsCell,
      updatedCell
    );

    return {
      row,
      status,
      statusText,
      name,
      host,
      mac,
      unknown,
      agent,
      latency,
      loss,
      ports,
      updated,
      portsKey: null,
      statusKey: "",
    };
  }

  function lanUpdateDeviceRow(entry, device, status, updatedText) {
    if (entry.statusKey !== status.key) {
      entry.statusKey = status.key;
      entry.status.className = `lan-status lan-status--${status.key}`;
      entry.statusText.textContent = status.label;
      entry.status.title = status.label;
    }

    lanSetText(entry.name, device.name || device.host || "—");
    lanSetText(entry.host, device.host || "—");

    if (device.mac) {
      entry.mac.hidden = false;
      lanSetText(entry.mac, `MAC ${device.mac}`);
    } else {
      entry.mac.hidden = true;
    }

    entry.unknown.hidden = device.known;
    lanSetText(entry.agent, device.agent);
    lanSetText(entry.latency, lanFormatLatency(device.latencyMs));
    lanSetText(entry.loss, `${lanFormatNumber(device.lossPct)}%`);

    const portsKey = lanPortsKey(device.ports);
    if (portsKey !== entry.portsKey) {
      entry.portsKey = portsKey;
      lanRenderPorts(entry.ports, device.ports);
    }

    lanSetText(entry.updated, updatedText);
    entry.updated.title = fullTimeFormatter.format(new Date(device.updatedMs));
  }

  function lanRenderPorts(container, ports) {
    const keys = Object.keys(ports).sort((a, b) => Number(a) - Number(b));
    container.replaceChildren();

    if (keys.length === 0) {
      container.textContent = "—";
      container.classList.add("lan-ports--empty");
      return;
    }

    container.classList.remove("lan-ports--empty");
    keys.forEach((port) => {
      const isOpen = ports[port];
      const chip = document.createElement("span");
      chip.className = isOpen ? "lan-port lan-port--open" : "lan-port lan-port--closed";
      chip.textContent = `${port} ${isOpen ? "mở" : "đóng"}`;
      container.append(chip);
    });
  }

  function lanPortsKey(ports) {
    return Object.keys(ports)
      .sort((a, b) => Number(a) - Number(b))
      .map((port) => `${port}:${ports[port] ? "1" : "0"}`)
      .join("|");
  }

  function lanDeviceStatus(device, now) {
    const agent = lanAgents.get(device.agent);
    if (agent && agent.state === "offline") {
      return { key: "agent-offline", label: "Agent offline" };
    }

    let threshold = 15;
    if (agent && lanIsFiniteNumber(agent.intervalS) && agent.intervalS > 0) {
      threshold = Math.max(3 * agent.intervalS, 10);
    }
    if (now - device.updatedMs > threshold * 1000) {
      return { key: "stale", label: "Mất tín hiệu" };
    }
    if (device.online) {
      return { key: "online", label: "Online" };
    }
    return { key: "offline", label: "Offline" };
  }

  function lanCompareDevices(a, b) {
    const rank = (item) => {
      if (item.status.key === "offline") {
        return 0;
      }
      if (item.status.key === "stale" || item.status.key === "agent-offline") {
        return 1;
      }
      if (!item.device.known) {
        return 2;
      }
      return 3;
    };

    const rankDiff = rank(a) - rank(b);
    if (rankDiff !== 0) {
      return rankDiff;
    }

    const nameA = (a.device.name || a.device.host || "").toLocaleLowerCase("vi");
    const nameB = (b.device.name || b.device.host || "").toLocaleLowerCase("vi");
    const nameDiff = nameA.localeCompare(nameB, "vi", { sensitivity: "base" });
    if (nameDiff !== 0) {
      return nameDiff;
    }

    const agentDiff = a.device.agent.localeCompare(b.device.agent, "vi", {
      sensitivity: "base",
    });
    if (agentDiff !== 0) {
      return agentDiff;
    }
    return a.device.key.localeCompare(b.device.key, "vi", { sensitivity: "base" });
  }

  function lanRelativeTime(timestampMs, now) {
    if (!lanIsFiniteNumber(timestampMs)) {
      return "—";
    }
    const seconds = Math.max(0, Math.floor((now - timestampMs) / 1000));
    return `${seconds} giây trước`;
  }

  function lanFormatClock(timestampMs) {
    const date = new Date(timestampMs);
    if (Number.isNaN(date.getTime())) {
      return "--:--:--";
    }
    const pad = (value) => String(value).padStart(2, "0");
    return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(
      date.getSeconds()
    )}`;
  }

  function lanFormatLatency(value) {
    return lanIsFiniteNumber(value) ? `${value.toFixed(1)} ms` : "—";
  }

  function lanFormatNumber(value) {
    if (!lanIsFiniteNumber(value)) {
      return "—";
    }
    if (Number.isInteger(value)) {
      return String(value);
    }
    return value.toFixed(1).replace(/\.0$/, "");
  }

  function lanSetText(node, text) {
    const next = String(text);
    if (node.textContent !== next) {
      node.textContent = next;
    }
  }

  function lanClampText(value, maxLength) {
    return value.length > maxLength ? value.slice(0, maxLength) : value;
  }

  function lanParseTimestamp(value, fallback) {
    if (typeof value !== "string") {
      return fallback;
    }
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  function lanIsFiniteNumber(value) {
    return typeof value === "number" && Number.isFinite(value);
  }

  // -------------------------------------------------------------------------
  // Chart and statistics
  // -------------------------------------------------------------------------
  function initChart() {
    if (typeof window.Chart !== "function") {
      showChartUnavailable();
      return;
    }

    try {
      chart = new window.Chart(els.chartCanvas.getContext("2d"), {
        type: "line",
        data: { datasets: [] },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          animation: false,
          parsing: false,
          interaction: { mode: "nearest", intersect: false },
          scales: {
            x: {
              type: "linear",
              ticks: {
                autoSkip: true,
                maxTicksLimit: 6,
                callback: (value) => formatChartTime(value),
              },
              grid: { drawTicks: true },
              border: {},
            },
            y: {
              type: "linear",
              beginAtZero: false,
              ticks: { maxTicksLimit: 5 },
              grid: {},
              border: {},
            },
          },
          plugins: {
            legend: { display: false },
            tooltip: {
              callbacks: {
                title: (items) =>
                  items.length ? formatChartTime(items[0].parsed.x) : "",
                label: (context) => `${context.dataset.label}: ${context.parsed.y}`,
              },
            },
          },
        },
      });
    } catch (error) {
      chart = null;
      showChartUnavailable();
      return;
    }

    els.chartUnavailable.hidden = true;
    els.chartCanvas.hidden = false;
    els.chartLegend.hidden = false;
    updateChartEmptyState();
    updateChartTheme();
  }

  function showChartUnavailable() {
    els.chartCanvas.hidden = true;
    els.chartEmpty.hidden = true;
    els.chartLegend.hidden = true;
    els.chartLimit.hidden = true;
    els.chartUnavailable.hidden = false;
  }

  function updateChartTheme() {
    if (!chart) {
      return;
    }
    const styles = getComputedStyle(document.documentElement);
    const getToken = (name) => styles.getPropertyValue(name).trim();
    const textColor = getToken("--text-muted");
    const gridColor = getToken("--border");

    chart.options.scales.x.ticks.color = textColor;
    chart.options.scales.y.ticks.color = textColor;
    chart.options.scales.x.grid.color = gridColor;
    chart.options.scales.y.grid.color = gridColor;
    chart.options.scales.x.border.color = gridColor;
    chart.options.scales.y.border.color = gridColor;
    chart.options.plugins.tooltip.backgroundColor = getToken("--surface");
    chart.options.plugins.tooltip.titleColor = getToken("--text");
    chart.options.plugins.tooltip.bodyColor = getToken("--text");
    chart.options.plugins.tooltip.borderColor = gridColor;
    chart.options.plugins.tooltip.borderWidth = 1;
    scheduleChartUpdate();
  }

  function chartColors() {
    const styles = getComputedStyle(document.documentElement);
    return CHART_COLOR_TOKENS.map(
      (token) => styles.getPropertyValue(token).trim() || styles.getPropertyValue("--accent").trim()
    );
  }

  function formatChartTime(value) {
    const date = new Date(Number(value));
    return Number.isNaN(date.getTime()) ? "" : timeFormatter.format(date);
  }

  function numericChartValues(topic, payload) {
    const trimmed = payload.trim();
    const rawNumberPattern = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/;
    if (rawNumberPattern.test(trimmed)) {
      const value = Number(trimmed);
      return Number.isFinite(value) ? [[topic, value]] : [];
    }

    let parsed;
    try {
      parsed = JSON.parse(trimmed);
    } catch (error) {
      return [];
    }
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      return [];
    }

    return Object.entries(parsed)
      .filter(([, value]) => typeof value === "number" && Number.isFinite(value))
      .map(([key, value]) => [`${topic} · ${key}`, value]);
  }

  function addMessageToChart(topic, payload, timestamp) {
    if (!chart) {
      return;
    }
    const values = numericChartValues(topic, payload);
    if (values.length === 0) {
      return;
    }

    const colors = chartColors();
    let changed = false;
    values.forEach(([name, value]) => {
      let dataset = chartSeries.get(name);
      if (!dataset) {
        if (chartSeries.size >= MAX_CHART_SERIES) {
          chartLimitReached = true;
          return;
        }
        const color = colors[chartSeries.size];
        dataset = {
          label: name,
          data: [],
          borderColor: color,
          backgroundColor: color,
          pointBackgroundColor: color,
          pointBorderColor: color,
          borderWidth: 2,
          pointRadius: 2,
          pointHoverRadius: 4,
          tension: 0.15,
          fill: false,
        };
        chartSeries.set(name, dataset);
        chart.data.datasets.push(dataset);
        changed = true;
      }

      dataset.data.push({ x: timestamp, y: value });
      if (dataset.data.length > MAX_CHART_POINTS) {
        dataset.data.shift();
      }
      changed = true;
    });

    els.chartLimit.hidden = !chartLimitReached;
    if (changed) {
      if (chartSeries.size > 0) {
        els.chartEmpty.hidden = true;
      }
      if (chart.data.datasets.length !== els.chartLegend.childElementCount) {
        renderChartLegend();
      }
      scheduleChartUpdate();
    }
  }

  function renderChartLegend() {
    els.chartLegend.replaceChildren();
    chart.data.datasets.forEach((dataset, index) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "chart-legend-button";

      const swatch = document.createElement("span");
      swatch.className = "chart-legend-swatch";
      swatch.style.backgroundColor = dataset.borderColor;
      swatch.setAttribute("aria-hidden", "true");

      const label = document.createElement("span");
      label.className = "chart-legend-label";
      label.textContent = dataset.label;

      const visible = chart.isDatasetVisible(index);
      button.setAttribute("aria-pressed", String(visible));
      button.setAttribute("aria-label", `${visible ? "Ẩn" : "Hiện"} đường ${dataset.label}`);
      button.append(swatch, label);
      button.addEventListener("click", () => {
        const nextVisible = !chart.isDatasetVisible(index);
        chart.setDatasetVisibility(index, nextVisible);
        button.setAttribute("aria-pressed", String(nextVisible));
        button.setAttribute(
          "aria-label",
          `${nextVisible ? "Ẩn" : "Hiện"} đường ${dataset.label}`
        );
        scheduleChartUpdate();
      });
      els.chartLegend.append(button);
    });
  }

  function scheduleChartUpdate() {
    if (!chart || chartUpdateScheduled) {
      return;
    }
    chartUpdateScheduled = true;
    window.requestAnimationFrame(() => {
      chartUpdateScheduled = false;
      if (chart) {
        chart.update("none");
      }
    });
  }

  function updateChartEmptyState() {
    if (chart) {
      els.chartEmpty.hidden = chartSeries.size > 0;
    }
  }

  function pruneRecentMessageTimes(now) {
    const cutoff = now - 5000;
    while (recentMessageTimes.length > 0 && recentMessageTimes[0] < cutoff) {
      recentMessageTimes.shift();
    }
  }

  function updateStatistics(now = Date.now()) {
    pruneRecentMessageTimes(now);
    els.statsTotal.textContent = String(state.totalMessages);
    els.statsRate.textContent =
      state.mqtt === "connected" ? (recentMessageTimes.length / 5).toFixed(1) : "0.0";
    els.statsTopics.textContent = String(receivedTopics.size);
  }

  function updateMessageRate() {
    if (state.mqtt !== "connected") {
      recentMessageTimes.length = 0;
    }
    updateStatistics();
  }

  function clearDashboardData() {
    const hadMessages = state.totalMessages > 0 || messages.length > 0;
    messages.length = 0;
    recentMessageTimes.length = 0;
    receivedTopics.clear();
    state.totalMessages = 0;
    chartSeries.clear();
    chartLimitReached = false;
    els.chartLimit.hidden = true;
    els.chartLegend.replaceChildren();
    if (chart) {
      chart.data.datasets.forEach((dataset, index) => {
        chart.setDatasetVisibility(index, true);
      });
      chart.data.datasets.length = 0;
      updateChartEmptyState();
      scheduleChartUpdate();
    }
    updateStatistics();
    renderMessages(false);
    if (hadMessages) {
      toast("Đã xóa toàn bộ tin nhắn.", "info");
    }
  }

  function exportCsv() {
    const visible = messages.filter((message) =>
      matchesTopicFilter(message.topic, filterQuery)
    );
    if (visible.length === 0) {
      return;
    }

    const rows = [
      ["timestamp", "topic", "payload", "qos", "retain"],
      ...visible.map((message) => [
        message.timestamp,
        message.topic,
        message.payload,
        message.qos,
        message.retain,
      ]),
    ];
    const csv = `\uFEFF${rows.map((row) => row.map(escapeCsvField).join(",")).join("\r\n")}\r\n`;
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const objectUrl = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = objectUrl;
    link.download = `mqtt-messages-${localFileTimestamp(new Date())}.csv`;
    link.hidden = true;
    document.body.append(link);
    link.click();
    link.remove();
    window.setTimeout(() => URL.revokeObjectURL(objectUrl), 1000);
    toast(`Đã xuất ${visible.length} tin nhắn`, "success");
  }

  function escapeCsvField(value) {
    let text = String(value == null ? "" : value);
    if (/^[=+\-@]/.test(text)) {
      text = `'${text}`;
    }
    return `"${text.replace(/"/g, '""')}"`;
  }

  function localFileTimestamp(date) {
    const pad = (value) => String(value).padStart(2, "0");
    return `${date.getFullYear()}${pad(date.getMonth() + 1)}${pad(date.getDate())}-${pad(
      date.getHours()
    )}${pad(date.getMinutes())}${pad(date.getSeconds())}`;
  }

  // -------------------------------------------------------------------------
  // Socket.IO
  // -------------------------------------------------------------------------
  function connectSocket() {
    if (typeof window.io !== "function") {
      applyConnectionState(
        "error",
        "Không tải được thư viện Socket.IO. Kiểm tra kết nối mạng.",
        false
      );
      toast("Không tải được thư viện Socket.IO. Kiểm tra kết nối mạng.", "error");
      return;
    }

    socket = window.io({
      reconnection: true,
      reconnectionDelay: 700,
      reconnectionDelayMax: 6000,
      timeout: 10000,
    });

    socket.on("connect", handleSocketConnect);
    socket.on("disconnect", handleSocketDisconnect);
    socket.on("connect_error", handleSocketConnectError);
    socket.on("status", handleStatus);
    socket.on("mqtt_message", handleMqttMessage);
  }

  function handleSocketConnect() {
    const firstConnection = !state.hadSocketConnection;
    state.socketReady = true;
    state.hadSocketConnection = true;
    state.socketErrorShown = false;

    if (firstConnection) {
      applyConnectionState(
        "disconnected",
        "Máy chủ đã sẵn sàng. Nhập thông tin broker để kết nối.",
        false
      );
    } else {
      applyConnectionState(
        "disconnected",
        "Đã kết nối lại máy chủ. Vui lòng kết nối broker.",
        false
      );
      toast("Đã kết nối lại máy chủ. Vui lòng kết nối broker.", "info");
    }
  }

  function handleSocketDisconnect() {
    state.socketReady = false;
    applyConnectionState(
      "error",
      "Mất kết nối tới máy chủ. Đang thử kết nối lại...",
      true
    );
    toast("Mất kết nối tới máy chủ. Đang thử kết nối lại...", "warning");
  }

  function handleSocketConnectError(error) {
    state.socketReady = false;
    const detail = error && error.message ? ` (${error.message})` : "";
    const message = `Không thể kết nối tới máy chủ${detail}. Đang thử lại...`;
    applyConnectionState("error", message, true);
    if (!state.socketErrorShown) {
      toast(message, "error");
      state.socketErrorShown = true;
    }
  }

  function handleStatus(data) {
    if (!data || typeof data !== "object" || !MQTT_STATES.includes(data.state)) {
      return;
    }
    const nextState = data.state;
    const message =
      typeof data.message === "string" && data.message.trim()
        ? data.message
        : DEFAULT_STATUS[nextState];
    const reconnecting = Boolean(data.reconnecting);
    const previous = state.mqtt;

    // The backend also emits "error" for a rejected subscribe/publish while the
    // MQTT connection is still alive. In that case keep the green state and
    // surface the failure as a toast instead of locking the whole UI.
    if (nextState === "error" && previous === "connected") {
      toast(message, "error");
      return;
    }

    applyConnectionState(nextState, message, reconnecting);

    if (nextState === "connected" && previous !== "connected") {
      toast(message, "success");
      syncSubscriptions();
    } else if (nextState === "error" && previous !== "error") {
      toast(message, "error");
    } else if (nextState === "disconnected" && reconnecting && previous !== "disconnected") {
      toast(message, "warning");
    }
  }

  function sendEvent(eventName, payload, timeout = 15000) {
    return new Promise((resolve, reject) => {
      if (!socket || !socket.connected) {
        reject(new Error("Chưa kết nối tới máy chủ."));
        return;
      }
      socket.timeout(timeout).emit(eventName, payload, (error, response) => {
        if (error) {
          reject(new Error("Máy chủ không phản hồi kịp."));
          return;
        }
        if (!response || typeof response !== "object") {
          reject(new Error("Phản hồi không hợp lệ từ máy chủ."));
          return;
        }
        resolve(response);
      });
    });
  }

  // -------------------------------------------------------------------------
  // Toasts
  // -------------------------------------------------------------------------
  function toast(message, type = "info") {
    const text = String(message || "").trim();
    if (!text) {
      return;
    }
    const kind = ["success", "error", "warning", "info"].includes(type) ? type : "info";
    const now = Date.now();
    const key = `${kind}:${text}`;
    if (state.lastToast.key === key && now - state.lastToast.at < 2500) {
      return;
    }
    state.lastToast = { key, at: now };

    const node = document.createElement("div");
    node.className = `toast toast--${kind}`;

    const icon = document.createElement("span");
    icon.className = "toast-icon";
    icon.setAttribute("aria-hidden", "true");
    icon.textContent =
      kind === "success" ? "✓" : kind === "error" || kind === "warning" ? "!" : "i";

    const body = document.createElement("span");
    body.className = "toast-text";
    body.textContent = text;

    const close = document.createElement("button");
    close.type = "button";
    close.className = "toast-close";
    close.setAttribute("aria-label", "Đóng thông báo");
    close.textContent = "×";

    node.append(icon, body, close);
    els.toastContainer.append(node);
    while (els.toastContainer.children.length > 4) {
      els.toastContainer.firstElementChild.remove();
    }

    let timer = window.setTimeout(
      () => dismissToast(node),
      kind === "error" ? 6500 : 4500
    );
    node.addEventListener("mouseenter", () => window.clearTimeout(timer));
    node.addEventListener("mouseleave", () => {
      timer = window.setTimeout(() => dismissToast(node), 2500);
    });
    close.addEventListener("click", () => {
      window.clearTimeout(timer);
      dismissToast(node);
    });
  }

  function dismissToast(node) {
    if (!node.isConnected || node.classList.contains("toast--leaving")) {
      return;
    }
    node.classList.add("toast--leaving");
    window.setTimeout(() => node.remove(), 180);
  }
})();
