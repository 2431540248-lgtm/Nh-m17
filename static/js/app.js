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

  initChart();
  initTheme();
  bindConnectionForm();
  bindSubscriptions();
  bindPublish();
  bindMessages();
  updateStatistics();
  window.setInterval(updateMessageRate, 1000);
  connectSocket();
  applyConnectionState("disconnected", DEFAULT_STATUS.disconnected, false);
  renderTopics();
  renderMessages(false);

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
