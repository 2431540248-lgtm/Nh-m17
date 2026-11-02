// Kết nối với Socket.IO Server
const socket = io();

// Lấy các thành phần trên giao diện HTML
const connectionForm = document.getElementById("connection-form");
const disconnectBtn = document.getElementById("disconnect-btn");
const subscribeForm = document.getElementById("subscribe-form");
const unsubscribeBtn = document.getElementById("unsubscribe-btn");
const publishForm = document.getElementById("publish-form");

const statusIndicator = document.getElementById("status-indicator");
const connectionStatus = document.getElementById("connection-status");

const subscribedTopics = document.getElementById("subscribed-topics");
const messageList = document.getElementById("message-list");
const messageCount = document.getElementById("message-count");
const activityLog = document.getElementById("activity-log");
const clearMessagesBtn = document.getElementById("clear-messages");

// Lưu danh sách Topic và tin nhắn nhận được
let topics = [];
let messages = [];
let connected = false;

// Cập nhật trạng thái kết nối trên giao diện
function updateStatus(state, message) {
    connectionStatus.textContent = state;
    statusIndicator.className = "status-dot";

    if (state === "connected") {
        statusIndicator.classList.add("connected");
        connected = true;
    } else if (state === "connecting") {
        statusIndicator.classList.add("connecting");
        connected = false;
    } else {
        connected = false;
    }

    addLog(message);
}

// Thêm thông tin vào lịch sử hoạt động
function addLog(message) {
    if (activityLog.querySelector(".empty-state")) {
        activityLog.innerHTML = "";
    }

    const item = document.createElement("p");
    const time = new Date().toLocaleTimeString();

    item.textContent = `[${time}] ${message}`;
    activityLog.prepend(item);
}

// Hiển thị danh sách Topic đã đăng ký
function showTopics() {
    subscribedTopics.innerHTML = "";

    if (topics.length === 0) {
        subscribedTopics.innerHTML =
            '<li class="empty-state">No topics subscribed</li>';
        return;
    }

    topics.forEach(topic => {
        const item = document.createElement("li");
        item.textContent = topic;
        subscribedTopics.appendChild(item);
    });
}

// Hiển thị các tin nhắn MQTT nhận được
function showMessages() {
    messageList.innerHTML = "";

    if (messages.length === 0) {
        messageList.innerHTML = `
            <tr>
                <td colspan="5" class="empty-state">
                    Waiting for MQTT messages...
                </td>
            </tr>
        `;
    } else {
        messages.forEach(message => {
            const row = document.createElement("tr");

            const values = [
                new Date(message.timestamp).toLocaleTimeString(),
                message.topic,
                message.payload,
                message.qos,
                message.retain ? "Yes" : "No"
            ];

            values.forEach(value => {
                const cell = document.createElement("td");
                cell.textContent = value;
                row.appendChild(cell);
            });

            messageList.appendChild(row);
        });
    }

    messageCount.textContent = `${messages.length} messages`;
}

// Khi trình duyệt kết nối được với Server
socket.on("connect", () => {
    addLog("Đã kết nối tới Dashboard Server.");
});

// Khi mất kết nối với Server
socket.on("disconnect", () => {
    updateStatus("disconnected", "Mất kết nối tới Dashboard Server.");
});

// Nhận trạng thái từ MQTT Broker gửi về
socket.on("status", data => {
    const state = data.state || "disconnected";

    updateStatus(state, data.message);

    if (state === "disconnected" && !data.reconnecting) {
        connected = false;
    }
});

// Nhận tin nhắn MQTT từ Backend
socket.on("mqtt_message", data => {
    messages.unshift(data);

    if (messages.length > 200) {
        messages.pop();
    }

    showMessages();
});

// Xử lý chức năng kết nối MQTT Broker
connectionForm.addEventListener("submit", event => {
    event.preventDefault();

    const host = document.getElementById("broker-host").value.trim();
    const port = document.getElementById("broker-port").value;
    const username = document.getElementById("broker-username").value.trim();
    const password = document.getElementById("broker-password").value;
    const useTLS = document.getElementById("use-tls").checked;

    if (!host) {
        alert("Vui lòng nhập địa chỉ Broker.");
        return;
    }

    socket.emit("connect_broker", {
        host: host,
        port: port,
        username: username,
        password: password || null,
        use_tls: useTLS
    }, response => {
        if (response && !response.ok) {
            addLog(response.error);
        }
    });
});

// Xử lý chức năng ngắt kết nối Broker
disconnectBtn.addEventListener("click", () => {
    socket.emit("disconnect_broker", {}, response => {
        if (response && response.ok) {
            topics = [];
            showTopics();
            updateStatus("disconnected", response.message);
        }
    });
});

// Xử lý chức năng đăng ký Topic
subscribeForm.addEventListener("submit", event => {
    event.preventDefault();

    const topic = document.getElementById("subscribe-topic").value.trim();

    if (!topic) {
        alert("Vui lòng nhập Topic.");
        return;
    }

    socket.emit("subscribe", { topic: topic }, response => {
        if (response && response.ok) {
            if (!topics.includes(topic)) {
                topics.push(topic);
                showTopics();
            }

            addLog(`Đã đăng ký Topic: ${topic}`);
        } else if (response) {
            addLog(response.error);
        }
    });
});

// Xử lý chức năng hủy đăng ký Topic
unsubscribeBtn.addEventListener("click", () => {
    const topic = document.getElementById("subscribe-topic").value.trim();

    if (!topic) {
        alert("Vui lòng nhập Topic cần hủy.");
        return;
    }

    socket.emit("unsubscribe", { topic: topic }, response => {
        if (response && response.ok) {
            topics = topics.filter(item => item !== topic);
            showTopics();
            addLog(`Đã hủy đăng ký Topic: ${topic}`);
        } else if (response) {
            addLog(response.error);
        }
    });
});

// Xử lý chức năng gửi dữ liệu bằng Publish
publishForm.addEventListener("submit", event => {
    event.preventDefault();

    const topic = document.getElementById("publish-topic").value.trim();
    const payload = document.getElementById("publish-payload").value;
    const qos = Number(document.getElementById("publish-qos").value);
    const retain = document.getElementById("publish-retain").checked;

    if (!topic) {
        alert("Vui lòng nhập Topic.");
        return;
    }

    socket.emit("publish", {
        topic: topic,
        payload: payload,
        qos: qos,
        retain: retain
    }, response => {
        if (response && response.ok) {
            addLog(`Đã gửi dữ liệu tới Topic: ${topic}`);
        } else if (response) {
            addLog(response.error);
        }
    });
});

// Xóa toàn bộ tin nhắn đang hiển thị
clearMessagesBtn.addEventListener("click", () => {
    messages = [];
    showMessages();
});

// Hiển thị trạng thái ban đầu khi mở trang web
updateStatus("disconnected", "Dashboard đã sẵn sàng.");
showTopics();
showMessages();
