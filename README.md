# LAN Monitor over MQTT

LAN Monitor over MQTT giám sát các thiết bị trong mạng LAN bằng cách ping, đo độ trễ, quét cổng TCP và phát hiện thiết bị lạ qua ARP, rồi gửi kết quả lên dashboard theo thời gian thực qua MQTT. Công cụ dành cho quản trị mạng nhỏ ở phòng máy, văn phòng hoặc lớp học, nếu cần một cách đơn giản để biết thiết bị nào đang online, mạng đang chậm hay có thiết bị lạ xuất hiện.

## Tính năng

- Agent ping ICMP từng thiết bị, đo độ trễ (ms) và tỉ lệ mất gói; thử TCP connect các cổng đã khai báo.
- Quét ARP định kỳ, so với baseline MAC để phát hiện thiết bị lạ (`new_device`).
- Last Will and Testament: nếu agent mất kết nối đột ngột, broker sẽ phát trạng thái `offline`.
- Dashboard Flask + SocketIO cập nhật realtime với 4 trạng thái: Online, Offline, Mất tín hiệu (stale) và Agent offline.
- Biểu đồ độ trễ theo thời gian; thống kê uptime, trung bình, min, max, P95, số lần mất kết nối và số mẫu.
- Xuất CSV cho tin nhắn, nhật ký cảnh báo và thống kê thiết bị.
- Chế độ `--simulate` tạo dữ liệu giả để chạy thử không cần chạm mạng thật.

## Kiến trúc

```text
 network_agent.py                 MQTT broker                app.py (Flask)
 ping / TCP / ARP  ──publish──▶  broker.hivemq.com  ──subscribe──▶  + SocketIO
        ▲                        <prefix>/#                         │
        └── Last Will: status=offline                               │ emit
                                                                    ▼
                                                          Trình duyệt / dashboard
```

Các topic (với `prefix` mặc định là `lanmon`, `agent` là tên agent):

| Topic | Retain | QoS | Nội dung |
| --- | --- | --- | --- |
| `<prefix>/<agent>/status` | có | 1 | Trạng thái agent `online`/`offline`, chu kỳ, số thiết bị, phiên bản; cũng là Last Will. |
| `<prefix>/<agent>/device/<id>` | có | 0 | Kết quả kiểm tra từng thiết bị: ping, độ trễ, mất gói, cổng, MAC. |
| `<prefix>/<agent>/event` | không | 1 | Sự kiện: `agent_started`, `device_up`, `device_down`, `new_device`, `port_opened`, `port_closed`. |

Ví dụ payload topic `device` (lấy đúng từ `build_device_payload()` trong `network_agent.py`):

```json
{"agent":"pc-lab","id":"192.168.1.1","host":"192.168.1.1","name":"Router","online":true,"latency_ms":12.3,"loss_pct":0.0,"ports":{"80":true,"443":true},"mac":null,"known":true,"ts":"2026-11-23T08:15:30.123Z"}
```

## Cài đặt (Windows PowerShell)

```powershell
git clone <URL-kho-repo> mqtt-dashboard
cd mqtt-dashboard
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Nếu PowerShell chặn kích hoạt venv, chạy `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` rồi thử lại.

## Cách chạy

**1. Khởi động dashboard** (cửa sổ 1):

```powershell
python app.py
```

Mở http://127.0.0.1:5000 trong trình duyệt. Đường dẫn kiểm tra sức khỏe: http://127.0.0.1:5000/healthz.

**2. Khởi động agent** (cửa sổ 2, sau khi đã kích hoạt venv). Các ví dụ lấy từ `python network_agent.py --help`:

```powershell
# Dữ liệu giả, chạy đúng 60 chu kỳ rồi thoát
python network_agent.py --simulate --count 60

# Đọc danh sách thiết bị từ file JSON
python network_agent.py --config targets.example.json

# Danh sách host phân cách bằng dấu phẩy (KHÔNG phải file JSON)
python network_agent.py --targets 192.168.1.1,8.8.8.8 --ports 22,80 --count 3

# Tự đoán mạng /24, học baseline MAC từ lần quét đầu
python network_agent.py --discover auto --learn --baseline-file baseline.json
```

`--targets` nhận danh sách IP/host cách nhau bằng dấu phẩy; muốn dùng file JSON thì dùng `--config`. `--discover auto` chỉ đoán mạng private `/24`; `--learn` sẽ ghi đè baseline ngay trong lần quét đầu. Agent và dashboard phải dùng cùng một prefix (mặc định `lanmon`).

**3. Trên dashboard**: nhập broker `broker.hivemq.com`, cổng `1883`, bấm **Connect**; trong ô "Prefix LAN Monitor" nhập `lanmon` (hoặc prefix đã đặt cho agent), rồi bấm **Theo dõi LAN Monitor**.

### Tham số agent

Đây là toàn bộ tham số của `network_agent.py` (`python network_agent.py --help`), kèm giá trị mặc định thực tế sau khi áp dụng cấu hình:

| Tham số | Ý nghĩa | Mặc định |
| --- | --- | --- |
| `--host` | Địa chỉ MQTT broker | `broker.hivemq.com` |
| `--port` | Cổng MQTT | `1883` (`8883` khi dùng `--tls`) |
| `--tls` | Bật TLS | tắt |
| `--username`, `--password` | Xác thực broker | không |
| `--prefix` | Prefix topic MQTT | `lanmon` |
| `--agent-id` | Mã agent trong topic | tên máy (`socket.gethostname()`) |
| `--config` | File JSON cấu hình | không |
| `--targets` | Danh sách host, phân cách bằng dấu phẩy | không (bắt buộc nếu không có `--config`, `--discover` hoặc `--simulate`) |
| `--ports` | Cổng áp dụng cho các host trong `--targets` | rỗng |
| `--interval` | Chu kỳ kiểm tra, giây (tối thiểu 2) | `5` |
| `--fail-threshold` | Số lần thất bại liên tiếp trước khi báo down | `3` |
| `--ping-timeout` | Timeout ping, giây | `1.0` |
| `--port-timeout` | Timeout TCP connect, giây | `0.5` |
| `--discover` | CIDR private IPv4 hoặc `auto` để đoán mạng `/24` | không |
| `--discover-every` | Khoảng quét lại mạng, giây | `60` |
| `--baseline-file` | File baseline MAC | `baseline.json` |
| `--learn` | Ghi lại baseline từ lần quét đầu (ghi đè file cũ) | tắt |
| `--simulate` | Tạo dữ liệu giả, không kiểm tra mạng thật | tắt |
| `--count` | Số chu kỳ; `0` = chạy liên tục | `0` |
| `--verbose` | Bật log chi tiết | tắt |

## Lưu ý trên Windows

Windows mặc định chặn ping ICMP đến, nên agent có thể báo mọi thiết bị là offline. Mở PowerShell với quyền Administrator và cho phép ICMPv4:

```powershell
netsh advfirewall firewall add rule name="Allow ICMPv4 In" protocol=icmpv4:8,any dir=in action=allow
```

Nếu dùng tính năng quét cổng, hãy mở tường lửa cho các cổng TCP cần kiểm tra (hoặc cho phép kết nối tới cổng dịch vụ tương ứng), nếu không các cổng sẽ luôn hiện là đóng.

## Bảo mật và đạo đức

- Broker công cộng như `broker.hivemq.com` ai cũng đọc được: hãy dùng prefix riêng, khó đoán cho nhóm của bạn.
- Chỉ ping, quét cổng và quét ARP trên thiết bị và mạng của chính mình; không thử nghiệm trên mạng tổ chức khác.
- Khi triển khai thật, dùng broker riêng có username/password và bật TLS (`--tls` ở agent, ô TLS trên dashboard).
- `baseline.json` chứa địa chỉ MAC thật của thiết bị trong mạng; đã nằm trong `.gitignore`, không commit file này.

## Giới hạn hiện tại

- Trạng thái thiết bị, agent và thống kê chỉ lưu trong bộ nhớ trình duyệt/phía server; tải lại trang sẽ mất lịch sử.
- Mỗi lần theo dõi chỉ xử lý một prefix; đổi prefix thì phải bấm lại "Theo dõi LAN Monitor".
- Tính năng phát hiện thiết bị/quét mạng chỉ hỗ trợ IPv4; dashboard chưa có đăng nhập hay phân quyền.
- `SECRET_KEY` có giá trị mặc định trong `app.py`; khi triển khai thật phải đặt biến môi trường `SECRET_KEY` khác đi.
- Việc phát hiện thiết bị lạ bằng ARP mới chỉ chạy ở chế độ mô phỏng, chưa được thử trên mạng thật.

## Ảnh chụp màn hình

![Giao diện dashboard LAN Monitor](docs/screenshot.png)

*Hình 1. Giao diện dashboard khi đang theo dõi các thiết bị. Nhóm sẽ chèn ảnh chụp thật vào `docs/screenshot.png`; hiện thư mục `docs/` chỉ có file giữ chỗ.*

## Hướng phát triển

- Gửi cảnh báo qua Telegram hoặc email khi có thiết bị down hoặc thiết bị lạ.
- Lưu lịch sử trạng thái và thống kê vào cơ sở dữ liệu thay vì bộ nhớ.
- Thêm đăng nhập, phân quyền và bắt buộc TLS khi triển khai thật.
- Hỗ trợ nhiều prefix/agent trên cùng một dashboard và mở rộng sang IPv6.
- Bổ sung thu thập thông tin thiết bị qua SNMP.

## Thành viên và phân công

| Thành viên | Giai đoạn | Nội dung |
| --- | --- | --- |
| [TÊN] | Giai đoạn 1 | Agent giám sát và hợp đồng MQTT |
| [TÊN] | Giai đoạn 2 | Dashboard thiết bị |
| [TÊN] | Giai đoạn 3 | Biểu đồ, thống kê và xuất CSV |

## Giấy phép

Dự án phát hành theo giấy phép [MIT](LICENSE).
