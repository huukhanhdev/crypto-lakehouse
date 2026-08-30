# Giải thích Project — Crypto Streaming Lakehouse (tiếng Việt)

Tài liệu này giải thích **project này là gì, để làm gì, và từng mảnh ghép hoạt
động ra sao**, viết cho người đọc muốn hiểu tổng thể trước khi đọc code. Bản
tiếng Anh gốc (đầy đủ, chi tiết kỹ thuật) nằm ở
[`PROJECT_SPEC_crypto.md`](../PROJECT_SPEC_crypto.md).

---

## 1. Project này là gì?

Một **lakehouse dữ liệu near-real-time** (gần thời gian thực) xây trên dữ liệu
thị trường **Binance**. Nói đơn giản: dữ liệu giao dịch crypto (lệnh khớp + sổ
lệnh) chảy vào liên tục qua websocket, được ghi lại, biến đổi qua nhiều tầng, và
cuối cùng hiện lên **dashboard** trực quan.

Đây là **project portfolio** (để trưng bày kỹ năng Data Engineering), chạy trên
**một chiếc laptop**, do một người làm. Phạm vi được giữ **gọn có chủ đích**.

> **Quan trọng:** Đây **KHÔNG** phải bot trading. Không có chiến lược, không dự
> đoán, không đặt lệnh. Nó chỉ là project **kỹ thuật dữ liệu** — phần phân tích
> chỉ mang tính mô tả (khối lượng, chênh lệch giá, biểu đồ nến...). Điểm nhấn là
> **kỹ năng xử lý dữ liệu**, không phải kiếm tiền.

### Tại sao chọn crypto (mà không phải chứng khoán / forex)?

Vì Binance cho dữ liệu websocket **miễn phí, không cần API key, chạy 24/7**. Và
quan trọng nhất: luồng **sổ lệnh (order book)** là một luồng **cập nhật từng
dòng** thật sự (giá tại một mức thay đổi liên tục). Chính điều này làm cho các
kỹ thuật **CDC** và **`MERGE INTO`** trong project trở nên "thật" và có ý nghĩa.
Chứng khoán/forex không có nguồn miễn phí, hợp pháp, luôn bật như vậy.

---

## 2. Bức tranh tổng thể (kiến trúc)

```
Binance WS ─▶ Ingestor ─▶ PostgreSQL ─(logical replication)─▶ Debezium ─▶ Redpanda
(trades +     (Python)      (OLTP)                                          │
 depth)                                                   Spark Structured Streaming
                                                                            │
                                        ┌───────────────────────────────────┤
                                        ▼                                    ▼
                                  Iceberg BRONZE                       Iceberg SILVER
                               (raw CDC, append)                   (trạng thái hiện tại, MERGE)
                                        │
                                        ▼
                                      dbt ─▶ Iceberg GOLD (star schema + tests)
                                                    │
                                                  Trino ─▶ Superset
```

**Dữ liệu đi theo dòng:** Binance → ghi vào Postgres → Debezium bắt thay đổi →
đẩy vào Redpanda → Spark đọc và đổ vào Iceberg (Bronze rồi Silver) → dbt dựng
tầng Gold (mô hình sao + kiểm thử) → Trino truy vấn → Superset vẽ dashboard.

### Mô hình "Medallion" (3 tầng Bronze / Silver / Gold)

Đây là cách tổ chức dữ liệu chuẩn trong lakehouse:

| Tầng | Ý nghĩa | Trong project này |
|---|---|---|
| **Bronze** | Dữ liệu thô, giữ nguyên, chỉ thêm (append) | Bản ghi CDC nguyên gốc từ Debezium |
| **Silver** | Dữ liệu đã làm sạch, **trạng thái hiện tại** | Sổ lệnh hiện tại (dùng `MERGE INTO`) |
| **Gold** | Dữ liệu sẵn sàng cho phân tích/BI | Mô hình sao: dim + fact, có test |

---

## 3. Giải thích từng thành phần (và tại sao chọn nó)

| Thành phần | Công nghệ | Vai trò |
|---|---|---|
| Nguồn dữ liệu | Binance websocket | Luồng `@trade` (lệnh khớp) + `@depth` (sổ lệnh), miễn phí, không key |
| Ingestor | Python (`websockets` + `psycopg2`) | Nhận websocket, ghi vào Postgres |
| OLTP nguồn | PostgreSQL 16 | "Nguồn sự thật" để CDC bắt thay đổi |
| CDC | Debezium 2.7 (pgoutput) | Bắt mọi thay đổi trong Postgres (insert/update/delete) |
| Log/broker | Redpanda | Giống Kafka nhưng nhẹ, không cần ZooKeeper |
| Kho object | MinIO | Đóng vai S3 chạy local |
| Định dạng bảng | Apache Iceberg | Cho phép update/delete **từng dòng** — lý do cốt lõi |
| Catalog | Nessie | Quản lý metadata bảng (có nhánh kiểu git) |
| Tính toán stream | Spark Structured Streaming | Dựng Bronze + Silver |
| Biến đổi | dbt-trino | Dựng tầng Gold |
| Query engine | Trino | Truy vấn Gold phục vụ BI |
| BI | Superset | Vẽ dashboard |
| Điều phối | Dagster | Chạy các job Spark + dbt theo thứ tự |

### Hai khái niệm khó nhất, giải thích dễ hiểu

**CDC (Change Data Capture)** — "bắt thay đổi dữ liệu".
Thay vì thỉnh thoảng quét lại cả bảng, Debezium **đọc nhật ký giao dịch (WAL)**
của Postgres và phát ra một sự kiện mỗi khi có dòng bị thêm/sửa/xóa. Nhờ đó dữ
liệu chảy xuống lakehouse gần như tức thì và không bỏ sót thay đổi nào.

**`MERGE INTO`** — "gộp vào".
Với sổ lệnh, cùng một mức giá bị cập nhật số lượng liên tục. `MERGE INTO` cho
phép: nếu dòng đã tồn tại thì **UPDATE**, chưa có thì **INSERT**, số lượng về 0
thì **DELETE** — tất cả trong một câu lệnh. Đây là **trái tim kỹ thuật** của
project: bảng Silver luôn phản ánh **sổ lệnh hiện tại** (bounded, không phình
mãi), trong khi Bronze thì cứ lớn dần vì lưu lịch sử. Sự đối lập đó chính là bằng
chứng `MERGE` hoạt động.

---

## 4. Cụ thể trong project: dữ liệu gì được thu?

Ingestor đăng ký **3 cặp** (nhỏ gọn đủ để thể hiện xử lý đa symbol mà không làm
nghẽn laptop): `BTCUSDT`, `ETHUSDT`, `SOLUSDT`.

Hai loại luồng, hai kiểu ghi khác nhau (chính sự khác biệt này là điểm nhấn):

- **`@trade`** — mỗi lệnh khớp một message. **Chỉ thêm (append-only)**, lượng
  rất lớn → chỉ INSERT vào bảng `trades`.
- **`@depth@100ms`** — cập nhật thay đổi của sổ lệnh (các mức giá đổi số lượng).
  **Đây là luồng "insert rồi update rồi delete"** buộc phải dùng `MERGE INTO`.

> **Lưu ý về sổ lệnh:** Binance gửi **diff (chênh lệch)**, không gửi ảnh chụp
> toàn bộ. Nên ingestor phải: lấy **ảnh chụp ban đầu** qua REST, rồi áp các diff
> lên, mỗi mức giá thay đổi thì upsert; mức nào về số lượng 0 thì xóa. Đây đúng
> là mẫu CDC mà lakehouse sẽ phản chiếu ở phía sau. Code tuân theo đúng thuật
> toán "How to manage a local order book correctly" của Binance để sổ lệnh không
> bị lệch.

### Các bảng trong Postgres

- `symbols` — chiều (dimension) chậm thay đổi. Cột `status` (TRADING/BREAK/HALT)
  là "động cơ" cho **SCD Type 2** (lưu lịch sử thay đổi) ở tầng Gold sau này.
- `exchanges` — hiện chỉ 1 dòng (Binance), để mô hình chiều có ý nghĩa & mở rộng.
- `trades` — bảng fact, chỉ thêm, khối lượng lớn.
- `orderbook_levels` — insert rồi update tại chỗ → nguồn của `MERGE INTO`.

---

## 5. Project chia làm mấy giai đoạn?

Xây **từng phase một**, mỗi phase phải chạy được và chứng minh được rồi mới sang
phase sau (không dựng hết mọi thứ cùng lúc).

- **Phase 0** — Ingestor Binance, schema Postgres, Debezium, Redpanda. ✅ *(xong)*
- **Phase 1** — MinIO, catalog, Spark, tầng Bronze + Silver (`MERGE INTO` sổ lệnh).
- **Phase 2** — dbt tầng Gold: mô hình sao, `fct_ohlcv_1m` (nến 1 phút), SCD2, test.
- **Phase 3** — Trino, Superset (dashboard), Dagster (điều phối), CI.

**Đích cuối (Definition of Done):** người khác `git clone` → chạy `make up` →
dữ liệu Binance thật chảy tới **biểu đồ nến trên Superset** trên một máy sạch,
chỉ cần làm theo README.

---

## 6. Hiện tại đang ở đâu? (trạng thái Phase 0)

Phase 0 đã **chạy thật** trên Docker:

- ✅ 5 container chạy: `postgres`, `redpanda`, `connect` (Debezium), `ingestor`,
  `console` (giao diện xem Redpanda).
- ✅ Ingestor kết nối Binance, seed 3 symbol, tải snapshot sổ lệnh, đang stream.
- ✅ Debezium connector trạng thái **RUNNING**, đang bắt CDC từ Postgres.

**Cách tự kiểm tra** (chi tiết ở [README](../README.md)):

```bash
make status    # connector phải RUNNING
make psql      # rồi: SELECT count(*) FROM trades;
make trades    # xem message CDC của trades
make book      # xem message CDC của sổ lệnh (thấy insert/update/delete)
```

Hoặc mở **Redpanda Console** ở <http://localhost:8080> để xem topic/message bằng
giao diện.

**Dấu hiệu "chạy đúng":**
- `trades` tăng liên tục; topic `crypto.public.trades` đầy sự kiện `op:"c"`.
- `orderbook_levels` **giữ số dòng bị chặn** (top-N mỗi bên mỗi symbol), còn
  topic `crypto.public.orderbook_levels` hiện đủ `op:"c"` (thêm), `op:"u"`
  (sửa), `op:"d"` (xóa) — đúng mẫu CDC mà Silver sẽ phản chiếu bằng `MERGE`.

---

## 7. Vài quyết định thiết kế đáng chú ý

Toàn bộ sai lệch so với spec được ghi ở [`docs/DECISIONS.md`](DECISIONS.md).
Vài điểm chính:

- **Xây từ đầu:** repo esports mà spec nói tới không có sẵn, nên Phase 0 được
  viết mới hoàn toàn theo §3.
- **Sổ lệnh chỉ lưu top-N mức mỗi bên** (`TRACKED_DEPTH`, mặc định 20): giữ
  **toàn bộ sổ lệnh trong RAM** để tính đúng giá mua/bán tốt nhất, nhưng chỉ ghi
  top-N xuống Postgres — để laptop không bị quá tải, mà vẫn thể hiện đủ
  insert/update/delete.
- **Redpanda Console** thêm vào làm công cụ demo (xem topic bằng giao diện UI), có thể bỏ.
- **SCD2:** `symbols.status` là động cơ SCD2. Thay đổi status thật rất hiếm, nên
  có thêm tùy chọn `DEMO_FLIP_SECS` để tự động đổi status theo chu kỳ khi cần
  demo (mặc định tắt).

---

## 8. Từ điển thuật ngữ nhanh

- **Lakehouse:** kết hợp ưu điểm của data lake (rẻ, linh hoạt) và data warehouse
  (có cấu trúc, truy vấn nhanh).
- **OLTP:** cơ sở dữ liệu giao dịch (như Postgres) — nơi dữ liệu được ghi vào.
- **WAL (Write-Ahead Log):** nhật ký ghi trước của Postgres; CDC đọc từ đây.
- **Iceberg:** định dạng bảng cho phép ACID, update/delete từng dòng trên file
  trong object store.
- **Star schema (mô hình sao):** cách tổ chức kho dữ liệu gồm bảng **fact** (số
  liệu) ở giữa và các bảng **dimension** (mô tả) xung quanh.
- **SCD Type 2:** cách lưu **lịch sử** thay đổi của một chiều (mỗi lần đổi tạo
  một phiên bản mới, đóng phiên bản cũ) thay vì ghi đè.
- **OHLCV:** Open-High-Low-Close-Volume — dữ liệu để vẽ **biểu đồ nến**.
```
