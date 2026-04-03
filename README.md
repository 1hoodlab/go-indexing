# Go Indexing

Indexer sự kiện on-chain đa chuỗi. Hiện hỗ trợ **EVM** (Ethereum, BSC, Base, Sepolia…). Các loại chain khác có thể bổ sung sau qua `CHAIN_TYPE`.

## Mô hình dữ liệu (quan trọng khi multi-chain)

- **Một process indexer** chỉ làm việc với **một MongoDB database** (`MONGODB_DATABASE`).
- Trong database đó, indexer đọc **một network `status: active`** và các **contract `status: active`** để biết RPC và contract cần index.
- **Tách chain bằng tên database**: mỗi chain (hoặc môi trường) nên dùng một `MONGODB_DATABASE` riêng (ví dụ `indexing_base`, `indexing_sepolia`). Cùng một cụm MongoDB, khác tên DB → dữ liệu không lẫn.
- **RPC** lấy từ MongoDB (`network.rpc_urls` sau khi seed), **không** cấu hình RPC trong env (trừ `MONGODB_URI`).

## Yêu cầu

- Go 1.25+
- Docker (để chạy MongoDB local)
- File `.env` (tùy chọn): copy từ `.env.example`

## Cách dùng nhanh (một chain)

### 1. Chạy MongoDB

```bash
docker compose up -d
```

### 2. Seed dữ liệu (network + contracts)

**Luôn dùng cùng `MONGODB_DATABASE` với bước chạy indexer.**

Ví dụ Sepolia:

```bash
# Linux / macOS
MONGODB_DATABASE=indexing_sepolia go run cmd/seed/main.go --config cmd/seed/data/sepolia.json
```

```powershell
# Windows PowerShell
$env:MONGODB_DATABASE="indexing_sepolia"
go run cmd/seed/main.go --config cmd/seed/data/sepolia.json
```

Có sẵn thêm `cmd/seed/data/base.json` (Base mainnet).

### 3. Chạy indexer

```bash
# Linux / macOS — khớp DB với bước seed
CHAIN_TYPE=evm \
CHAIN_NAME=sepolia \
MONGODB_DATABASE=indexing_sepolia \
go run cmd/indexer/main.go
```

```powershell
# Windows PowerShell
$env:CHAIN_TYPE="evm"
$env:CHAIN_NAME="sepolia"
$env:MONGODB_DATABASE="indexing_sepolia"
go run cmd/indexer/main.go
```

Hoặc đặt các biến trong file `.env` rồi chạy `go run cmd/indexer/main.go` (ứng dụng đọc `.env` qua `godotenv`).

### Lỗi thường gặp

| Thông báo | Nguyên nhân / cách xử lý |
|-----------|---------------------------|
| `no active contracts to index` | Chưa seed, hoặc `MONGODB_DATABASE` indexer khác DB đã seed, hoặc không có contract `active`. |
| Không kết nối được MongoDB | Kiểm tra `MONGODB_URI`, MongoDB đã `docker compose up`. |

## Biến môi trường

| Biến | Mặc định | Mô tả |
|------|----------|--------|
| `CHAIN_TYPE` | `evm` | Loại chain (hiện tại: `evm`). |
| `CHAIN_NAME` | cùng `CHAIN_TYPE` | Tên hiển thị trong log (ví dụ `base`, `sepolia`). |
| `POLL_INTERVAL` | theo `CHAIN_TYPE` | Chu kỳ poll block (ví dụ `12s`, `2s`, `400ms`). Ghi đè nếu set. |
| `MONGODB_URI` | `mongodb://localhost:27017` | Chuỗi kết nối MongoDB. |
| `MONGODB_DATABASE` | `indexing` | **Tên database** — phải trùng giữa seed và indexer. |

**Poll mặc định theo `CHAIN_TYPE`:** EVM `12s`, Solana `400ms`, Tron `3s`, Sui `1s` (các loại sau khi được triển khai).

Indexer dùng **RPC pool**: lần lượt thử các URL trong `network.rpc_urls` khi lỗi.

## Triển khai multi-chain

Ý tưởng: **mỗi chain = một database MongoDB + một tiến trình (hoặc một container) indexer** với env tương ứng.

1. **Chuẩn bị seed**  
   - Mỗi chain một file JSON trong `cmd/seed/data/` (đã có ví dụ `sepolia.json`, `base.json`).  
   - `network.chain_id` phải đúng chain; `contracts` khai báo `address`, `abi`, `events`, `start_block`.

2. **Seed từng chain vào DB riêng**

   ```bash
   MONGODB_DATABASE=indexing_base go run cmd/seed/main.go --config cmd/seed/data/base.json
   MONGODB_DATABASE=indexing_sepolia go run cmd/seed/main.go --config cmd/seed/data/sepolia.json
   ```

3. **Chạy một indexer cho mỗi chain** (terminal riêng, systemd unit riêng, hoặc replica Kubernetes/Docker):

   ```bash
   # Terminal 1 — Base
   CHAIN_TYPE=evm CHAIN_NAME=base MONGODB_DATABASE=indexing_base POLL_INTERVAL=2s go run cmd/indexer/main.go

   # Terminal 2 — Sepolia
   CHAIN_TYPE=evm CHAIN_NAME=sepolia MONGODB_DATABASE=indexing_sepolia go run cmd/indexer/main.go
   ```

4. **Production**  
   - Build một binary: `go build -o indexer ./cmd/indexer`.  
   - Deploy N bản copy với N file env hoặc N biến môi trường (mỗi service một `MONGODB_DATABASE` + `CHAIN_NAME` + `POLL_INTERVAL` nếu cần).  
   - Một MongoDB cluster có thể host nhiều database; backup/scale theo từng `MONGODB_DATABASE` nếu cần.

**Không** cần đổi code để thêm chain EVM mới: thêm JSON seed + process mới với `MONGODB_DATABASE` riêng.

## Thêm chain / contract mới

1. Tạo `cmd/seed/data/<chain>.json` theo mẫu:

```json
{
  "network": {
    "name": "Tên hiển thị",
    "chain_type": "evm",
    "chain_id": 1,
    "rpc_urls": ["https://..."],
    "type": "mainnet",
    "status": "active"
  },
  "contracts": [
    {
      "address": "0x...",
      "abi": [],
      "events": ["Transfer(address,address,uint256)"],
      "start_block": 12345678
    }
  ]
}
```

2. Seed với đúng `MONGODB_DATABASE`.  
3. Chạy indexer với cùng `MONGODB_DATABASE`.

Event cần index được khai báo trong `events` (chuỗi canonical giống Solidity). Handler mặc định lưu raw log và decode theo ABI vào collection `decoded_events`.

## Cấu trúc thư mục

```
cmd/
  indexer/          # Entry point indexer
  seed/             # Seed MongoDB
    data/           # JSON theo từng chain
internal/
  core/             # Interface, Registry, kiểu chung
  chains/evm/       # RPC pool, subscriber, fetcher, storage EVM
  config/           # Đọc env
  handlers/         # Handler mặc định (lưu log + decode)
  pipeline/         # Pipeline index block
  models/
  storage/          # MongoDB (nếu dùng)
pkg/utils/          # Decode ABI, topic0, ...
```

## Test

```bash
go test ./... -v
```
