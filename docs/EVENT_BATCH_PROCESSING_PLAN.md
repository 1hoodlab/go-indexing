# Event Batch Processing Plan

## Overview

Khi scan 1 block, có nhiều event/log. Thay vì xử lý từng event tuần tự, sẽ **gộp theo type** và **xử lý theo batch**.

---

## Flow

```
Block scanned
    │
    ▼
Extract all logs from block (filter by contract addresses)
    │
    ▼
Group logs by event_signature (topic0)
    │   └── Transfer(...)  → batch A
    │   └── VoteCast(...) → batch B  
    │   └── VoteCast(...) → batch B (same type, merged)
    │   └── FundsDeposited → batch C
    │   └── ...
    │
    ▼
For each batch (by event type):
    └── Lookup Handler for event_signature
    └── Dispatch batch to Handler (or default)
    └── Handler xử lý (save, transform, notify, ...)
```

---

## Xử lý event_signature

### Nguồn dữ liệu

| Nguồn | Format | Ví dụ |
|-------|--------|-------|
| **Chain (log)** | topic0 = keccak256 hash, 32 bytes hex | `0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef` |
| **Contract schema** | Canonical string | `Transfer(address,address,uint256)` |
| **Handler register** | Canonical string hoặc topic0 | `VoteCast(uint256,address,uint8,uint256,uint256)` |

### Chuẩn hóa (canonical)

- Format: `EventName(type1,type2,...)` — **không có space**, type đúng chuẩn Solidity
- Ví dụ đúng: `address`, `uint256`, `uint8`, `string` (chữ thường)
- Ví dụ sai: `Address`, `Uint256`, `Transfer( address , address , uint256 )`

### Chiến lược xử lý

1. **Lưu trong logs**: Dùng **topic0** (hash hex) — trùng với dữ liệu chain, query nhanh, không cần convert
2. **Group batch**: Dùng **topic0** làm key — log từ chain đã có sẵn topic0
3. **Handler registry**: Cho phép register bằng **canonical string** — dễ đọc, dễ config
   - Khi register `"Transfer(address,address,uint256)"` → compute `keccak256` → lưu map `topic0 → handler`
   - Khi dispatch: log có topic0 → lookup trực tiếp trong map
4. **Filter contract events**: Contract có `events: ["Transfer(...)", "VoteCast(...)"]`
   - Compute topic0 cho từng event (keccak256) → danh sách topic0 cần index
   - Hoặc khi nhận log: nếu topic0 nằm trong danh sách đã hash thì xử lý

### Flow convert

```
Register: "VoteCast(uint256,address,uint8,uint256,uint256)"
    → keccak256 → 0xabc123...
    → registry[0xabc123...] = handler

Log from chain: topics[0] = 0xabc123...
    → lookup registry[0xabc123...] → handler
```

### Lưu ý

- Cùng event signature (cùng params) → cùng topic0 trên mọi chain/contract
- Transfer ERC20 vs Transfer ERC721 đều là `Transfer(address,address,uint256)` → cùng topic0

---

## Chi tiết

### 1. Group theo event type

- **Key**: `event_signature` (topic0) — mỗi event type có 1 topic0 cố định
- **Value**: Danh sách logs cùng type
- Ví dụ: 5 `VoteCast` + 2 `FundsDeposited` → 2 batch: `[5 logs]`, `[2 logs]`

### 2. Handler Registry

- **Handler**: Chứa các event handler đã register theo `event_signature`
- **Key**: `event_signature` (topic0, ví dụ `0x7a9e5e...` hoặc tên `VoteCast`)
- **Value**: Handler function nhận batch logs, xử lý (save DB, transform, notify, ...)
- **Default handler**: Event không có handler register → lưu vào collection `logs` (bulk insert)
- **Register**: `handler.Register("VoteCast", voteCastHandler)` hoặc `Register(topic0Hash, handler)`

```
Handler Registry
├── Transfer(address,address,uint256)  → transferHandler
├── VoteCast(...)                       → voteCastHandler  
├── FundsDeposited(...)                 → fundsDepositedHandler
└── * (default)                        → saveToLogsHandler
```

### 3. Batch dispatch

- Mỗi batch (cùng type) → lookup handler → gọi `Handler.Handle(batch)`
- Handler nhận batch logs, xử lý (insertMany, gửi webhook, ...)
- Giảm round-trip khi handler dùng bulk ops

### 4. Update `last_scanned_block`

- Sau khi xử lý xong block, update `last_scanned_block` cho các contract có log trong block
- Gộp theo `contract_id` → 1 update per contract (hoặc bulk update)

---

## Lợi ích

| Trước (tuần tự) | Sau (batch) |
|-----------------|-------------|
| N insertOne cho N events | vài insertMany (1 per event type) |
| N round-trip DB | vài round-trip |
| Chậm khi block nhiều event | Nhanh hơn |

---

## Thứ tự xử lý trong block

- **Trong 1 block**: Giữ thứ tự logs (theo `log_index`) khi group
- **Giữa các batch**: Có thể xử lý song song (mỗi event type 1 goroutine) hoặc tuần tự tùy design
- **last_scanned_block**: Chỉ update khi toàn bộ events của block đã insert xong

---

## Handler Interface (concept)

```
Handler(ctx, batch []Log) error
```

- Input: Batch logs cùng event type
- Output: error nếu xử lý thất bại
- Ví dụ handler: decode ABI → transform → insert vào collection riêng (votes, deposits, ...)
- Default: insert batch vào `logs` collection

---

## Edge cases

- Block không có event → skip insert, vẫn update `last_scanned_block` (contract đã scan qua block đó)
- Nhiều contract trong cùng block → group theo (contract_id, event_signature) hoặc (event_signature) tùy chiến lược query sau
- Event type không có handler register → dùng default handler (save to `logs` collection)
