# Order Tracking API: ร้านเติมเกม

ระบบติดตามออเดอร์และจัดการเคสปัญหา ใช้ **Go + Fiber + GORM + PostgreSQL**
ระบบนี้ออกแบบให้รับโหลดพร้อมกันสูง (high concurrency) ได้ และรันหลาย replica พร้อมกันได้โดยไม่ต้องใช้ Redis หรือ distributed lock เพิ่ม

---

## 1. Quick Start (คำสั่งเดียว)

```bash
docker-compose up -d --build
# API: http://localhost:8080   (มี seed data 10 ออเดอร์ ครบทุกสถานะ, user_id = user-001)
```

รัน test (unit + integration กับ Postgres จริง):

```bash
docker compose --profile test run --rm test
# หรือ local:  go test ./...                       (unit อย่างเดียว; integration จะ skip)
#             TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/orders_test?sslmode=disable go test ./...
```

ทดสอบ API ด้วยมือได้ที่ [`requests.http`](requests.http) (VS Code REST Client / JetBrains) ซึ่งครอบคลุมทุก endpoint และทั้ง 5 scenario

> [!TIP]
> ถ้าต้องการขับ bot เองผ่าน `/bot/*` (scenario 2, 4) หรือดู report-issue escalation ของออเดอร์ seed 900010 ให้ตั้ง `BOT_WORKERS: "0"` ใน `docker-compose.yml` ก่อน เพราะถ้าไม่ตั้ง bot ในตัวจะหยิบออเดอร์ QUEUED ไปทำจนเสร็จภายในไม่กี่วินาที

---

## 2. Architecture

```
cmd/server/            composition root (config, seed, wiring, graceful shutdown)
internal/
  domain/              Entities, State Machine, Ports (Repository, GameProvider, PaymentGateway)
  usecase/             Business rules: order, bot coordination, webhook, maintenance jobs
  repository/          GORM/PostgreSQL adapter (SKIP LOCKED, CAS, ON CONFLICT), migrations
  handler/             Fiber HTTP delivery, error mapping, structured access log
  worker/              Scheduler (cron) + simulated payment Bots (heartbeat)
  provider/            Mock game provider / bank gateway (พฤติกรรมขึ้นกับ prefix ของ UID)
  apperror/            รูปแบบ error กลาง { code, message, details? }
```

ทิศทางของ dependency: `handler/worker → usecase → domain ← repository/provider` โดย domain ไม่ import อะไรจาก layer นอกเลย

```mermaid
flowchart LR
  Client -->|REST| API[Fiber Handlers]
  Bank -->|HMAC webhook| API
  API --> UC[Usecase / OrderService]
  Bots[Bot workers xN] --> UC
  Cron[Scheduler] --> UC
  UC --> Repo[(PostgreSQL)]
  Bots --> Provider[Game Provider]
```

### เหตุผลที่เลือก Tech Stack
| เลือก | เหตุผล |
|---|---|
| **Go** | goroutine เหมาะกับงานแบบ worker/bot/scheduler, ใช้หน่วยความจำน้อย, compile ออกมาเป็น static binary ไฟล์เดียว (image ~15MB distroless) |
| **Fiber** | เร็ว (ใช้ fasthttp) และ API คล้าย Express ทำให้อ่านง่าย (ตั้ง `Immutable: true` เพื่อกันบั๊กจากการ reuse buffer ของ fasthttp) |
| **GORM** | รองรับ `clause.Locking` (`FOR UPDATE SKIP LOCKED`), `OnConflict` และ `TranslateError` ครบทุกอย่างที่ระบบนี้ต้องใช้ *(TypeORM/Prisma ที่ระบุมาในโจทย์เป็น ORM ของ Node.js ใช้กับ Go ไม่ได้ จึงใช้ GORM ตาม stack ที่กำหนด)* |
| **PostgreSQL** | row-level lock, `SKIP LOCKED`, partial index, CHECK constraint และ transaction แบบ ACID ทำให้**ฐานข้อมูลเป็นตัวการันตีความถูกต้อง**โดยไม่ต้องพึ่ง Redis หรือ distributed lock เพิ่ม |
| **Native `go test`** | ไม่ต้องลง dependency เพิ่ม และรัน integration test กับ Postgres จริง เพราะพฤติกรรมของ lock จำลองด้วย mock ไม่ได้ |

**Structured logging** ใช้ `log/slog` เขียนเป็น JSON และใส่ `orderNo` ในทุก log ที่เกี่ยวกับออเดอร์ (access log, state change, alert) ส่วน alert จะมี field `"alert": true` ไว้ให้ระบบ log aggregator ตั้ง rule จับได้

---

## 3. State Machine

```mermaid
stateDiagram-v2
  [*] --> PENDING_PAYMENT: POST /orders
  PENDING_PAYMENT --> QUEUED: webhook payment.succeeded
  PENDING_PAYMENT --> EXPIRED: scheduler (QR > 15m)
  QUEUED --> IN_PROGRESS: bot claim (SKIP LOCKED)
  QUEUED --> DELAYED: customer report (stuck)
  QUEUED --> REFUNDED
  IN_PROGRESS --> COMPLETED: bot complete (+payment row)
  IN_PROGRESS --> QUEUED: transient error / lease expired (backoff)
  IN_PROGRESS --> DELAYED: retries exhausted (alert)
  IN_PROGRESS --> FAILED: permanent error (invalid UID)
  DELAYED --> QUEUED: resolve RETRY
  DELAYED --> REFUNDED: resolve REFUND
  DELAYED --> FAILED
  FAILED --> QUEUED: resolve RETRY (+new UID)
  FAILED --> REFUNDED: resolve REFUND
  EXPIRED --> REFUNDED: late webhook auto-refund
  COMPLETED --> [*]
  REFUNDED --> [*]
```

* กฎการเปลี่ยนสถานะทั้งหมดอยู่ที่เดียวคือ [`statemachine.go`](internal/domain/statemachine.go)
* การเปลี่ยนสถานะ**ทุกครั้ง**ต้องผ่าน `OrderService.transition()` ซึ่งทำ 3 อย่างใน transaction เดียวกัน: (1) เช็ก `CanTransition` (2) `UPDATE ... WHERE id=? AND version=?` (CAS) (3) insert แถวลง `order_timelines`
  ผลคือ timeline กับ status จะไม่มีวันขัดกัน
* สถานะที่ห้ามเกิดเด็ดขาด เช่น `EXPIRED → COMPLETED` หรือ `COMPLETED → REFUNDED` มี test ครอบไว้แล้ว

## 4. Database Schema

| ตาราง | ฟิลด์สำคัญ / constraint |
|---|---|
| `orders` | `order_no` **UNIQUE** + `CHECK (order_no ~ '^ORD-[0-9]+-[0-9]+$')` · `status` CHECK enum · `version` (optimistic lock) · `locked_at`, `locked_by` (lease) · `retry_count`, `max_retries`, `next_attempt_at` (backoff) · `queued_at` (SLA) · `expires_at` (QR) · `idempotency_key` **UNIQUE** · `amount` CHECK > 0 |
| `order_timelines` | `order_id` (idx), `from_status`, `to_status`, `reason`, `metadata JSONB`, `created_at` |
| `payment_transactions` | `order_id` **UNIQUE**: ชั้นป้องกันสุดท้ายของ double payment |
| `webhook_events` | `event_id` **PRIMARY KEY**: กันประมวลผล webhook ซ้ำ |

Index: `(status, created_at DESC)` สำหรับหน้า list, `(user_id, status)` สำหรับดูออเดอร์รายผู้ใช้ และ **partial index** `(created_at,id) WHERE status='QUEUED'` ที่ตรงกับ query ตอน claim พอดี ทำให้ query claim ยังเร็วแม้ตาราง orders จะมีหลายล้านแถว

`order_no` = `ORD-YYYYMMDD-NNNNNN` โดยเลขท้ายมาจาก **Postgres SEQUENCE** จึงไม่ซ้ำกันแน่นอน แม้จะรันหลาย replica และไม่ต้องสุ่มแล้วคอยเช็กว่าชนหรือไม่

---

## 5. API

Error ทุกตัวใช้รูปแบบเดียวกัน: `{ "code": "ORDER_NOT_FOUND", "message": "...", "details": {...} }`

| Method | Path | หมายเหตุ |
|---|---|---|
| POST | `/orders` | ต้องส่ง header `Idempotency-Key` · สร้างใหม่ตอบ **201** · ส่ง key เดิม payload เดิมตอบ **200** พร้อม header `Idempotent-Replayed: true` และออเดอร์เดิม · key เดิมแต่ payload ต่างตอบ **422** |
| GET | `/orders/:orderNo` | format ผิดตอบ 400 `INVALID_ORDER_NO`, หาไม่เจอตอบ 404 |
| GET | `/orders?tab=current\|history&page&limit[&user_id]` | เรียง `created_at DESC`, limit ≤ 100 |
| GET | `/orders/:orderNo/timeline` | ประวัติทุก transition พร้อม reason และ metadata |
| POST | `/orders/:orderNo/report-issue` | บันทึกลง timeline เสมอ และเปลี่ยนเป็น DELAYED ถ้าเข้าเงื่อนไข (ดูข้อ 5 ในหัวข้อถัดไป) |
| PATCH | `/orders/:orderNo/resolve` | `{action: RETRY\|REFUND, game_uid?}` ใช้ได้กับ FAILED/DELAYED เท่านั้น · เช็ก `X-User-ID` ว่าเป็นเจ้าของออเดอร์ |
| POST | `/webhooks/payment` | header `X-Signature` = hex(HMAC-SHA256(secret, body)) |
| POST | `/bot/claim`, `/bot/orders/:no/heartbeat\|complete\|fail` | API ให้ bot ภายนอกเรียก (bot ในตัวก็ใช้ usecase ชุดเดียวกัน) |
| POST | `/admin/maintenance/run` | สั่ง scheduler ให้รัน 1 รอบทันที (ใช้ตอน demo) |

---

## 6. บทวิเคราะห์และแนวทางแก้ปัญหา 5 ข้อ

### ปัญหา 1: Bot จ่ายเงินไม่ทำงาน / ค้าง / ล่ม
**ต้นเหตุ:** ตอน bot เคลมงานแล้ว process ตาย (OOM, deploy, network partition) ออเดอร์จะติดอยู่ที่ `IN_PROGRESS` ไปตลอด เพราะไม่มีใครเปลี่ยนสถานะให้

**วิธีแก้: Lease + Heartbeat**
1. ตอน claim ระบบจะเซ็ต `locked_by=botId` และ `locked_at=now()` ซึ่งถือเป็น lease ของ bot ตัวนั้น
2. ระหว่างทำงาน bot ต้องเรียก heartbeat ทุก `BOT_HEARTBEAT_INTERVAL` (30s) เพื่ออัปเดต `locked_at` โดย query จะมี `WHERE locked_by = me AND status = IN_PROGRESS` กำกับเสมอ
3. Scheduler (`ReapStaleLeases`) หาออเดอร์ที่ `locked_at < now - LEASE_TIMEOUT (3m)` แล้ว `SELECT ... FOR UPDATE` เพื่อ**เช็กเงื่อนไขซ้ำ**อีกรอบ (เผื่อ bot เพิ่งส่ง heartbeat มาทัน) จากนั้นจัดการเหมือน transient error (`BOT_LEASE_EXPIRED`)
4. Retry ใช้ **exponential backoff** `30s → 60s → 120s` โดยเก็บเวลาไว้ใน `next_attempt_at` ซึ่ง query claim จะข้ามออเดอร์ที่ยังไม่ถึงเวลา เมื่อครบ `max_retries=3` แล้วยังไม่สำเร็จ ออเดอร์จะไป `DELAYED` และเขียน log `ALERT ...` (`alert=true`)
5. **Fencing:** ถ้า bot ที่ "ตายไปแล้ว" ฟื้นกลับมา heartbeat หรือ complete จะถูกปฏิเสธด้วย `LEASE_LOST` เพราะ `locked_by` ไม่ใช่ตัวมันแล้ว และเมื่อ heartbeat ล้มเหลว bot ในตัวจะ cancel context ของงานทันที

*ทำไมไม่ใช้ advisory lock หรือ Redis lock:* lease ที่เก็บเป็นคอลัมน์ใน DB จะอยู่ใน transaction เดียวกับการเปลี่ยน state ตรวจสอบย้อนหลังได้ และไม่ต้องเพิ่ม infrastructure

### ปัญหา 2: Bot แย่งงานกัน (Race Condition / Double Payment)
ระบบป้องกันไว้ 3 ชั้น (defense in depth):
1. **`SELECT ... FOR UPDATE SKIP LOCKED`** ในขั้นตอน claim ([`ClaimNext`](internal/repository/postgres.go)) ทำให้ bot N ตัวที่ query พร้อมกันได้ออเดอร์**คนละแถว**เสมอ ไม่มีตัวไหนต้องรอ lock ของตัวอื่น จึง scale ตามจำนวน bot ได้ ต่างจาก `FOR UPDATE` ธรรมดาที่ bot จะต่อคิวรอกันทั้งหมด
2. **Optimistic locking (`version`)** การอัปเดตทุกครั้งเป็น `WHERE version = ?` ถ้ามีเส้นทางอื่น (เช่น reaper หรือ resolve) แก้แถวเดียวกันไปก่อน ฝั่งที่มาช้าจะได้ `CONCURRENT_MODIFICATION`
3. **`UNIQUE(payment_transactions.order_id)`** ต่อให้ logic ทั้งหมดข้างบนพลาด DB ก็ยังไม่ยอมให้มี payment แถวที่สองของออเดอร์เดียวกัน (`DUPLICATE_PAYMENT` + alert)

นอกจากนี้ `Complete` ยังต้อง lock แถวและเช็กว่า `locked_by == botId` ด้วย

**Test:** `TestClaim_ConcurrentBots_NoDoubleClaim` (bot 10 ตัว × 30 ออเดอร์ ต้องไม่มีออเดอร์ไหนถูกเคลมซ้ำ), `TestComplete_Concurrent_SinglePayment` (เรียก complete พร้อมกัน 10 goroutine ต้องได้ payment แค่ 1 แถว), `TestOptimisticLock_VersionConflict`

### ปัญหา 3: Webhook ธนาคารมาช้า / ซ้ำ / สลับลำดับ
1. **Signature:** ใช้ HMAC-SHA256 กับ raw body และเทียบด้วย `hmac.Equal` (constant-time กัน timing attack) ถ้าไม่ผ่านตอบ 401
2. **Idempotency:** `INSERT INTO webhook_events ... ON CONFLICT (event_id) DO NOTHING` **อยู่ใน transaction เดียวกับการเปลี่ยน state** ดังนั้น "บันทึกแล้ว" แปลว่า "ประมวลผลแล้ว" เสมอ ถ้า process ตายกลางทาง ทั้งสองอย่างจะ rollback ไปพร้อมกัน แล้วธนาคารส่งมาใหม่ก็ยังประมวลผลได้ปกติ ส่วนถ้า request ที่ซ้ำกันมาพร้อมกัน ตัวที่สองจะถูก unique index บล็อกไว้จนตัวแรก commit แล้วได้ผล `DUPLICATE_IGNORED`
3. **State validation (ตัดสินจากสถานะปัจจุบัน ไม่ใช่ลำดับที่ webhook มาถึง):** lock แถวออเดอร์ก่อนแล้วจึงตัดสิน
   * `PENDING_PAYMENT` → `QUEUED`
   * `EXPIRED` (จ่ายหลัง QR หมดอายุ) → **refund อัตโนมัติ** → `REFUNDED` ไม่ฟื้นออเดอร์ที่หมดอายุแล้วกลับมาเป็น COMPLETED
   * สถานะอื่น (จ่ายไปแล้ว หรือเป็น event ใหม่ของการจ่ายเดิม) → บันทึกลง timeline แล้วข้าม
   * `payment.failed` ที่มาหลัง success (สลับลำดับ) → บันทึกไว้เฉย ๆ สถานะไม่เปลี่ยน
   * ยอดเงินไม่ตรง → `AMOUNT_MISMATCH` + alert และไม่เปลี่ยนสถานะ
4. ถ้าหาออเดอร์ไม่เจอ → transaction rollback (event ไม่ถูกบันทึก) → ตอบ 404 เพื่อให้ธนาคาร retry ภายหลัง

### ปัญหา 4: เติมเกมไม่สำเร็จ (UID ผิด vs Provider ล่ม)
* [`ClassifyErrorCode`](internal/domain/ports.go) แยกประเภท error ชัดเจน:
  * **Permanent** (`INVALID_UID`, `UID_NOT_FOUND`, `ACCOUNT_BANNED`, `REGION_MISMATCH`, ...) → `FAILED` ทันที ไม่เปลือง retry และบันทึก `PERMANENT_ERROR: <code>` พร้อม metadata ลง timeline
  * **Transient** (`PROVIDER_UNAVAILABLE`, `PROVIDER_TIMEOUT`, `BOT_LEASE_EXPIRED`, รวมถึง code ที่ระบบไม่รู้จัก) → กลับ `QUEUED` พร้อม backoff และเมื่อครบจำนวน retry → `DELAYED`
  * code ที่ไม่รู้จักให้ถือเป็น transient เพราะการ retry มีจำนวนจำกัด และสุดท้ายจะไปถึงคนตรวจที่ `DELAYED` ซึ่งปลอดภัยกว่าการตัดสินว่า FAILED ทั้งที่อาจสำเร็จได้ถ้าลองใหม่
* **`PATCH /orders/:orderNo/resolve`** (ใช้ได้กับ FAILED/DELAYED):
  * `RETRY` (+`game_uid` ใหม่) → reset `retry_count`, `QUEUED` และบันทึก UID เก่า/ใหม่ลง timeline
  * `REFUND` → เรียก gateway คืนเงิน → `REFUNDED`

### ปัญหา 5: ออเดอร์ค้าง / QR หมดอายุ / เกิน SLA
Scheduler ([`worker.Scheduler`](internal/worker/worker.go)) รันทุก `SCHEDULER_INTERVAL`:
* **ExpirePending:** `PENDING_PAYMENT` ที่ `expires_at < now` (15 นาที) → `EXPIRED` โดย lock แถวและเช็กซ้ำก่อน เผื่อ webhook จ่ายเงินเข้ามาพอดี
* **AlertStuckQueued:** ออเดอร์ `QUEUED` ที่ `queued_at < now - QUEUE_SLA (10m)` → `ALERT order queued beyond SLA` (ไม่เปลี่ยนสถานะเอง ให้ทีม ops เป็นคนตัดสิน)
* **report-issue:** บันทึกเรื่องที่ลูกค้าแจ้งลง timeline เสมอ และถ้าออเดอร์อยู่ใน `QUEUED` นานเกิน `REPORT_DELAY_AFTER` จะ escalate เป็น `DELAYED` + alert
  * ออเดอร์ที่ `IN_PROGRESS` จะ**ไม่ถูกดึงออกจาก bot** เพราะอาจทำให้จ่ายเงินซ้ำ ให้กลไก lease reaper (ปัญหา 1) จัดการแทน

ทุก job ปลอดภัยเมื่อรันหลาย replica พร้อมกัน เพราะแต่ละแถวผ่าน `FOR UPDATE` + เช็กเงื่อนไขซ้ำ + CAS ก่อนเปลี่ยนสถานะ

---

## 7. Mock scenarios (bot ในตัว)
Mock provider ตัดสินผลจาก prefix ของ `game_uid`:

| UID prefix | ผลลัพธ์ |
|---|---|
| `INVALID…` | Permanent `INVALID_UID` → FAILED |
| `DOWN…` | Transient `PROVIDER_UNAVAILABLE` → retry ×3 → DELAYED |
| `FLAKY…` | สุ่มล้มเหลว 50% |
| `HANG…` | bot จำลองการ crash (ไม่ heartbeat) → ถูก reaper ดึงกลับ |
| อื่น ๆ | สำเร็จ → COMPLETED |

## 8. Configuration (env)
`PAYMENT_TTL=15m` `LEASE_TIMEOUT=3m` `QUEUE_SLA=10m` `REPORT_DELAY_AFTER=5m` `BACKOFF_BASE=30s` `BACKOFF_MAX=30m` `MAX_RETRIES=3` `SCHEDULER_INTERVAL=15s` `BOT_WORKERS=2` `BOT_HEARTBEAT_INTERVAL=30s` `WEBHOOK_SECRET` `SEED_DATA` `DEV_ENDPOINTS`

## 9. Production notes / ต่อยอด
* ใส่ jitter ให้ backoff เพื่อกัน thundering herd และส่ง alert เข้า PagerDuty/Slack แทนการเขียน log อย่างเดียว
* ย้าย refund ไปทำผ่าน outbox pattern (ตอนนี้เรียก gateway ภายใน transaction ซึ่งพอสำหรับ mock แต่ระบบจริงควรเป็น async + idempotent refund key)
* Auth: ตอนนี้ใช้ `X-User-ID` แทน JWT ไปก่อน ส่วน `/bot/*`, `/admin/*` ควรใช้ mTLS หรือ service token
* ปิด `DEV_ENDPOINTS` ใน production
