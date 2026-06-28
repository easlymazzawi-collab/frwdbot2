# frwdbot2 — Bot phân phối bài (copy_message)

Nâng cấp từ userbot `tool__tauto_nostage.py` v20 sang **bot Telegram** phân phối đa kênh.

## Thay đổi chính

| Trước (userbot v20) | Sau (bot v1) |
|---|---|
| Forward vào Saved Messages | Forward vào **bot** (DM) |
| Lệnh qua nhóm trung gian | Lệnh trực tiếp trong chat bot |
| `ForwardMessages(drop_author=True)` | `copy_message` / `copy_media_group` |
| User account làm admin kênh | **Bot** làm admin kênh |

`copy_message` = Telegram sao chép server-side → giữ **100%** emoji premium, bold, link ẩn, spoiler, caption.

## Yêu cầu

1. Tạo bot qua [@BotFather](https://t.me/BotFather), lấy `BOT_TOKEN`
2. Thêm bot làm **admin** các kênh đích (quyền đăng bài)
3. Thêm bot vào **nhóm ads** (`ADS_CHAT`) — để copy ads ra kênh
4. Copy file **`test_session.session`** từ tool userbot cũ vào cùng thư mục
5. `API_ID` / `API_HASH` từ [my.telegram.org](https://my.telegram.org)

**Kiến trúc hybrid:**
- **Bot** (`@upbaibot`) — nhận bài, lệnh, copy ra kênh
- **User session** (`test_session`) — đọc folder, load ads, detect topic, `/botadd`

## Cài đặt

```bash
pip install -r requirements.txt
cp .env.example .env
# Sửa .env với giá trị thật
python tool_tauto_bot.py
```

## Luồng sử dụng

1. Forward bài vào bot (chat riêng)
2. `/done*` / `/xdone` / `/zdone` → tool xếp sequence (content + ads)
3. Gõ tên kênh hoặc tap `/lệnh` → bot copy ra kênh đích
4. Tự reset, sẵn sàng batch tiếp

## Lệnh

```
/add /addf /list /del /alias /check /clean
/botadd /botaddf          ← auto mời bot + cấp admin kênh
/map /mapgen /xepbai /xepbaiwhite
/all /next /skip /help
/done1 ~ /done10 /xdone /zdone
```

### Auto add bot vào kênh folder

Bot **không thể tự join** kênh — cần tài khoản user (admin kênh) mời bot:

1. Cấu hình `SESSION_STRING` hoặc file `user_session.session` trong `.env`
2. Tài khoản user phải là **admin** các kênh trong folder (quyền thêm member + cấp admin)
3. Chạy:

```
/botaddf https://t.me/addlist/xxxxx
```

→ Đọc folder, lưu kênh vào `channels.json`, mời bot vào từng kênh, cấp quyền đăng bài.

```
/botadd          → tất cả kênh trong channels.json
/botadd 1 3 5    → kênh theo số thứ tự (/list)
/botadd 1-20     → dải kênh
```

Tạo session string (chạy 1 lần trên máy local):

```python
from pyrogram import Client
app = Client("user_session", api_id=API_ID, api_hash=API_HASH)
app.run()
# Sau khi login: print(await app.export_session_string())
```

## File dữ liệu

- `channels.json` — danh sách kênh
- `folders.json` — folder auto-sync (mỗi 1h)
- `topic_map.txt` — map topic forum → lệnh kênh
- `failed_msgs.json` — bài lỗi để retry thủ công

## Lưu ý

- **Folder sync** (`/addf`) dùng raw API — có thể cần user session riêng nếu bot không đọc được invite link
- Topic auto-map cần bot đọc được kênh nguồn (hoặc forward metadata đủ thông tin)
- Không cần `INTERMEDIATE_CHAT` — mọi tương tác qua DM bot
