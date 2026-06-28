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
3. Thêm bot vào **nhóm ads** (`ADS_CHAT`)
4. Lấy `API_ID` / `API_HASH` từ [my.telegram.org](https://my.telegram.org)
5. Lấy `ALLOWED_USER_IDS` từ [@userinfobot](https://t.me/userinfobot)

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
/map /mapgen /xepbai /xepbaiwhite
/all /next /skip /help
/done1 ~ /done10 /xdone /zdone
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
