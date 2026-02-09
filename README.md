# docker_opensearch_syn

Cronjob Docker container สำหรับ sync ข้อมูลจาก Supabase → S3 → OpenSearch แล้ว mark `opensearch_sync=true` ใน Supabase

## Flow

```
Start Container
  → 01_download.js  (Supabase → S3 as JSONL files)
  → 02_sync.js      (S3 → OpenSearch via bulk upsert)
  → 03_test.js      (ตรวจสอบ sync ครบ, ถ้าไม่ครบ re-sync อีกรอบ)
  → 04_mark_synced.js (mark opensearch_sync=true ใน Supabase)
  → 05_clean_s3.js  (ลบ temp files ใน S3)
  → Exit (container ปิดตัว)
```

## Environment Variables (ตั้งใน Render)

| Variable | Description | Example |
|----------|-------------|---------|
| `SUPABASE_URL` | Supabase project URL | `https://xxx.supabase.co` |
| `SUPABASE_ANON_KEY` | Supabase service role key | `eyJ...` |
| `OPENSEARCH_NODE` | AWS OpenSearch endpoint | `https://search-xxx.es.amazonaws.com` |
| `S3_BUCKET` | S3 bucket name | `scamtify-pageseeker-data` |
| `S3_REGION` | AWS region | `ap-southeast-1` |
| `AWS_ACCESS_KEY_ID` | AWS access key | `AKIA...` |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | `ja63...` |

## Deploy to Render (Cron Job)

1. สร้าง **Cron Job** ใน Render
2. เลือก Docker runtime
3. ชี้ไปที่ folder `docker_opensearch_syn`
4. ตั้ง Environment Variables ตามตาราง
5. ตั้ง Schedule เช่น `0 */6 * * *` (ทุก 6 ชม.)
6. Container จะรัน sync แล้วปิดตัวเองเมื่อเสร็จ

## Build & Test Local

```bash
# Build
docker build -t opensearch-sync-job .

# Run (ต้องมี .env file)
docker run --env-file ../.env opensearch-sync-job
```
