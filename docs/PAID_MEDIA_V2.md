# Paid Media integration for Marketing Hub v2

The v2 dashboard reads aggregate media data from `reports/paid_media/summary.json`.

## Required aggregate schema

```json
{
  "updated_at": "2026-07-16 18:00 KST",
  "spend": 12500000,
  "revenue": 48300000,
  "roas": 386.4,
  "cac": 32100,
  "new_customer_cac": 44700,
  "platforms": [
    {"platform": "Google", "spend": 4200000, "revenue": 19400000, "roas": 461.9},
    {"platform": "Meta", "spend": 3600000, "revenue": 12100000, "roas": 336.1},
    {"platform": "Naver", "spend": 3900000, "revenue": 16800000, "roas": 430.8}
  ]
}
```

## Security requirements

- Aggregate values only.
- No customer IDs, emails, phone numbers, order-level records, click IDs, or audience export files.
- API credentials must remain in GitHub Actions Secrets or Google Cloud Workload Identity.
- The browser must never call Google Ads, Meta, Naver, Kakao, or BigQuery APIs directly.
- The existing dashboard security gate scans the generated output before Cloudflare deployment.

## Recommended source

Create one BigQuery view that normalizes platform cost and attributed revenue by date and platform. A scheduled GitHub Action can query that view and write the aggregate JSON file before `Dashboard Finalize` runs.
