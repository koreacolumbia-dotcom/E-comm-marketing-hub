# CRM scenario intelligence (KR)
- Public scanner: robots/HTTPS/same-site only, no bypass, no forms, no authenticated data. Max 8 pages and 12 first-party scripts per brand per run. Denied/429 pauses the origin. Records allowlisted signal tokens, not HTML or JS bodies.
- A tag or vendor signature is not proof of adoption, event execution, segmentation, or automated CRM send. Public text is a clue only.
- Manual/authorized customer test: keep test accounts, consent, action timestamps, device history and messages in a PRIVATE local workspace. Do not use public GitHub Actions for sessions or credentials.
- Split cohorts where permitted: signup only, product view, wishlist, cart, coupon nonuse. Use consented existing account; no automated account creation, no CAPTCHA bypass, no actual purchase/payment. No order creation for checkout testing unless explicitly permitted; stop before any order submission.
- Observe 1h/3h/24h/72h/7d/14d/30d; for inactivity 60/90d. Baseline/control receives same general promotion; correlate event timestamp, delivery timestamp, observed channel and subject locally. Repeated controlled observations required for trigger inference. Only aggregated or manually approved public-safe evidence should be exported; no personal mailbox data.
- Browser runtime and private message ingestion are future separate components; do not claim they are currently running.
