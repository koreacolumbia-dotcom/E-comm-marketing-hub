# Dashboard Data Security Policy

## Public artifact policy

This repository may publish static dashboard files. Public artifacts must contain aggregated metrics only.

The following data must never be committed or published:

- customer names, personal email addresses, phone numbers, addresses, birthdays, CI/DI, resident registration numbers
- customer-level CRM exports or contact lists
- API keys, access tokens, passwords, private keys, service-account JSON
- raw order or member tables containing direct or stable personal identifiers

Corporate contact email addresses under `columbia.com` and `columbiakorea.co.kr` may remain public when intentionally used as business contact information.

## Automated enforcement

`Dashboard Finalize` runs `scripts/dashboard_security.py` before committing generated files.

- JSON and CSV fields matching sensitive identifiers are irreversibly pseudonymized with SHA-256-derived tokens.
- Personal email addresses, Korean mobile numbers, and resident-registration-number patterns are masked.
- Private keys, service-account credentials, API keys, GitHub tokens, AWS keys, and generic embedded secrets block publication.
- Security findings fail the workflow before generated artifacts are committed.

## Operational access

Customer-level exports must be stored outside GitHub in an access-controlled data system such as BigQuery, Google Drive with restricted sharing, or Cloud Storage with IAM. The public dashboard should receive only aggregated and minimum-necessary data.

## Incident response

When sensitive data is found in Git history:

1. Revoke or rotate exposed credentials immediately.
2. Remove the file from the current branch.
3. Rewrite Git history when required; deleting only the latest version is insufficient.
4. Review GitHub Pages and Actions artifacts for copies.
5. Record the incident and the preventive control added afterward.
