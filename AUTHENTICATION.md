# Dashboard Authentication

The production dashboard is intended to be served through Cloudflare Pages with Cloudflare Access in front of it.

## Access model

- The GitHub repository contains source code and sanitized aggregate outputs.
- The production dashboard is deployed to the Cloudflare Pages project `csk-ecomm-dashboard`.
- Cloudflare Access must require company identity before any dashboard page is served.
- Do not use a JavaScript password prompt or embed passwords in HTML.

## Required GitHub environment

Create a GitHub Environment named `dashboard-production` and add:

- `CLOUDFLARE_ACCOUNT_ID`
- `CLOUDFLARE_API_TOKEN`

The API token should have only Cloudflare Pages edit permission for the dashboard account.

## Cloudflare Access policy

1. Create the Pages project `csk-ecomm-dashboard`.
2. Attach the production hostname, preferably `dashboard.columbiakorea.co.kr`.
3. In Cloudflare Zero Trust, create an Access application for the hostname.
4. Require login through the company identity provider.
5. Allow only approved company domains or an explicit employee group.
6. Keep session duration short enough for internal analytics data, such as 8 to 12 hours.
7. Disable bypass rules and public service tokens unless separately reviewed.

Recommended allow rules:

- approved `@columbia.com` accounts
- approved `@columbiakorea.co.kr` accounts
- optionally, a dedicated e-commerce or marketing group from the identity provider

## Important cutover requirement

After Cloudflare Access is confirmed, disable the public GitHub Pages site or change the repository visibility to private. Otherwise the original `github.io` address remains a direct bypass around the login page.

Team members do not need GitHub repository access when using the authenticated Cloudflare hostname. They only need an approved company account.

## Verification checklist

- An approved company account can sign in and open every report.
- A personal email account is denied.
- Incognito access is denied before HTML or JSON is returned.
- The old GitHub Pages URL no longer serves the dashboard.
- Direct requests to report JSON files also require authentication.
