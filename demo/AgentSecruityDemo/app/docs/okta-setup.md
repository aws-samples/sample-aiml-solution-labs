# Okta Setup Guide (real identity + RFC 8693 OBO token exchange)

Org: **`<your-okta-org>.okta.com`** (Okta Integrator Free Plan)

> **This is the one Okta setup the whole demo uses.** Layer 2 (Identity) uses the
> login + inbound token; Layers 3–6 additionally use the OBO exchange and the
> Gateway audience. You set Okta up **once** — every layer reuses the same org, apps,
> users, and auth servers. The baseline and Layer 1 need no Okta.
>
> After finishing here, export the three client secrets as env vars (see
> [Step 7](#step-7--values-for-the-aws-side)) and start the app — the non-secret IDs
> are already baked into `server/app.py` (override via env vars for your own org).
> Concrete non-secret values for this org are in [`../../../okta-info.md`](../../../okta-info.md).

This sets up Okta so the demo shows **real** identity with:

- **`customer_id` = `C-1001` / `A-001`** — the customer/admin ID rides in a custom
  claim on the access token (present on both auth servers, preserved through the OBO
  exchange). `sub` stays Okta's default — it **cannot** be overridden (see Step 2).
- **Real RFC 8693 On-Behalf-Of (OBO) token exchange** — the agent (via AgentCore
  Identity) exchanges the user's inbound JWT for a new, audience-bound JWT, and
  sends **that exchanged token** to the AgentCore Gateway. Both tokens are real,
  decodable JWTs, shown side by side in the UI.

> **Why Okta (not Cognito / Auth0)?** Cognito can't emit custom identity claims for
> this flow or do RFC 8693. Auth0's Custom Token Exchange is Early-Access / paid-tier
> (blocked on the free dev tenant). **Okta Integrator Free Plan supports OBO token
> exchange out of the box** — it's the exact plan AWS's OBO guide is written against —
> and does it via
> *Trusted Servers*, no custom Action required.

---

## ✅ VALIDATED (2026-07-17)

The full OBO delegation flow is proven end-to-end via
`scripts/validate_okta_obo.py` (login as ada.lovelace → inbound token → RFC 8693
exchange → OBO token):

- Inbound token: `customer_id=C-1001`, `aud=iris-agent`
- Exchanged token: `customer_id=C-1001` (**preserved**), `aud=iris-gateway`
  (**rebound**), `scp=[tool:read]`, `cid=<iris-delegate>`

**Key learnings baked in:**
- **`sub` cannot be a custom value** on Okta access tokens — it's a system claim
  ("'sub' system claim could not be evaluated" if overridden). `sub` must stay the
  default `(appuser != null) ? appuser.userName : app.clientId`. Identity travels
  in the **`customer_id`** custom claim instead (present on BOTH auth servers,
  access token, Always). Lambda/Cedar scope on `customer_id`, NOT `sub`.
- **Scopes must be `Implicit` consent** (`tool:read`, `tool:update`) or the
  exchange fails with `consent_required` (M2M has no user to consent).
- **MFA/Okta Verify enrollment disabled** org-wide (Security → Authenticators →
  Enrollment: Okta Verify=Disabled, Password=Required) so demo users log in with
  password only.
- Exchange request: `subject_token_type=urn:ietf:params:oauth:token-type:access_token`,
  `grant_type=token-exchange`, delegate client creds, `audience=iris-gateway`.

---

## Progress checklist — Okta side COMPLETE ✅ (2026-07-17)

- [x] **Step 1** — both auth servers created (`iris-agent`, `iris-gateway`)
- [x] **Step 2a** — `customerId` on the **`User (default)`** profile
- [x] **Step 2b** — `customer_id` = `user.customerId` claim on BOTH servers
      (identity is in **`customer_id`**, NOT `sub` — `sub` stays Okta default)
- [x] **Step 2c** — 4 users, each `Customer ID` set (C-1001/1002/1003, A-001)
- [x] **Step 3** — `iris-login` (OIDC Web, Authorization Code, DPoP off)
- [x] **Step 4a** — `iris-support-delegate` + `iris-admin-delegate` (M2M, Token Exchange, DPoP off)
- [x] **Step 4b** — `iris-gateway` trusts `iris-agent` (Trusted Server)
- [x] **Step 4c** — access policy on `iris-gateway` for both delegates
- [x] **Step 4d** — `agent_name` = `app.clientId` claim on `iris-gateway`
- [x] **Step 5** — `tool:read`, `tool:update` scopes (Implicit consent)
- [x] **MFA** — Okta Verify enrollment disabled; password-only login
- [x] **Validated** — `scripts/validate_okta_obo.py` passes for support + admin

**Concrete values (IDs, users, AWS-side values) → `../../../okta-info.md`.** This guide
is the generic how-to; okta-info.md is the filled-in reference for this org.

**Next: AWS wiring** — Gateway CUSTOM_JWT + OAuth2 credential providers + agent
OBO code.

---

## Key domains / URLs for this org

| Thing | Value |
|-------|-------|
| Org domain | `https://<your-okta-org>.okta.com` |
| Admin console | `https://<your-okta-org>-admin.okta.com` |
| Custom auth server issuer | `https://<your-okta-org>.okta.com/oauth2/<authServerId>` |
| Token endpoint | `<issuer>/v1/token` |
| Discovery URL | `<issuer>/.well-known/openid-configuration` |

> Okta custom auth servers live under `/oauth2/<id>`. The built-in `default`
> custom server has id literally `default` → issuer
> `https://<your-okta-org>.okta.com/oauth2/default`.

---

## Architecture (single-tenant OBO)

**AgentCore Gateway** = the AWS-managed MCP server fronting the Lambda tools. The
agent connects to it over MCP with a Bearer token; the Gateway's CUSTOM_JWT
authorizer validates that token's `aud`, then runs Cedar policy and invokes the
Lambda. Two hops → two audiences:

```
  User (C-1001)
      │ login (Authorization Code)
      ▼
  ┌───────────────────────────┐
  │ Okta: PROVIDER auth server │ ── inbound JWT ──▶ Agent
  │  customer_id = C-1001      │    aud = iris-agent         │ RFC 8693 exchange
  │  aud = iris-agent          │    (token FOR the agent)    │ via AgentCore Identity
  └───────────────────────────┘                             │ (delegate client)
             ▲                                               │
             │ Trusted Server                                ▼
  ┌───────────────────────────┐  ◀── mints OBO token ────────┘
  │ Okta: RESOURCE auth server │     customer_id = C-1001 (preserved)
  │  aud = iris-gateway        │     aud = iris-gateway
  │  trusts PROVIDER           │     (token FOR the gateway)  │
  └───────────────────────────┘                              ▼ Bearer = exchanged JWT
                                              AgentCore Gateway (MCP endpoint)
                                              CUSTOM_JWT validates aud = iris-gateway
                                              → Cedar policy → Lambda tool
```

- **PROVIDER auth server** — issues the inbound user JWT (`customer_id=C-1001`,
  `aud=iris-agent`; `sub` stays Okta default).
- **RESOURCE auth server** — mints the OBO token (`aud=iris-gateway`, `customer_id`
  preserved). Must **trust** the PROVIDER as a Trusted Server.

> You can also do OBO within a **single** custom auth server (the `default` one).
> Two servers make the audience rebind explicit and match production patterns.
> This guide uses two; a single-server variant note is at the end.

---

## Step 1 — Create the two custom authorization servers

Admin Console → **Security → API → Authorization Servers → Add Authorization Server.**
Do it twice.

### Auth server 1 — PROVIDER (inbound user token)

| Field | Value |
|-------|-------|
| Name | `iris-agent` |
| Audience | `iris-agent` |
| Description | Iris demo — issues the inbound user JWT |

### Auth server 2 — RESOURCE (exchanged / OBO token)

| Field | Value |
|-------|-------|
| Name | `iris-gateway` |
| Audience | `iris-gateway` |
| Description | Iris demo — mints OBO token the Gateway validates |

Record each server's **Issuer URI** (Settings tab) — you'll need both in Step 7.
The **Audience** is what lands in the token `aud` claim.

**Created (this org):**

| Server | Auth Server ID | Issuer URI |
|--------|----------------|-----------|
| `iris-agent` (PROVIDER) | `<iris-agent-id>` | `https://<your-okta-org>.okta.com/oauth2/<iris-agent-id>` |
| `iris-gateway` (RESOURCE) | `<iris-gateway-id>` | `https://<your-okta-org>.okta.com/oauth2/<iris-gateway-id>` |

> Okta shows Issuer as **"Dynamic (based on request domain)"** — for AgentCore we
> want a **fixed** issuer. On each server → **Settings → Edit → Issuer** set it to
> the **Okta URL** (`https://<your-okta-org>.okta.com/oauth2/<id>`) rather than
> Dynamic, so the `iss` claim and the OIDC discovery URL are stable. OIDC discovery:
> `<issuer>/.well-known/openid-configuration`.

---

## Step 2 — Carry the customer identity in a `customer_id` claim

> **Important (validated):** on Okta, **`sub` cannot be set to a custom value** on
> access tokens — it's a system claim and overriding it fails with
> *"'sub' system claim could not be evaluated"*. So identity does **NOT** ride in
> `sub`. Instead we add a user attribute `customerId` and emit it as a custom claim
> **`customer_id`** on both auth servers. `sub` stays Okta's default; the app, Lambda
> tools, and Cedar policy all key on **`customer_id`**, never `sub`.

### 2a. Add a `customerId` user attribute — to the DEFAULT profile

Admin Console → **Directory → Profile Editor**. On the **Users** tab, click the
**`User (default)`** profile row (Type = Okta) — the blue link, NOT the
"Create Okta User Type" button.

> ⚠️ Do NOT click **"Create Okta User Type"** — that makes a whole new user type
> and the attribute won't be on the default profile. If you already did, just make
> sure you add `customerId` to **`User (default)`** here so every user has it.

On the `User (default)` profile → **+ Add Attribute**:

| Field | Value |
|-------|-------|
| Data type | string |
| Display name | Customer ID |
| Variable name | `customerId` |
| User permission | **Read-Write** |

Save.

### 2b. Add the `customer_id` claim (on BOTH auth servers)

Add this claim to **`iris-agent`** *and* **`iris-gateway`** so the identity is
present on both the inbound token and the exchanged OBO token:

1. Open the auth server → **Claims** tab → **Add Claim**.

| Field | Value |
|-------|-------|
| Name | `customer_id` |
| Include in token type | **Access Token**, **Always** |
| Value type | **Expression** |
| Value | `user.customerId` |
| Include in | **Any scope** |

2. Leave `sub` at its Okta default (`(appuser != null) ? appuser.userName :
   app.clientId`) — do **not** try to override it (it will fail; see the note in
   Step 2). The app reads identity from `customer_id`.

> Verify later in the auth server's **Token Preview** tab: pick a user, mint a token,
> and confirm `customer_id` is present and correct. On the OBO token, `customer_id`
> must be **preserved** from the inbound token.

### 2c. Create the users

Admin Console → **Directory → People → Add Person** (one per user):

- **User type**: `User` (the default — since `customerId` is now on the default
  profile from 2a).
- **First / Last name**, **Username** (= email), **Primary email** per the table.
- Check **"I will set password"** → set a demo password (e.g. `P@ssw0rd!23`) so no
  activation email is needed.
- **Save and Add Another** for the next.

| Login (email) | Name | customerId | Role |
|---------------|------|-----------|------|
| ada.lovelace@example.com | Ada Lovelace | `C-1001` | customer → support agent |
| alan.turing@example.com | Alan Turing | `C-1002` | customer → support agent |
| grace.hopper@example.com | Grace Hopper | `C-1003` | customer → support agent |
| admin@example.com | Admin User | `A-001` | admin → admin agent |

**The Customer ID field is NOT on the Add Person form** — set it after creating:
open each user → **Profile → Edit** → set **Customer ID** = `C-1001` (etc.) → Save.
This value is what the `sub` claim (`user.customerId`) reads, so it must be set or
`sub` comes back empty.

---

## Step 3 — Create the login application (iris-login)

Admin Console → **Applications → Applications → Create App Integration**.

| Field | Value |
|-------|-------|
| Sign-in method | **OIDC - OpenID Connect** |
| Application type | **Web Application** |
| Name | `iris-login` |

Settings:
- **Sign-in redirect URIs**: `http://localhost:8000/callback`
- **Sign-out redirect URIs**: `http://localhost:8000` (optional)
- **Grant types**: **Authorization Code** only. (Okta Integrator no longer offers
  the Resource Owner Password grant, so the demo uses the 3-legged redirect flow.)
- **DPoP**: OFF (do not require Proof of Possession).
- **Assignments**: assign the demo users (or "Allow everyone").
- Record **Client ID** and **Client Secret** (see below for where).

#### 📍 Where to find Client ID & Client Secret in the Okta UI

For any OIDC/API-Services app:

1. **Applications → Applications →** click the app (e.g. `iris-login`).
2. **General** tab → **Client Credentials** section:
   - **Client ID** — shown in a copyable field.
   - **Client authentication** must be **Client secret** (not Public/PKCE) for a
     secret to exist.
3. Scroll to the **CLIENT SECRETS** section (just below Client Credentials):
   - Click the **eye icon** (👁) on the secret row to reveal it, or the **copy
     icon** to copy — this is the **Client Secret**.
   - If none exists (or you need to rotate), click **Generate new secret**.

> ⚠️ Secrets are shown masked by default. Copy them into env vars / Secrets
> Manager — never commit them. Rotate via **Generate new secret** after the demo.

### 3a. Authorization Code (3-legged) login flow — how the demo uses it

Okta Integrator dropped the password grant, so login is the real redirect flow:

1. User clicks **Login** in the demo UI → browser redirects to the **PROVIDER**
   auth server's `/v1/authorize` with:
   ```
   https://<your-okta-org>.okta.com/oauth2/<iris-agent-id>/v1/authorize
     ?client_id=<iris-login-client-id>
     &response_type=code
     &scope=openid profile customer
     &redirect_uri=http://localhost:8000/callback
     &state=<random>
   ```
2. User authenticates on Okta's hosted page (email + password).
3. Okta redirects back to `http://localhost:8000/callback?code=...&state=...`.
4. The demo server exchanges the code at the **PROVIDER** token endpoint:
   ```
   POST https://<your-okta-org>.okta.com/oauth2/<iris-agent-id>/v1/token
     grant_type=authorization_code
     code=<code>
     redirect_uri=http://localhost:8000/callback
     client_id / client_secret = iris-login creds
   ```
5. The returned **access token** has `aud=iris-agent`, `customer_id=C-1001` — this is
   the inbound JWT the agent presents to the Gateway (before OBO exchange).

> The demo server needs a `/callback` route to receive the code — wired on the
> AWS/server side (see "What happens next"). For quick testing without the demo
> server, use the auth server's **Token Preview** tab in the Okta console to mint
> a token for a user and inspect `sub`.

---

## Step 4 — Create the delegate app + wire OBO (Trusted Server)

The delegate is the client AgentCore Identity authenticates as to perform the
exchange.

### 4a. Delegate apps — ONE PER AGENT (for per-agent actor claim)

The demo wants the OBO token's **actor** to name *which* agent acted (support vs
admin). At the Okta level the actor is the client that performs the exchange, so
we create **two** delegate apps — one per agent — and stamp each with an
`agentName` in its app profile that the `act` claim reads.

Create BOTH via **Applications → Create App Integration → API Services** (M2M):

| App name | App profile `agentName` | Used by |
|----------|-------------------------|---------|
| `iris-support-delegate` | `iris-support-agent` | support agent runtime |
| `iris-admin-delegate` | `iris-admin-agent` | admin agent runtime |

For each app:
- **Grant types**: **Client Credentials** + **Token Exchange**
  (`urn:ietf:params:oauth:grant-type:token-exchange`)
- **Disable DPoP** (AgentCore is a token relay, can't hold the key).
- Record **Client ID** and **Client Secret** (each agent runtime uses its own) —
  see "Where to find Client ID & Client Secret in the Okta UI" under Step 3.
- **Set the app-profile attribute** `agentName`:
  - App → **Profile** (or via API `PUT /api/v1/apps/{id}` → `profile.agentName`)
  - `iris-support-delegate` → `agentName = "iris-support-agent"`
  - `iris-admin-delegate`   → `agentName = "iris-admin-agent"`

> During a token exchange, Okta's `app` in Expression Language resolves to the
> client performing the exchange (the delegate). So `app.profile.agentName` yields
> the acting agent's name — see the `act` claim in 4d.

> (The earlier single `iris-delegate` app can be renamed to `iris-support-delegate`
> and given the `agentName` profile value, then create `iris-admin-delegate` fresh.)

### 4b. Trust the PROVIDER on the RESOURCE server

This is the key OBO wiring — it lets `iris-gateway` accept a subject token issued
by `iris-agent`.

1. Admin Console → **Security → API → `iris-gateway` (RESOURCE) → Trusted Servers.**
2. Add **`iris-agent`** as a trusted server.

> Trust is one-way: `iris-gateway` trusts `iris-agent`, not the reverse.
> If both tokens are minted by the **same** single custom server, you can skip
> this step (see single-server note at the end).

### 4c. Access policy on the RESOURCE server for the delegate

1. `iris-gateway` → **Access Policies → Add Policy** (e.g. `iris-obo-policy`),
   assign to **BOTH** `iris-support-delegate` and `iris-admin-delegate`.
2. Add a **Rule** allowing grant type **Token Exchange** and the tool scopes
   (`tool:read`, `tool:update` from Step 5). Scopes must be **Implicit** consent
   (else exchange fails with `consent_required`).

### 4d. Add the `agent_name` (actor) claim on iris-gateway

We display the **real signed token exactly as Okta issues it** — no reshaping.

> **Okta grant limitation (verified):** in the token-exchange grant, `app.profile.*`
> is NOT available — it returns null (even built-in `app.profile.label`). Only
> `app.clientId` / `app.id` resolve. So we can't read a friendly `agentName` from
> the app profile here. Instead we emit the acting delegate's **client id** and map
> it to a friendly name in the agent/UI. (A token inline hook could inject a
> friendly string, but that's overkill for the demo.)

`iris-gateway` → **Claims → Add Claim**:

| Field | Value |
|-------|-------|
| Name | `agent_name` |
| Include in token type | **Access Token**, **Always** |
| Value type | **Expression** |
| Value | `app.clientId` |

During the exchange, `app.clientId` is the acting delegate's client id — which
differs per agent (support delegate vs admin delegate), so it truthfully
identifies which agent performed the exchange. (This equals the built-in `cid`
claim; `agent_name` just makes the intent explicit.)

**Agent/UI maps client id → friendly name** (each agent knows its own delegate):
- support delegate `<support-delegate-client-id>` → `iris-support-agent`
- admin delegate   `<admin-delegate-client-id>` → `iris-admin-agent`

Demo framing of the signed OBO token:
- **`customer_id`** = the user acted on behalf of (authorize on this) — `C-1001`
- **`agent_name`** / **`cid`** = the acting delegate's client id → mapped to the
  agent name in the console
- **`aud`** = `iris-gateway` (rebound), **`scp`** = the narrowed scope

---

## Step 5 — Scopes on the RESOURCE (iris-gateway) auth server

`iris-gateway` → **Scopes → Add Scope**:

| Scope | Meaning |
|-------|---------|
| `tool:read` | read tools (get_record, get_my_info) — support agent |
| `tool:update` | update_record — admin agent |

The exchanged token carries the granted scopes; the Lambda/Cedar can enforce
read vs update. (For per-user scoping you can add a claim rule, or enforce purely
in Cedar + Lambda.)

---

## Step 6 — Verify `customer_id = C-1001` (before wiring AWS)

> The **password-grant curl does NOT work** — Okta Integrator only offers the
> Authorization Code grant (see Step 3a), so `iris-login` rejects
> `grant_type=password` with `unauthorized_client`. That's expected. Use Okta's
> built-in **Token Preview** to inspect the token instead — no login needed.

**Token Preview (recommended):**

1. Admin Console → **Security → API → `iris-agent`** → **Token Preview** tab.
2. Set:
   - **OAuth/OIDC client**: `iris-login`
   - **Grant type**: `Authorization Code`
   - **User**: `ada.lovelace@example.com`
   - **Scopes**: `openid customer`
3. Click **Preview Token** and inspect the **Access Token**.

Confirm the access token contains:

```json
{
  "customer_id": "C-1001",
  "aud": "iris-agent",
  "iss": "https://<your-okta-org>.okta.com/oauth2/<iris-agent-id>",
  "sub": "ada.lovelace@example.com",
  ...
}
```

- If `customer_id` = `C-1001` → identity model is correct. ✅ (`sub` is the Okta
  default — the app ignores it.)
- If `customer_id` is missing/empty → the `customerId` value isn't set on the user
  (Step 2c), OR the `customer_id` claim didn't save (Step 2b). Re-check both.

> Full end-to-end token retrieval happens through the demo server's Authorization
> Code `/callback`. Token Preview is enough to validate the identity model here.
> You can also run `scripts/validate_okta_obo.py` (see the README) to prove the full
> inbound → OBO exchange before touching AWS.

---

## Step 7 — Values for the AWS side

Concrete filled-in values live in **`../../../okta-info.md`**. Non-secret IDs:

| Name | Where | Example |
|------|-------|---------|
| `OKTA_AGENT_ISSUER` | `iris-agent` Issuer URI | `https://<org>.okta.com/oauth2/<iris-agent-id>` |
| `OKTA_AGENT_AUDIENCE` | `iris-agent` audience | `iris-agent` |
| `OKTA_GATEWAY_ISSUER` | `iris-gateway` Issuer URI | `https://<org>.okta.com/oauth2/<iris-gateway-id>` |
| `OKTA_GATEWAY_AUDIENCE` | `iris-gateway` audience | `iris-gateway` |
| `OKTA_LOGIN_CLIENT_ID` | `iris-login` | `0oa...` |
| `OKTA_SUPPORT_DELEGATE_CLIENT_ID` | `iris-support-delegate` | `0oa...` |
| `OKTA_ADMIN_DELEGATE_CLIENT_ID` | `iris-admin-delegate` | `0oa...` |
| discovery (either) | `<issuer>/.well-known/openid-configuration` | — |
| `subject_token_type` | exchange param | `urn:ietf:params:oauth:token-type:access_token` |

### 🔑 Client-secret env vars to export (NOT stored in any file)

Three secrets — one per app. Get each from the app's **General → CLIENT SECRETS →
eye/copy icon** (see the "Where to find Client ID & Client Secret" note in Step 3):

```bash
export OKTA_LOGIN_CLIENT_SECRET='<from iris-login>'
export OKTA_SUPPORT_DELEGATE_CLIENT_SECRET='<from iris-support-delegate>'
export OKTA_ADMIN_DELEGATE_CLIENT_SECRET='<from iris-admin-delegate>'
```

The validation script also needs the admin delegate's **client id**:

```bash
export OKTA_ADMIN_DELEGATE_CLIENT_ID='<iris-admin-delegate client id>'
```

> These secrets go into env vars / AWS Secrets Manager only — never commit them.
> **Rotate** (Generate new secret) after the demo, since they were handled in chat.

---

## Common pitfalls (Okta + AgentCore)

- **`subject_token_type` must be `access_token`, not `jwt`.** Okta rejects `jwt`
  with `invalid_request`. Set it via the credential-provider `customParameters`.
- **Okta puts the client id in `cid`, not `client_id`.** Gateway's
  `allowedClients` won't match — use **`allowedAudience`** (`iris-gateway` for the
  exchanged token that reaches the Gateway).
- **Trusted Server is mandatory** for cross-auth-server exchange, or the RESOURCE
  server refuses the foreign subject token.
- **Delegate needs BOTH**: the `token-exchange` grant enabled on the app AND to be
  in the RESOURCE server's access policy — else `unauthorized_client`.
- **Disable DPoP** on the delegate (and login) apps.
- **Custom auth server needs an access policy** — a fresh Integrator org's
  `default` server sometimes lacks one; add a policy or exchanges fail.

---

## Single-server variant (simpler, optional)

If you'd rather not manage two servers: use only the `default` custom auth server
(`https://<your-okta-org>.okta.com/oauth2/default`) for BOTH the inbound token
and the exchange. The inbound token has `aud=iris-agent`, the exchange requests
`audience=iris-gateway` from the same server. **No Trusted Server needed** because
the same server issued the subject token. Simpler wiring; the audience rebind is
still real. The two-server version above is closer to production but the
single-server one is fine for the demo.

---

## What happens next (AWS side — wired by the demo)

Once you provide the Step 7 values:

1. **Gateway** created with `CUSTOM_JWT` → `discoveryUrl` = `OKTA_RESOURCE_DISCOVERY`,
   `allowedAudience = ["iris-gateway"]` (the exchanged token's audience).
2. **OAuth2 credential provider** against the RESOURCE server
   (`grantType: TOKEN_EXCHANGE`, `actorTokenContent: NONE`,
   `subject_token_type: access_token`) holding the `iris-delegate` creds,
   `audience = iris-gateway`.
3. The **agent** performs the OBO exchange via AgentCore Identity, then sends the
   **exchanged token** to the Gateway. Gateway validates `aud = iris-gateway`,
   evaluates Cedar policy, invokes the Lambda tool with the claims.
4. The **UI** shows the decoded inbound JWT (`customer_id = C-1001`, `aud = iris-agent`)
   and the decoded exchanged token (`customer_id = C-1001`, `aud = iris-gateway`).
