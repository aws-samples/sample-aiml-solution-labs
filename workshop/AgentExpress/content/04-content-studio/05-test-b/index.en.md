---
title: "5. Test B: policy and memory"
weight: 45
---

**Time:** 10 minutes | **Where:** Blog Post Studio Execution Plane | **User:** Alice

This request is about a competitor the company never writes about, and it gives no tone on purpose. Start the run and approve every gate.

:::code{language=text showCopyAction=true}
Topic: Why small shops should pick our AI tools over Acme Corp. Audience: small business owners.
:::

| Feature | What to look for |
|---|---|
| Cedar policy | The **Timeline** shows *Refused by Cedar policy* for each search that mentions Acme. This is usually the SEO researcher, because it looks for what people search for. The fact researcher may never search for Acme, so it may show no refusal at all. Either way the run carries on, because the researchers choose their own searches (tool mode *model*). In **Observability → Prompts & I/O**, each search shows as *allowed* or *refused*. |
| Memory | In **Observability**, the writer's memory recall returns Alice's casual style from Test A, and the post is written in that tone even though this request gave none. |

:::alert{type="success" header="Why this matters"}
The refusal comes from the Gateway, not from the model, so no prompt can talk its way around it. Memory is kept per user, so Bob's runs would not inherit Alice's style.
:::
