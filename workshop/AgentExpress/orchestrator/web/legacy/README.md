# web/legacy

`observability.js` is the Observability tab: charts, run detail, the Prompts & I/O inspector,
evaluation scores and Insights. It predates the React shell and is kept as plain JavaScript on
purpose, as a self-contained island, rather than ported to components in the same change that
moved the shell to Cloudscape. `src/views/Observability.tsx` loads it from `/observability.js` and
calls its `mount(el, token, workflow)`; nothing else is shared.

It is not imported by the Vite bundle. Both IaC paths copy it beside the bundle after
`npm run build`: CDK in `cdk/lib/orchestrator-stack.ts` (the UI asset's bundling command and its
local fallback) and Terraform in `terraform/ui.tf` (served with `no-cache`, since its name has no
content hash). `tests/test_observability_syntax.py` checks that it parses, since no type-checker or
bundler reads it. Moving or renaming it means changing both IaC paths.
