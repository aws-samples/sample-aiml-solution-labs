/** Deploy, destroy and run a build — from the Build view, with no terminal.
 *
 *  Each build deploys as ITS OWN STACK, named from a fixed id (`ax-xxxxxxxx-stack`), so
 *  deploying one build never touches another. Deploy freezes the current draft as the
 *  next version and builds exactly that; destroy always uses the tool the build was
 *  deployed with, which is why the choice is locked once a build is deployed. The work
 *  itself runs in the console's deploy project (CodeBuild), so it survives closing the
 *  tab; this panel only starts it and reports on it. */

import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Container from "@cloudscape-design/components/container";
import ExpandableSection from "@cloudscape-design/components/expandable-section";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Link from "@cloudscape-design/components/link";
import Modal from "@cloudscape-design/components/modal";
import SpaceBetween from "@cloudscape-design/components/space-between";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import Autosuggest from "@cloudscape-design/components/autosuggest";
import Input from "@cloudscape-design/components/input";
import Select from "@cloudscape-design/components/select";
import Tiles from "@cloudscape-design/components/tiles";
import { useCallback, useEffect, useState } from "react";

import type { Action } from "../types";
import CopyToClipboard from "@cloudscape-design/components/copy-to-clipboard";
import {
  accountName, accounts, appLogin, jobActive, jobLog, type Account, type AccountLaunch, type AppLogin, type BuildSummary,
  type Tool,
} from "./storage";

/** Regions offered for a connected account. Any other can be typed. */
export const REGIONS = ["us-east-1", "us-east-2", "us-west-2", "eu-west-1", "eu-central-1", "eu-west-2",
  "ap-northeast-1", "ap-southeast-1", "ap-southeast-2", "ap-south-1", "ca-central-1"];

const HERE = "";
const accountLabel = (a: string, known: Account[] = []) => {
  if (!a) return "This console's account";
  const k = known.find((x) => x.accountId === a);
  return k ? accountName(k) : `AWS account ${a}`;
};
const REGION_RE = /^[a-z]{2}(-[a-z]+)+-\d$/;
/** A region: pick a common one, or type any other. */
export function RegionField({ value, onChange, label = "Region", description, disabled }: {
  value: string; onChange: (r: string) => void; label?: string; description?: string; disabled?: boolean;
}) {
  return (
    <FormField label={label} description={description}
      errorText={value && !REGION_RE.test(value) ? "A region looks like us-east-1" : undefined}>
      <Autosuggest value={value} disabled={disabled} ariaLabel={label} enteredTextLabel={(v) => `Use ${v}`}
        options={REGIONS.map((r) => ({ value: r }))} placeholder="us-east-1"
        onChange={({ detail }) => onChange(detail.value.trim())} />
    </FormField>
  );
}

export const TOOL_NAMES: Record<Tool, string> = { cdk: "AWS CDK", terraform: "Terraform" };

/** What a build deploys AS: a CloudFormation stack with CDK, a Terraform deployment
 *  (its own state, no stack) with Terraform. `tool` defaults to the one it uses. */
export const stackOf = (b: BuildSummary, tool?: Tool) => {
  if (!b.agentName) return "its own stack";
  return (tool ?? b.tool ?? b.deployed?.tool) === "terraform"
    ? `the Terraform deployment ${b.agentName}`
    : `the stack ${b.agentName.replace(/_/g, "-")}-stack`;
};

const PHASES: Record<string, string> = {
  queued: "waiting for the deploy project",
  generating: "generating the code for this version",
  deploying: "creating and updating resources",
  destroying: "removing resources",
  done: "done",
};

/** One line saying where the build stands. Exported for the tests. */
export function deployState(b: BuildSummary | null): {
  type: "success" | "error" | "in-progress" | "stopped" | "info"; text: string;
} {
  if (!b) return { type: "info", text: "Saving…" };
  const job = b.job;
  if (job && jobActive(b)) {
    const verb = job.action === "destroy" ? (job.deleteAfter ? "Destroying, then deleting" : "Destroying")
      : `Deploying version ${job.version}`;
    return { type: "in-progress", text: `${verb} with ${TOOL_NAMES[job.tool]} — ${PHASES[job.phase ?? ""] ?? job.phase ?? "starting"}` };
  }
  if (job?.status === "FAILED") {
    return { type: "error", text: job.action === "destroy" ? `Destroy with ${TOOL_NAMES[job.tool]} failed`
      : `Deploying version ${job.version} with ${TOOL_NAMES[job.tool]} failed` };
  }
  if (b.deployed) {
    const at = b.deployed.at ? ` · ${new Date(b.deployed.at).toLocaleString()}` : "";
    const fw = b.deployed.frameworkVersion ? ` · framework ${b.deployed.frameworkVersion}` : "";
    return { type: "success", text: `Version ${b.deployed.version} deployed with ${TOOL_NAMES[b.deployed.tool]}${fw}${at}` };
  }
  return { type: "stopped", text: "Not deployed" };
}

/** Connect an AWS account: one stack in THEIR account, then a check that it works.
 *  `resume`: an account already started (finish a pending one) or connected (update its
 *  role to this console's current permissions) — straight to the stack step. */
export function ConnectAccountModal({ visible, onDismiss, onConnected, resume }: {
  visible: boolean; onDismiss: () => void; onConnected: (a: Account) => void; resume?: Account | null;
}) {
  const [accountId, setAccountId] = useState("");
  const [label, setLabel] = useState("");
  const [region, setRegion] = useState("us-east-1");
  const [launch, setLaunch] = useState<AccountLaunch | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const valid = /^\d{12}$/.test(accountId.trim()) && REGION_RE.test(region) && label.length <= 64;
  useEffect(() => {
    if (!visible || !resume) return;
    setLaunch(null);
    setError(null);
    accounts.launch(resume.accountId)
      .then((l) => setLaunch({ ...l, update: resume.status === "connected" }))
      .catch((e: Error) => setError(e.message));
  }, [visible, resume]);
  const start = async () => {
    setBusy(true);
    try { setLaunch(await accounts.connect(accountId.trim(), region.trim(), label.trim())); setError(null); }
    catch (e) { setError((e as Error).message); }
    finally { setBusy(false); }
  };
  const verify = async () => {
    if (!launch) return;
    setBusy(true);
    try {
      onConnected(await accounts.verify(launch.accountId));
      setLaunch(null); setAccountId(""); setLabel(""); setError(null);
    }
    catch (e) { setError((e as Error).message); }
    finally { setBusy(false); }
  };

  return (
    <Modal visible={visible} onDismiss={onDismiss}
      header={resume ? (resume.status === "connected" ? `Update the role in ${accountName(resume)}` : `Finish connecting ${accountName(resume)}`)
        : "Connect an AWS account"}
      footer={
        <Box float="right">
          <SpaceBetween direction="horizontal" size="xs">
            <Button variant="link" onClick={onDismiss} disabled={busy}>Cancel</Button>
            {launch || resume
              ? <Button variant="primary" loading={busy} disabled={!launch} onClick={() => void verify()}>Verify connection</Button>
              : <Button variant="primary" loading={busy} disabled={!valid} onClick={() => void start()}>Continue</Button>}
          </SpaceBetween>
        </Box>
      }>
      <SpaceBetween size="m">
        {resume && !launch && !error ? <StatusIndicator type="loading">Preparing the stack</StatusIndicator> : null}
        {!launch && !resume ? (
          <>
            <Box>
              Your builds can deploy into an AWS account of your own, so their data, cost and resources are yours.
              Connecting creates one IAM role there, <b>AgentExpressDeploy-…</b>, that only this console&apos;s deploy
              project can assume, with an External ID unique to you. It is not an administrator: it can read the account
              and create, change and delete only what a build deploys (and bootstrap AWS CDK there, if needed). Delete
              its stack to disconnect. Connecting an account again updates its role after this console is upgraded.
            </Box>
            <FormField label="AWS account ID" description="12 digits." errorText={accountId && !valid ? "12 digits" : undefined}>
              <Input value={accountId} onChange={({ detail }) => setAccountId(detail.value.replace(/\s/g, ""))} autoFocus />
            </FormField>
            <FormField label="Name" description="Optional. How it shows in lists, e.g. Team sandbox."
              errorText={label.length > 64 ? "At most 64 characters" : undefined}>
              <Input value={label} onChange={({ detail }) => setLabel(detail.value)} placeholder="Team sandbox" />
            </FormField>
            <RegionField value={region} onChange={setRegion} label="Default region"
              description="Where a deploy goes unless you pick another. You can change it later." />
          </>
        ) : !launch ? null : (
          <SpaceBetween size="s">
            {launch.update ? (
              <>
                <Box>
                  <b>1.</b> Account {launch.accountId} is connected already. To bring its role up to date, update
                  stack <b>{launch.stackName}</b> there with this <Link href={launch.templateUrl} external>template</Link>{" "}
                  (CloudFormation → the stack → Update → Replace existing template), or save it as
                  agentexpress-connect.json and run <code>{launch.cli}</code>. The link is valid for an hour.
                </Box>
                <Box><b>2.</b> When it shows UPDATE_COMPLETE, press <b>Verify connection</b>.</Box>
              </>
            ) : (
              <>
                <Box><b>1.</b> Sign in to account {launch.accountId} and create the stack (about a minute):</Box>
                <Button href={launch.launchUrl} target="_blank" iconName="external" iconAlign="right">Launch stack in AWS</Button>
                <Box variant="small" color="text-body-secondary">
                  Or with the CLI: download the <Link href={launch.templateUrl} external>template</Link> as
                  agentexpress-connect.json, then run <code>{launch.cli}</code>. The link is valid for an hour.
                </Box>
                <Box><b>2.</b> When it shows CREATE_COMPLETE, press <b>Verify connection</b>.</Box>
              </>
            )}
          </SpaceBetween>
        )}
        {error ? <Box color="text-status-error">{error}</Box> : null}
      </SpaceBetween>
    </Modal>
  );
}

function DeployModal({ build, visible, onDismiss, onDeploy }: {
  build: BuildSummary; visible: boolean; onDismiss: () => void;
  onDeploy: (t: Tool, account: string, region: string) => Promise<void>;
}) {
  const locked = build.tool;
  const [region, setRegion] = useState<string>(build.region ?? "");
  const lockedAccount = locked ? (build.account ?? HERE) : undefined;
  const [tool, setTool] = useState<Tool>(locked ?? "cdk");
  const [account, setAccount] = useState<string>(lockedAccount ?? HERE);
  const [connected, setConnected] = useState<Account[]>([]);
  const [connecting, setConnecting] = useState(false);
  const [busy, setBusy] = useState(false);
  const next = (build.versions ?? 0) + 1;

  const refresh = useCallback(() => {
    accounts.list().then((a) => setConnected(a.filter((x) => x.status === "connected"))).catch(() => setConnected([]));
  }, []);
  useEffect(() => { if (visible) refresh(); }, [visible, refresh]);

  const where = lockedAccount ?? account;
  const conn = connected.find((a) => a.accountId === where);
  const options = [
    { value: HERE, label: accountLabel(HERE), description: "Where this console runs." },
    ...connected.map((a) => ({ value: a.accountId, label: accountName(a),
      description: `Connected · default region ${a.region}${a.builds?.length ? ` · ${a.builds.length} build${a.builds.length > 1 ? "s" : ""} deployed` : ""}` })),
    ...(lockedAccount === undefined ? [{ value: "__connect", label: "Connect an AWS account…" }] : []),
  ];
  return (
    <Modal visible={visible && !connecting} onDismiss={onDismiss} header={`Deploy version ${next}`}
      footer={
        <Box float="right">
          <SpaceBetween direction="horizontal" size="xs">
            <Button variant="link" onClick={onDismiss} disabled={busy}>Cancel</Button>
            <Button variant="primary" loading={busy}
              onClick={async () => { setBusy(true); try { await onDeploy(locked ?? tool, where, where === HERE ? "" : region.trim()); } finally { setBusy(false); } }}>
              Deploy with {TOOL_NAMES[locked ?? tool]}
            </Button>
          </SpaceBetween>
        </Box>
      }>
      <SpaceBetween size="m">
        <Box>
          This saves the build as version {next} and deploys exactly that version as <b>{stackOf(build, locked ?? tool)}</b>.
          Other builds, and this console, are not touched. It takes 15–30 minutes and keeps going if you close
          this page. You get your own login to the app it deploys: shown here, and emailed to you.
        </Box>
        <FormField label="Deploy to"
          description={lockedAccount !== undefined ? `This build is deployed in ${lockedAccount ? accountLabel(lockedAccount, connected) : "this console's account"}. To move it, destroy it first.`
            : "Your own account keeps the build's data, cost and resources yours. A build there runs from its own app."}>
          <Select
            disabled={lockedAccount !== undefined}
            selectedOption={options.find((o) => o.value === where) ?? options[0]}
            options={options}
            onChange={({ detail }) => {
              if (detail.selectedOption.value === "__connect") setConnecting(true);
              else {
                const v = detail.selectedOption.value ?? HERE;
                setAccount(v);
                setRegion(connected.find((a) => a.accountId === v)?.region ?? "");   // its default
              }
            }}
          />
        </FormField>
        {where !== HERE ? (
          <RegionField value={region || (locked ? build.region ?? "" : conn?.region ?? "")} onChange={setRegion}
            disabled={Boolean(locked)}
            description={locked ? `This build is deployed in ${build.region ?? "its region"}. To move it, destroy it first.`
              : `Any region of that account; its deploy role works in all of them. Its default is ${conn?.region ?? "the one you connected it with"}.`} />
        ) : null}
        <FormField label="Deploy with"
          description={locked ? `This build is deployed with ${TOOL_NAMES[locked]}. To switch, destroy it first — both tools would create resources with the same names.`
            : "Pick one. Destroying the build later uses the same tool automatically."}>
          <Tiles value={locked ?? tool} onChange={({ detail }) => setTool(detail.value as Tool)}
            items={[
              { value: "cdk", label: "AWS CDK", disabled: Boolean(locked && locked !== "cdk"),
                description: "A CloudFormation stack. A connected account is bootstrapped for CDK automatically." },
              { value: "terraform", label: "Terraform", disabled: Boolean(locked && locked !== "terraform"),
                description: "Terraform state is kept in this console, per user and build, and removed on destroy." },
            ]} />
        </FormField>
      </SpaceBetween>
      <ConnectAccountModal visible={connecting} onDismiss={() => setConnecting(false)}
        onConnected={(a) => { setConnecting(false); refresh(); setAccount(a.accountId); }} />
    </Modal>
  );
}

function LogView({ id }: { id: string }) {
  const [lines, setLines] = useState<string[] | null>(null);
  const [link, setLink] = useState<string | undefined>();
  const [err, setErr] = useState<string | null>(null);
  const load = async () => {
    try {
      const r = await jobLog(id);
      setLines(r.lines); setLink(r.logsUrl); setErr(null);
    } catch (e) {
      setErr((e as Error).message);
    }
  };
  return (
    <ExpandableSection headerText="Log" variant="footer"
      onChange={({ detail }) => { if (detail.expanded && lines === null) void load(); }}>
      <SpaceBetween size="xs">
        <SpaceBetween direction="horizontal" size="xs">
          <Button iconName="refresh" onClick={() => void load()}>Refresh</Button>
          {link ? <Link href={link} external>Full log in CloudWatch</Link> : null}
        </SpaceBetween>
        {err ? <Box color="text-status-error">{err}</Box> : null}
        <pre className="axb-code axb-log">{lines === null ? "Loading…" : lines.join("\n") || "No output yet."}</pre>
      </SpaceBetween>
    </ExpandableSection>
  );
}

/** The owner's temporary password for the build's own console, fetched only when asked. */
function AppLoginView({ build, signInWith }: { build: BuildSummary; signInWith?: string }) {
  const [login, setLogin] = useState<AppLogin | null>(null);
  const [error, setError] = useState<string | null>(null);
  const user = build.deployed?.appUser;
  if (signInWith) return <>People sign in with {signInWith} (Identity, Sign-in).</>;
  if (!user) return <>It has a login of its own.</>;
  if (login) {
    return (
      <SpaceBetween size="xxs">
        <span>Sign in as <b>{login.user}</b> with the temporary password{" "}
          <code>{login.password}</code>{" "}
          <CopyToClipboard variant="inline" textToCopy={login.password} copyButtonAriaLabel="Copy the password"
            copySuccessText="Password copied" copyErrorText="Could not copy" /></span>
        <span>It is temporary: the first sign-in asks you to set your own, and it stops working 7 days after it was
          issued. It was also emailed to you.</span>
      </SpaceBetween>
    );
  }
  return (
    <>
      Sign in as <b>{user}</b>.{" "}
      <Button variant="inline-link" onClick={() => {
        appLogin(build.id).then(setLogin).catch((e: Error) => setError(e.message));
      }}>Show the temporary password</Button>
      {error ? <Box color="text-status-error">{error}</Box> : null}
    </>
  );
}

export function DeployPanel({ build, errors, can, onDeploy, onDestroy, onRun, signInWith, onPublish }: {
  build: BuildSummary | null;
  /** Publish the deployed build to an Agent Registry (admins; RegistryPublish.tsx). */
  onPublish?: () => void;
  /** Its app signs people in with this provider (Identity > Sign-in), not a login of its own. */
  signInWith?: string;
  errors: number;
  can: (a: Action) => boolean;
  onDeploy: (tool: Tool, account: string, region: string) => Promise<void>;
  onDestroy: () => Promise<void>;
  /** Start a run of it from this console. Absent on a control-plane console, where a
   *  build's runs live in its own app (Open app). */
  onRun?: () => void;
}) {
  const [open, setOpen] = useState(false);
  const state = deployState(build);
  const active = jobActive(build ?? undefined);
  const deployBlocked = !build ? "Saving the build first."
    : active ? "A deploy or destroy of this build is running."
      : errors ? `Fix the ${errors} error${errors > 1 ? "s" : ""} first — they are listed under Problems.`
        : !can("deploy") ? "You do not have permission to deploy." : undefined;
  const destroyBlocked = !can("destroy") ? "You do not have permission to destroy." : undefined;
  const failed = build?.job?.status === "FAILED" ? build.job : null;

  return (
    <Container
      header={
        <Header variant="h2"
          description={build?.agentName ? <>Deploys as <b>{build.tool || build.deployed ? stackOf(build) : "its own stack"}</b>. Every AWS name in it starts with {build.agentName}.</> : undefined}
          actions={
            <SpaceBetween direction="horizontal" size="xs">
              {build?.deployed?.uiUrl && !active ? (
                <Button href={build.deployed.uiUrl} target="_blank" iconName="external" iconAlign="right">Open app</Button>
              ) : null}
              {onPublish && build?.deployed?.version && !active ? (
                <Button iconName="share" onClick={onPublish}>Publish to registry</Button>
              ) : null}
              {onRun && build?.deployed && !build.deployed.account && !active
                ? <Button iconName="caret-right-filled" onClick={onRun}>Run here</Button> : null}
              {build?.tool && !active ? (
                <Button disabled={Boolean(destroyBlocked)} disabledReason={destroyBlocked}
                  onClick={() => {
                    if (window.confirm(`Destroy ${stackOf(build)}? Every resource it created is removed with ${TOOL_NAMES[build.tool!]}, `
                      + "and its runs go with it. The build itself stays, and can be deployed again.")) void onDestroy();
                  }}>Destroy</Button>
              ) : null}
              <Button variant="primary" iconName="upload" disabled={Boolean(deployBlocked)} disabledReason={deployBlocked}
                onClick={() => setOpen(true)}>Deploy</Button>
            </SpaceBetween>
          }
        >Deployment</Header>
      }
    >
      <SpaceBetween size="s">
        <StatusIndicator type={state.type}>{state.text}</StatusIndicator>
        {failed?.error ? <pre className="axb-code axb-log">{failed.error}</pre> : null}
        {build?.deployed?.uiUrl && !active ? (
          <Box variant="small" color="text-body-secondary">
            Its app: <Link href={build.deployed.uiUrl} external>{build.deployed.uiUrl}</Link>
            {build.deployed.account ? <> — in AWS account {build.deployed.account} ({build.deployed.region}).</> : "."}{" "}
            <AppLoginView build={build} signInWith={signInWith} />
            {onRun && !build.deployed.account ? <> Or run it from here with <b>Run here</b>, or pick it in the Runs list.</> : null}
            {!onRun ? <> Its runs, observability and assistant are in its app.</> : null}
          </Box>
        ) : null}
        {build?.job ? <LogView key={`${build.job.startedAt}`} id={build.id} /> : null}
      </SpaceBetween>
      {build && open ? (
        <DeployModal build={build} visible onDismiss={() => setOpen(false)}
          onDeploy={async (t, a, r) => { await onDeploy(t, a, r); setOpen(false); }} />
      ) : null}
    </Container>
  );
}
