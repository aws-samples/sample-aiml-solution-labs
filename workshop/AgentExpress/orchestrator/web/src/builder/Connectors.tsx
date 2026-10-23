/** The Connectors gallery: pick a SaaS system, fill in two or three fields, and get an
 *  ordinary tool (connectorCatalog.ts says what each one writes). The tool then opens in the
 *  Build view like any other, to finish (the client secret) and change. */
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Container from "@cloudscape-design/components/container";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Input from "@cloudscape-design/components/input";
import Link from "@cloudscape-design/components/link";
import Modal from "@cloudscape-design/components/modal";
import SpaceBetween from "@cloudscape-design/components/space-between";
import TextFilter from "@cloudscape-design/components/text-filter";
import Tiles from "@cloudscape-design/components/tiles";
import { useEffect, useState } from "react";
import { addConnector, CONNECTORS, fieldError, matches, methodsOf, type Connector, type ConnectorMethod } from "./connectorCatalog";
import type { Project } from "./model";
import "./Connectors.css";
import { InfoLink } from "../docs/InfoLink";

const WHO: Record<ConnectorMethod, string> = { user: "Each person signs in", app: "App login" };

function Card({ c, onPick }: { c: Connector; onPick: () => void }) {
  return (
    <li>
      <Container header={<Header variant="h3" description={c.description}>{c.name}</Header>}
        footer={<SpaceBetween direction="horizontal" size="s" alignItems="center">
          <Button ariaLabel={`Set up ${c.name}`} onClick={onPick}>Set up</Button>
          <Box variant="small" color="text-body-secondary">{methodsOf(c).map((m) => WHO[m]).join(" or ")}</Box>
        </SpaceBetween>}>
        <Box variant="small" color="text-body-secondary">{c.operations.length} actions</Box>
      </Container>
    </li>
  );
}

function SetUp({ c, values, setValues, method, setMethod, clientId, setClientId, tried }: {
  c: Connector; values: Record<string, string>; setValues: (v: Record<string, string>) => void;
  method: ConnectorMethod; setMethod: (m: ConnectorMethod) => void;
  clientId: string; setClientId: (v: string) => void; tried: boolean;
}) {
  const methods = methodsOf(c);
  return (
    <SpaceBetween size="m">
      <Box>{c.description}</Box>
      {c.fields.map((f) => {
        const err = fieldError(f, values[f.id] ?? "");
        return (
          <FormField key={f.id} label={f.label} description={f.description}
            errorText={tried || values[f.id] ? err || undefined : undefined}>
            <Input value={values[f.id] ?? ""} placeholder={f.placeholder} ariaLabel={f.label}
              onChange={({ detail }) => setValues({ ...values, [f.id]: detail.value })} />
          </FormField>
        );
      })}
      {methods.length > 1 ? (
        <FormField label="How it signs in">
          <Tiles value={method} columns={2} ariaLabel="How it signs in"
            onChange={({ detail }) => setMethod(detail.value as ConnectorMethod)}
            items={methods.map((m) => ({ value: m, label: WHO[m], description: c.methods[m]!.about }))} />
        </FormField>
      ) : (
        <FormField label="How it signs in"><Box>{WHO[method]}: {c.methods[method]!.about}</Box></FormField>
      )}
      <FormField label="Client ID" description="The OAuth client's id from the setup below. Its secret is set on the tool next."
        errorText={tried && !clientId.trim() ? "Required" : undefined}>
        <Input value={clientId} ariaLabel="Client ID" onChange={({ detail }) => setClientId(detail.value)} />
      </FormField>
      <SpaceBetween size="xxs">
        <Box variant="awsui-key-label">Set up at {c.name} first</Box>
        <Box>{c.setup}</Box>
      </SpaceBetween>
      <SpaceBetween size="xxs">
        <Box variant="awsui-key-label">What it does not do</Box>
        <Box>{c.limits}</Box>
      </SpaceBetween>
      <SpaceBetween direction="horizontal" size="m">
        {c.docs.map((d) => <Link key={d.url} href={d.url} external>{d.label}</Link>)}
      </SpaceBetween>
    </SpaceBetween>
  );
}

export function Connectors({ visible, project, onDismiss, onAdded }: {
  visible: boolean;
  /** The build the tool is added to. */
  project: Project;
  onDismiss: () => void;
  /** The build with the connector's tool in it, under `key`. */
  onAdded: (r: { project: Project; key: string }, c: Connector) => void;
}) {
  const [filter, setFilter] = useState("");
  const [picked, setPicked] = useState<Connector | null>(null);
  const [values, setValues] = useState<Record<string, string>>({});
  const [method, setMethod] = useState<ConnectorMethod>("user");
  const [clientId, setClientId] = useState("");
  const [tried, setTried] = useState(false);
  useEffect(() => { if (visible) { setPicked(null); setFilter(""); } }, [visible]);

  const pick = (c: Connector) => {
    setPicked(c); setValues({}); setMethod(methodsOf(c)[0]); setClientId(""); setTried(false);
  };
  const shown = CONNECTORS.filter((c) => matches(c, filter));
  const ready = picked !== null && clientId.trim() !== "" && picked.fields.every((f) => !fieldError(f, values[f.id] ?? ""));
  const add = () => {
    setTried(true);
    if (picked && ready) onAdded(addConnector(project, picked, values, method, clientId), picked);
  };

  return (
    <Modal visible={visible} onDismiss={onDismiss} size={picked ? "medium" : "max"}
      header={<>{picked ? `Connect ${picked.name}` : "Connectors"}<InfoLink feature="connectors" /></>}
      footer={<Box float="right"><SpaceBetween direction="horizontal" size="xs">
        {picked ? <Button onClick={() => setPicked(null)}>Back</Button> : null}
        <Button variant="link" onClick={onDismiss}>Cancel</Button>
        {picked ? <Button variant="primary" onClick={add}>Add tool</Button> : null}
      </SpaceBetween></Box>}>
      {picked ? (
        <SetUp c={picked} values={values} setValues={setValues} method={method} setMethod={setMethod}
          clientId={clientId} setClientId={setClientId} tried={tried} />
      ) : (
        <SpaceBetween size="m">
          <Box>
            A connector is a ready-made tool for a major service. It becomes an ordinary REST API tool that
            signs in with OAuth, which you can change afterwards.
          </Box>
          <TextFilter filteringText={filter} filteringPlaceholder="Find a connector" filteringAriaLabel="Find a connector"
            countText={`${shown.length} ${shown.length === 1 ? "match" : "matches"}`}
            onChange={({ detail }) => setFilter(detail.filteringText)} />
          {shown.length ? (
            <ul className="axb-connectors" aria-label="Connectors">
              {shown.map((c) => <Card key={c.id} c={c} onPick={() => pick(c)} />)}
            </ul>
          ) : <Box color="text-body-secondary">No connector matches. Add a REST API or MCP server tool instead.</Box>}
        </SpaceBetween>
      )}
    </Modal>
  );
}
