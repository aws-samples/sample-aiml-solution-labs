/** v139 Track D: a person chooses one of a run's named options.
 *
 *  Used by a review gate with `hitl.options` (HitlGate.tsx OptionGate) and by an agent's
 *  request for a decision (`hitl.kind` "decision", AskPerson.tsx). One button per option,
 *  labelled exactly as the author wrote it, and an optional comment. The choice goes through
 *  the decision route as {option, comment} (bff/asks.py); the option becomes the run field
 *  `decision` that the stage's branch rules route on. */

import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import FormField from "@cloudscape-design/components/form-field";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Textarea from "@cloudscape-design/components/textarea";
import { useState } from "react";

/** The longest comment the BFF accepts (bff/asks.py ANSWER_MAX). */
export const COMMENT_MAX = 4000;

export function DecisionOptions({ options, canDecide, onChoose }: {
  options: string[];
  canDecide: boolean;
  onChoose: (option: string, comment: string) => Promise<void>;
}) {
  const [comment, setComment] = useState("");
  const [busy, setBusy] = useState<string | null>(null);
  const tooLong = comment.length > COMMENT_MAX;

  async function choose(option: string) {
    setBusy(option);
    try { await onChoose(option, comment); setComment(""); } finally { setBusy(null); }
  }

  return (
    <SpaceBetween size="m">
      <FormField label="Comment (optional)" stretch
                 errorText={tooLong ? `At most ${COMMENT_MAX} characters.` : undefined}>
        <Textarea value={comment} rows={2} placeholder="Why, for the run's timeline and the activity log"
                  onChange={({ detail }) => setComment(detail.value)} />
      </FormField>
      <Box float="right">
        <SpaceBetween direction="horizontal" size="xs">
          {options.map((o) => (
            <Button key={o} loading={busy === o} disabled={!canDecide || tooLong || (busy !== null && busy !== o)}
                    onClick={() => void choose(o)}>
              {o}
            </Button>
          ))}
        </SpaceBetween>
      </Box>
    </SpaceBetween>
  );
}
