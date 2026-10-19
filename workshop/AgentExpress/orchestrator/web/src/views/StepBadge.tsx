/** A step's number, drawn as a filled badge coloured by the step's status (lib/stepNumbers.ts).
 *  The same size everywhere it appears: the run graph, the Flow tab and the Steps table. */
import { stepLabel, stepTone } from "../lib/stepNumbers";
import "./stepbadge.css";

export function StepBadge({ num, status }: { num: string; status?: string }) {
  // role="img" with a label, so the number and the status are read together as one thing
  // ("Step 2, done") rather than as a bare "2".
  return (
    <span className={`axs-badge axs-${stepTone(status)}`} role="img" aria-label={stepLabel(num, status)}
          data-testid={`step-badge-${num}`}>
      <span aria-hidden="true">{num}</span>
    </span>
  );
}
