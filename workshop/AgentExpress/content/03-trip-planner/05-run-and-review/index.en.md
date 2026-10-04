---
title: "5. Run and review"
weight: 35
---

**Time:** 20 minutes | **Where:** Trip Planner Execution Plane | **User:** Alice

Agents are good at research and drafting. Humans are good at judgement. At each review gate the run pauses, a human checks the work, and the run continues, redoes part of the work, or stops. Each run below shows one of the controls a reviewer has.

The Trip Planner has two review gates: the **intake gate**, after the intake agent writes the trip brief, and the **research gate**, after the two researchers finish.

To start a run, choose **+** next to **Runs**, paste the request and choose **Start run**. The **Graph** fills in as each stage runs, and the **Timeline** records every step.

## Run 1: approve and revise

:::code{language=text showCopyAction=true}
Plan a 4-day trip to Lisbon for 2 people. Budget 2500 USD total. We like food, history and walking.
:::

1. At the **intake gate** (the first review gate), the run pauses with **Review required**. Read the trip brief and choose **Approve**. On a single agent you can also **Revise** with a note, or **Deny** to stop the run.
2. At the **research gate** (the second review gate), you decide for each agent separately:
   - For the destination researcher, choose **Revise** and enter this note:

     :::code{language=text showCopyAction=true}
     Add one free walking tour and the best neighbourhood for food.
     :::

   - For the currency researcher, choose **Approve**.
   - Choose **Submit decisions**. Only the destination researcher runs again, with your note as feedback. The currency result is kept.
3. Approve both researchers. The itinerary writer, budget checker and travel brief now run in sequence.
4. When the run finishes, open **Outputs** to read the travel brief: a summary, a day-by-day itinerary with at most three activities a day and one free half-day, the budget check in both currencies, top tips and sources.

## Run 2: over budget

:::code{language=text showCopyAction=true}
5 days in Tokyo for 3 people on a 500 USD budget, interested in anime and street food.
:::

Approve every gate. The budget checker's Lambda function calculates the total cost in both currencies and reports that the trip does not fit the budget. Because of your refinement in step 1, the travel brief flags the problem and suggests two concrete fixes, such as fewer days or a cheaper area to stay.

## Run 3: a branch ends the run

:::code{language=text showCopyAction=true}
We'd like a relaxing trip for 2 people next month. Budget 1500 EUR.
:::

At the intake gate, approve the trip brief. It has no destination, so the branch rule sends the run to **END**. The **Timeline** records which rule matched. The later agents show as **skipped**, and they cost nothing.

To test the rule you added on the canvas, start another run with this request. The trip is longer than 14 days, so it also ends after the intake gate.

:::code{language=text showCopyAction=true}
20 days in Bali for 2 people, budget 6000 USD.
:::

## Run 4: deny

:::code{language=text showCopyAction=true}
3 days in Paris for 1 person, budget 2000 GBP. We want a 5-star hotel.
:::

At the **intake gate**, enter this note and choose **Deny**:

:::code{language=text showCopyAction=true}
5-star hotels are not allowed by policy.
:::

 The run ends with the status *denied*. The note is kept with the run, and the **Activity** page records the decision.

## Re-run a step

1. Open Run 1, select the **itinerary writer**, and open its **Actions** tab.
2. In **Feedback**, enter:

   :::code{language=text showCopyAction=true}
   Make day 3 a free half-day and add one rainy-day alternative.
   :::

3. Choose **Re-run itinerary writer**. Only the itinerary writer and the steps after it run again; the research is kept, so you do not pay for it twice.
4. In the step details, the agent's history shows each version of its output and the feedback that triggered it.

At a parallel stage you can also tick several agents and re-run them together.

## Stop a run

:::code{language=text showCopyAction=true}
6 days in Rome for 2 people, budget 4000 EUR, we love art and architecture.
:::

At the **intake gate**, approve the trip brief. Then choose **Stop run** while the two researchers are working. The run stops at the next step boundary, and agents that had not started never run.

:::alert{type="success" header="What you just did"}
You used every reviewer control: approve, revise for one agent, deny, a branch, a re-run and a stop. A human stays in control of a multi-agent run from start to finish.
:::
