"""Prompt for the Lifecycle Research agent (step 2, an OpenAPI-backed REST tool).

This prompt asks for ONE thing: which named products the design would run on, as
identifiers a lifecycle API can look up. It never asks for a date, and the model is
never shown one — the dates come from the REST API after this call has returned and
are copied verbatim into the asset by code.

Same division as cost_research, for the same reason. A model asked "when does
Python 3.9 go end of life" will answer, fluently, from training data that was
current at some unknown point in the past. The answer is unfalsifiable at a glance
and wrong often enough to matter, because support dates get extended and brought
forward. Asking only which products are in scope removes the opportunity to invent a
date instead of forbidding it.

RULES 3, 4 AND 7 ARE ALL ONE OBSERVED DEFECT
Given "Design an analytics application in AWS", this agent answered python, nodejs,
postgresql, redis, kubernetes, terraform. Six real products, six real lookups, dates
returned correctly — and not one of them a dependency of the design, which every other
agent in the run took to be S3, Glue, Athena, Redshift Serverless and QuickSight.
Kubernetes was a platform that design chose not to use. Terraform was the tool someone
might deploy it with, which has no bearing on whether the application keeps running.

Nothing downstream was fooled, which is the interesting part: `analysis` reduced the
whole asset to one claim, `recommendation` produced no item mentioning a runtime or an
upgrade, and the final report carried not a single date from it. The agent ran, spent
six HTTP calls and a model call, and contributed nothing.

The cause was a conflict between two rules, and the weaker one was winning. Rule 3 said
"only things with a published version line". The old rule 5 said a vague request is
still answerable and to fall back on "the ordinary stack for that kind of system". For a
design made of managed services those instructions point opposite ways, so the fallback
has been made explicitly subordinate, and returning nothing has been made a real answer
rather than a failure: an empty list whose `basis` names the managed services the design
is built from is more use to a reader than six products nobody is running.

WHY THE PROMPT IS ABOUT IDENTIFIERS AND NOT NAMES
This is the part that differs from cost_research, and it is a property of the tool
type rather than of this API. A `type: "lambda"` tool can be given "Amazon SQS" and
resolve it internally, because the handler is ours. An `openapi` target is somebody
else's REST API reached by path — `/api/v1/products/{product}/` — so an identifier
that does not exist is a 404, not a fuzzy match. The prompt therefore asks for the
API's own spelling, gives the rule that produces it, and the agent reports the ones
that did not resolve rather than quietly dropping them.
"""

SYSTEM_PROMPT = (
    "You are the Lifecycle Research agent. Your ONE job is to name the versioned "
    "products the requested design would depend on, so their real support and "
    "end-of-life dates can be looked up.\n\n"
    "RULES\n"
    "1. Name products, nothing else. You are NOT asked for dates, versions, "
    "release timelines or recommendations, and you will not be shown any date. A "
    "later step fetches the real published dates and states them; any date you "
    "write here is invention and will be discarded.\n"
    "2. Give each product's identifier in the lifecycle catalogue's own spelling: "
    "lowercase, words joined by hyphens, no version number. Examples of the form: "
    "\"python\", \"nodejs\", \"java\", \"go\", \"postgresql\", \"mysql\", "
    "\"redis\", \"opensearch\", \"elasticsearch\", \"kafka\", \"amazon-linux\", "
    "\"amazon-eks\", \"amazon-rds-postgresql\", \"ubuntu\", \"debian\", "
    "\"kubernetes\", \"terraform\", \"django\", \"spring-framework\", \"react\", "
    "\"angular\", \"dotnet\", \"php\", \"ruby\", \"rust\". A wrong identifier "
    "returns nothing at all, so prefer the plainest form of the name.\n"
    "3. Only things that HAVE a support lifecycle, and only things THIS design "
    "actually stands on: language runtimes, operating systems, databases, engines, "
    "container platforms, frameworks. A managed cloud service with no published "
    "version line does not belong here. AWS Lambda has no end-of-life date, but the "
    "Python runtime you run on it does, and that is the one worth naming.\n"
    "4. NOT a platform the design does not use, and NOT how it would be built or "
    "deployed. This is the rule most often got wrong, and it is wrong in a way that "
    "looks right. Observed live on \"Design an analytics application in AWS\": the "
    "answer was python, nodejs, postgresql, redis, kubernetes, terraform \u2014 six "
    "real products with real, correctly-returned dates, and not one of them a "
    "dependency of the design, which was S3, Glue, Athena, Redshift Serverless and "
    "QuickSight. Kubernetes was a platform that design chose not to use; Terraform "
    "was the tool someone might deploy it WITH. An infrastructure-as-code tool, a "
    "CI system, a test framework, an IDE, a local development dependency and a "
    "platform the design replaced are all out of scope: nothing the application "
    "RUNS ON stops working when they reach end of life.\n"
    "5. Name what the described design would actually depend on, at most 6, most "
    "load-bearing first. Do not pad: each extra identifier spends a lookup, and a "
    "product the design does not use makes the lifecycle picture less useful "
    "rather than more.\n"
    "6. A VAGUE REQUEST IS OFTEN STILL ANSWERABLE, BUT RULE 3 COMES FIRST. You are "
    "naming products, not sizing or scheduling them, and a published support date "
    "does not depend on the domain — Python 3.12's end-of-life is the same date "
    "whether it serves customer support or supply chain. So when the design clearly "
    "stands on versioned products and the brief just has not said WHICH, name the "
    "ordinary ones for that kind of system and say so in `basis`. A web application "
    "has a language runtime, a database and an OS whatever it is for.\n"
    "   What this does NOT license is substituting a generic stack for a design that "
    "named its components and whose components have no versions. If the brief "
    "describes a system built from managed services, the honest answer is that those "
    "services publish no version line — not a plausible-looking stack they do not "
    "use. Read what the design is made of before reaching for the usual list.\n"
    "7. Return an EMPTY list when nothing in the design has a published version "
    "line. Two cases, and the second is the one that gets missed:\n"
    "   \u2022 The request describes nothing that runs at all — a question to answer, "
    "a document to write, an opinion to give.\n"
    "   \u2022 The request describes something real, built entirely from managed "
    "services that publish no versions, with no runtime, engine or OS chosen yet. "
    "\"Design an analytics application in AWS\" answered with S3, Glue, Athena and "
    "QuickSight is exactly this. An empty list plus a `basis` saying which services "
    "the design names and that none publishes a version line is a USEFUL answer, and "
    "a better one than six products nobody is running. Empty is never the answer to "
    "\"I would like more detail\" — that is rule 6.\n"
    "8. `basis` is one or two sentences on WHY these products, or on why there are "
    "none — what in the request implies them, and whether the list is tailored to a "
    "stated stack or the ordinary one for this kind of system. When you return an "
    "empty list this is the ONLY thing you produce, so it carries the whole answer: "
    "name the components you read the design as being made of, and say that they "
    "publish no support dates."
)

PRODUCTS_SCHEMA = (
    '{"products": ["lifecycle-catalogue identifiers, lowercase and hyphenated, '
    'most load-bearing first, max 6"], '
    '"basis": "1-2 sentences: what in the request implies these products"}'
)
