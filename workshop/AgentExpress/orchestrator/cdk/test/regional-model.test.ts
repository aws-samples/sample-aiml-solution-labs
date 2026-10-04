/**
 * `regionalModelId` moves a model id to the deployment region's inference-profile
 * geography. The same rule is implemented in Python (app/common/vocabulary.py and
 * bff/workflow.py) and HCL (terraform/models.tf); this holds the CDK copy to the cases
 * tests/test_hardcoding_fixes.py checks the Python copies against.
 */
import * as fs from "fs";
import * as path from "path";
import { regionalModelId } from "../lib/vocabulary";

const CASES: [string, string, string][] = JSON.parse(
  fs.readFileSync(path.join(__dirname, "..", "..", "tests", "fixtures", "regional_model_cases.json"), "utf8")
);

test("the fixture has cases", () => {
  expect(CASES.length).toBeGreaterThan(5);
});

test.each(CASES)("%s in %s -> %s", (model, region, expected) => {
  expect(regionalModelId(model, region)).toBe(expected);
});

test("no region (an unresolved token) leaves the id alone", () => {
  expect(regionalModelId("us.anthropic.claude-sonnet-5")).toBe("us.anthropic.claude-sonnet-5");
});

import { imageResidencyError } from "../lib/vocabulary";

test("an image brief may not leave the deployment's geography unless allowed", () => {
  const sd = "stability.sd3-5-large-v1:0";
  expect(imageResidencyError(sd, "us-east-1", false)).toBe("");
  expect(imageResidencyError(sd, "us-west-2", false)).toBe("");
  expect(imageResidencyError(sd, "eu-west-1", false)).toMatch(/allowCrossRegion/);
  expect(imageResidencyError(sd, "eu-west-1", true)).toBe("");
});
