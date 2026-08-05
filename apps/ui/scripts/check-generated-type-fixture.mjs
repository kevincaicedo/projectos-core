import { ESLint } from "eslint";

const eslint = new ESLint({ cwd: process.cwd() });
const fixturePath = "src/__fixtures__/handwritten-server-type.ts";
const [violation] = await eslint.lintText("export interface ProjectResponse { id: string }\n", {
  filePath: fixturePath,
});
const namedViolation = violation.messages.some(
  ({ ruleId, severity }) => ruleId === "no-restricted-syntax" && severity === 2,
);
if (!namedViolation) {
  console.error("generated-type fixture did not trigger the no-restricted-syntax law");
  process.exit(1);
}

const [clean] = await eslint.lintText(
  "export interface ProjectPanelViewState { selectedId: string | null }\n",
  { filePath: "src/__fixtures__/view-state.ts" },
);
if (clean.messages.some(({ severity }) => severity === 2)) {
  console.error("generated-type fixture rejected permitted view state");
  process.exit(1);
}

console.log("generated-type fixture: seeded server DTO rejected; view state accepted");
