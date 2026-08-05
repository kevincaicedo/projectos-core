// UI lint law (m0-s01): generated server types only, with no domain DTO fork.
// strict TS + no-any are the PROJECTOS_STYLE floor, not aspirations.
import tseslint from "typescript-eslint";

export default tseslint.config(
  { ignores: ["dist/**", "node_modules/**"] },
  ...tseslint.configs.recommended,
  {
    files: ["src/**/*.{ts,tsx}"],
    ignores: ["src/api/gen/**"],
    rules: {
      "@typescript-eslint/no-explicit-any": "error",
      "no-restricted-imports": [
        "error",
        {
          patterns: [
            {
              group: [
                "**/api/handwritten/**",
                "**/crates/pos-api/**",
                "pos-api",
                "@projectos/pos-api",
              ],
              message:
                "Server types are generated (src/api/gen) — hand-declared server types are an L12 review reject.",
            },
          ],
        },
      ],
      "no-restricted-syntax": [
        "error",
        {
          selector:
            "TSInterfaceDeclaration[id.name=/^(Evidence|Insight|Decision|Spec|Task|Milestone|Run|Memory|Incident|Release|.*(Request|Response|Event|Envelope|Descriptor|Dto|DTO|Payload|Record|Capability|CapabilityId|CapabilityState))$/]",
          message:
            "Server/domain interfaces must be generated under src/api/gen; UI source may declare view state only.",
        },
        {
          selector:
            "TSTypeAliasDeclaration[id.name=/^(Evidence|Insight|Decision|Spec|Task|Milestone|Run|Memory|Incident|Release|.*(Request|Response|Event|Envelope|Descriptor|Dto|DTO|Payload|Record|Capability|CapabilityId|CapabilityState))$/]",
          message:
            "Server/domain aliases must be generated under src/api/gen; UI source may declare view state only.",
        },
        {
          selector: "TSEnumDeclaration",
          message:
            "UI-owned enums can drift from Rust; generate server enums and model view state with local unions.",
        },
      ],
    },
  },
);
