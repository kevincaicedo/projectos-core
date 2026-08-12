import type { CostRollupReport } from "../api/gen/api";
import type { QueryView } from "../api/query";

export function CostTicker({ view }: { readonly view: QueryView<CostRollupReport> }) {
  if (view.state === "loading") {
    return (
      <p className="run-cost mono" data-run-cost-state="loading">
        Ledger cost loading…
      </p>
    );
  }
  if (view.state === "empty") {
    return (
      <p className="run-cost mono" data-run-cost-state="empty">
        0 calls · no model cost yet
      </p>
    );
  }
  if (view.state === "error") {
    return (
      <p className="run-cost notice-clay" data-run-cost-state="error">
        Cost ledger unavailable: {view.error.message}
      </p>
    );
  }
  const row = view.data.rows.find((candidate) => candidate.feature === "echo");
  return (
    <p className="run-cost mono" data-run-cost-state="success">
      {view.data.totals.calls} model call · {view.data.totals.tokensIn} in /{" "}
      {view.data.totals.tokensOut} out · {view.data.totals.projectosUsdMicros} ProjectOS µUSD
      {row === undefined ? "" : ` · ${row.agent ?? "unattributed"}@${row.model}`}
    </p>
  );
}
