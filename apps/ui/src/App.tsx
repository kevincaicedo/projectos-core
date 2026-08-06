import { CapabilityRegistryView } from "./CapabilityRegistryView";

// m0-s09 replaces this bootstrap frame with the designed shell. The capability
// view is m0-s17 boundary evidence: it renders the runtime's own reported state,
// read through pos-api at runtime, never a compile-time claim about it.
export function App() {
  return (
    <main>
      <h1>ProjectOS</h1>
      <p>Walking skeleton — the UI shell lands in m0-s09.</p>
      <CapabilityRegistryView />
    </main>
  );
}
