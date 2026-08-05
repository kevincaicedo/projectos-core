import { CapabilityRegistryPreview } from "./CapabilityRegistryPreview";

// m0-s09 replaces this bootstrap frame with the designed shell. The capability
// preview is m0-s17 boundary evidence and remains explicit about non-live state.
export function App() {
  return (
    <main>
      <h1>ProjectOS</h1>
      <p>Walking skeleton — the UI shell lands in m0-s09.</p>
      <CapabilityRegistryPreview />
    </main>
  );
}
