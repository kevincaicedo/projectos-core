# Generated pos-api types (do not edit, do not hand-declare)

The M0 capability-card vocabulary already enters through the `pos-api` exporter.
m0-s06 extends that same one-way boundary with ts-rs for every server type. The
UI imports server contracts from here and nowhere else — a hand-declared server
type is an L12 review reject, and CI checks generated output for staleness
against the Rust definitions.
