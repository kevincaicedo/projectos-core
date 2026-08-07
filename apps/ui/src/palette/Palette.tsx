// The ⌘K palette overlay (m0-s09, F41). Pure view over the command
// registry: filter by subsequence, arrow/enter to run, escape to close.

import { useEffect, useMemo, useRef, useState } from "react";
import type { PaletteCommand } from "./registry";
import { subsequenceMatches } from "./subsequence";

interface PaletteProps {
  readonly commands: readonly PaletteCommand[];
  readonly onClose: () => void;
}

export function Palette({ commands, onClose }: PaletteProps) {
  const [query, setQuery] = useState("");
  const [selected, setSelected] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);

  const matches = useMemo(
    () => commands.filter((command) => subsequenceMatches(query, command.title)),
    [commands, query],
  );
  const clamped = Math.min(selected, Math.max(matches.length - 1, 0));

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  const run = (command: PaletteCommand | undefined) => {
    if (command !== undefined) {
      onClose();
      command.handler();
    }
  };

  const onKeyDown = (event: React.KeyboardEvent) => {
    if (event.key === "Escape") {
      onClose();
    } else if (event.key === "ArrowDown") {
      event.preventDefault();
      setSelected(Math.min(clamped + 1, matches.length - 1));
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      setSelected(Math.max(clamped - 1, 0));
    } else if (event.key === "Enter") {
      event.preventDefault();
      run(matches[clamped]);
    }
  };

  return (
    <div className="palette-backdrop" onClick={onClose} role="presentation">
      <div
        className="palette"
        data-palette
        role="dialog"
        aria-label="Command palette"
        onClick={(event) => {
          event.stopPropagation();
        }}
      >
        <input
          ref={inputRef}
          value={query}
          placeholder="Type a command…"
          aria-label="Search commands"
          onChange={(event) => {
            setQuery(event.target.value);
            setSelected(0);
          }}
          onKeyDown={onKeyDown}
        />
        <ul className="palette-list">
          {matches.length === 0 ? (
            <li className="palette-item" aria-disabled="true">
              No command matches. Commands register here as their features land.
            </li>
          ) : (
            matches.map((command, index) => (
              <li
                key={command.id}
                className="palette-item"
                data-selected={index === clamped}
                data-command={command.id}
                onClick={() => {
                  run(command);
                }}
                onMouseEnter={() => {
                  setSelected(index);
                }}
              >
                <span>{command.title}</span>
                <span className="palette-scope">{command.scope}</span>
              </li>
            ))
          )}
        </ul>
      </div>
    </div>
  );
}
