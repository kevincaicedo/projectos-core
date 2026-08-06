# Security policy

ProjectOS is local-first software that holds a person's own project data on
their own machine. A vulnerability here is a vulnerability in someone's private
work, so reports are treated as first-class defects, not as public relations
events.

## Reporting a vulnerability

Send one report to **ing.sys.kevincaicedo@gmail.com**. This is the single
intake for `projectos-core` and for the hosted ProjectOS Cloud service; you do
not need to work out which side is affected.

Please do not open a public issue, pull request, or discussion for a suspected
vulnerability. A public report starts the disclosure clock before a fix exists.

A useful report contains:

- the affected version, commit, or release tag, and the shell (desktop, web,
  or CLI);
- the operating system and architecture;
- the smallest reproduction you have, ideally against a synthetic project
  rather than real data;
- the impact you believe it has — what an attacker gains, and what they need
  first.

Never include real customer content, credentials, session tokens, or API keys
in a report. A redacted reproduction is more useful than a real one, because we
cannot legally or safely store the real one.

## What happens next

| Stage | Target |
|---|---|
| Acknowledgement that a human has the report | 3 business days |
| Initial assessment, severity, and affected-version list | 10 business days |
| Fix or documented mitigation for high and critical severity | 90 days from acknowledgement |

If a target slips, the reporter is told why and given a new date rather than
silence. These are targets for a small team, not a contractual SLA.

## Disclosure

Triage is private. When a fix ships, we publish a security advisory on the
`projectos-core` repository naming the affected versions, the fixed version,
the impact, and — with the reporter's consent — the reporter's credit. We ask
reporters to hold public details until the advisory is published or 90 days
have passed, whichever comes first.

We do not currently operate a paid bug-bounty program, and we will say so
plainly rather than implying one exists.

## Scope

In scope:

- `projectos-core`: the local desktop shell, the self-hosted server, the CLI,
  the SDK and public contracts, the plugin and pack mechanisms, and the
  capability sockets in `pos-capabilities`.
- The hosted ProjectOS Cloud service, at the same intake address.

Out of scope, unless you can demonstrate concrete impact on a ProjectOS user:

- vulnerabilities in third-party dependencies that we merely consume — report
  those upstream first, then tell us so we can pin or eject;
- findings that require an attacker to already have code execution or physical
  access as the same operating-system user, since a local-first product
  deliberately trusts that user's own session;
- missing hardening headers, scanner output, or best-practice advice without a
  demonstrated attack path.

## Safe harbour

We will not pursue or support legal action against anyone who, in good faith,
researches and reports a vulnerability under this policy, provided they avoid
privacy violations, data destruction, service degradation, and access to
accounts or data that are not their own. If you are unsure whether an action is
in bounds, ask at the intake address before you take it.
