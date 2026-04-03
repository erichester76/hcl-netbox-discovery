# Security Policy

## Supported Versions

This project is currently maintained on the `main` branch. Security fixes are expected to land there first.

If you are running an older image, branch, or fork, upgrade to the latest supported version before reporting a vulnerability unless the issue prevents upgrading safely.

## Reporting a Vulnerability

Please do **not** open public GitHub issues for suspected security vulnerabilities.

Instead:

1. Gather the details needed to reproduce and assess the issue.
2. Include impact, affected area, setup assumptions, and any mitigation you already know.
3. Send the report privately to the maintainer through the repository owner contact channel.

If a dedicated security contact address is added later, update this document and the issue-template contact links.

## What To Include

Helpful reports usually include:

- a short description of the issue
- affected component or file path
- attack prerequisites
- reproduction steps or proof of concept
- expected impact
- any known workaround or mitigation

Please redact secrets, tokens, passwords, internal hostnames, and sensitive production data.

## Response Goals

Best effort goals:

- acknowledge receipt
- validate and triage the issue
- coordinate a fix
- publish a patch and any needed release notes

No SLA is guaranteed, but good reports with reproducible detail are much easier to act on quickly.

## Scope Notes

This repository includes:

- the collector engine
- the Flask web UI
- Docker packaging
- HCL-driven integrations and source adapters

Third-party services, source systems, and downstream NetBox deployments may have separate security boundaries and should be assessed independently.
