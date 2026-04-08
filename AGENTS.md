# AGENTS.md

General working policies for AI coding agents. Repository-specific details
belong in the repository guide and linked docs referenced below.

## First Read

Before changing code, read:

1. `README.md`
2. `docs/ARCHITECTURE.md`
3. `docs/REPOSITORY_GUIDE.md`

The repository guide owns project-specific details such as layout, commands,
runtime entry points, testing shortcuts, codebase landmarks, root-cause
guardrails, and the remote job API workflow.

## Operating Principles

- Stay tightly scoped to the user’s request.
- Prefer surgical edits over broad refactors.
- Keep changes consistent with the repository’s current architecture and naming.
- Update docs when behavior, commands, or architecture change.
- Add or update tests when you change behavior.
- Do not invent new frameworks, layers, or abstractions unless the task truly
  requires them.

## Review-To-Delivery Workflow

- When working from review findings, group the work by root cause instead of
  applying one-off local patches in unrelated branches.
- Prefer one GitHub issue per root cause area or tightly related bug family.
- In issue and PR descriptions, call out:
  - the invariant being restored
  - the execution paths affected
  - the regression tests added
- Keep branches focused. Avoid mixing scheduler lifecycle work, engine integrity
  fixes, and adapter transport refactors in one PR unless the user explicitly
  wants a larger coordinated change.

## Default Delivery Loop

Follow this workflow by default unless the user explicitly overrides it:

1. **Artifact-first triage**
   - Review newly synced artifact bundles before making speculative changes.
   - Triage findings by source (`vmware`, `xclarity`, `catc`, `azure`, etc.)
     and by root cause.
   - Prefer real runtime evidence over assumptions.

2. **Issue creation and labeling**
   - Create or update GitHub issues for each root cause area.
   - Apply labels for:
     - priority (`priority:P0` … `priority:P3`)
     - issue type (`type:engine`, `type:mapping`, `type:adapter`,
       `type:observability`, `type:data-quality`)
     - source (`source:vmware`, `source:xclarity`, `source:catc`,
       `source:azure`, etc.)
   - Use the open issues as the authoritative work queue.

3. **Branching discipline**
   - **Always** start new feature and bugfix branches from the current remote
     `origin/dev`, not from the local workspace branch.
   - Before creating a branch:
     - `git fetch origin`
     - branch from the correct promotion branch:
       - `origin/dev` for normal work
       - active `origin/release/<version>` for release-only fixes
       - `origin/main` only for production hotfixes
   - If the target long-lived branch moves while a PR is open, rebase or merge
     the latest target branch before attempting merge.

4. **Parallel execution**
   - Work the queue in priority order.
   - Parallelize only when write scopes are clearly separable.
   - When using multiple agents, act as a merge manager:
     - keep branches current with `origin/main`
     - resolve conflicts proactively
     - avoid duplicating work between branches

5. **PR gating**
   - Do not merge a PR until both are true:
     - CI is finished and green
     - Copilot review comments have arrived and actionable comments are handled
   - After applying any review-driven fix, rerun the gate:
     - push update
     - wait for CI again
     - wait for Copilot review again if needed

6. **Merge criteria**
   - Merge only after:
     - the PR is up to date with its current target branch
     - CI is green
     - Copilot review is clean or any actionable comments are addressed
   - Treat comments about stale branch bases, drift from the target branch, or
     missing regression coverage as actionable by default.

7. **Promotion model**
   - Treat `dev` as the integration branch for routine work.
   - Promote vetted batches from `dev` to the active `release/<version>` branch.
   - Promote `release/<version>` to `main` for production.
   - Create version tags only from a clean checkout of current `origin/main`.
   - Use semantic versioning for tags:
     - `MAJOR` for breaking changes only
     - `MINOR` for backward-compatible features
     - `PATCH` for backward-compatible fixes only
   - If a hotfix lands in `main`, ensure it is merged or cherry-picked back
     into the active `release/<version>` branch and `dev`.

8. **Idle behavior**
   - When otherwise waiting, the next checks should be:
     - open PR status
     - new artifact bundles
   - Resume the loop from new artifacts as they arrive.

## Documentation Expectations

For repository-specific documentation updates, use `docs/REPOSITORY_GUIDE.md`
as the index of what belongs where.

## Safety Notes

- Never commit secrets.
- Treat `.env.example` as the reference list of supported environment variables.
- Be careful with shared persistence/schema changes; verify all execution paths
  that depend on them.

## Scope Discipline

- Do exactly what was asked.
- Do not silently rewrite unrelated docs or code.
- If you notice stale guidance that directly affects the requested task, fix it in the same change and mention it clearly.

Last updated: 2026-04-06
