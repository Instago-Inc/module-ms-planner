# module/ms-planner - V8 module

Microsoft Planner keeps teams aligned on work that actually gets shipped. This module is a small helper for creating and assigning Planner tasks from your Instago flows without turning every step into a long Graph call. Learn more about Instago at https://instago.ai.

It focuses on a simple, reliable path: create tasks in a plan and assign people by user ID or email, with defensive defaults so missing data fails softly.

## Technical reference

### API

- `configure(opts)`
  - Optional defaults: `planLink`, `bucketName`, `resolveUsers`, `debug`, `auth`.
- `createTask(opts) -> { ok, data, error, status }`
  - Required: `title`, `planLink` (or `configure()`/env).
  - Optional: `bucketName`, `assignTo`/`assignees`, `dueDate`, `startDate`, `priority`, `labels`/`categories`, `description`, `references`, `resolveUsers`, `auth`, `debug`.
- `assignTask(opts) -> { ok, data, error, status }`
  - Required: `taskId`, `assignTo`/`assignees`.
  - Optional: `resolveUsers`, `auth`, `debug`.

### Configuration

`configure()` and per-call options accept:

- `planLink`: Planner plan link (Teams/Planner URL). The module extracts `planId` automatically.
- `bucketName`: Bucket name to resolve within the plan.
- `resolveUsers`: Boolean. When true, resolves emails/UPNs to user IDs.
- `auth`: Graph auth overrides (access token or OAuth details used by `graph@latest`).
- `debug`: Boolean to emit extra logs.

Env keys:

- `ms-planner.planLink`
- `ms-planner.bucket` (optional)
- `ms-planner.assignTo` (optional; comma- or semicolon-separated emails or user IDs)
- `ms-planner.resolveUsers` (optional; `1` to force resolving non-email identifiers like usernames/display names)

Note: the module accepts `ms-planner.planLink` and resolves `planId` automatically when provided.

### Usage

```js
const planner = require('ms-planner@latest');

planner.configure({
  planLink: 'https://teams.microsoft.com/l/entity/com.microsoft.teamspace.tab.planner/mytasks?...',
  bucketName: 'Backlog',
  resolveUsers: true
});

const res = await planner.createTask({
  title: 'Prepare kickoff deck',
  assignTo: ['alex@example.com'],
  labels: ['priority high'], // auto-creates plan label slots when missing
  references: [{ url: 'https://github.com/acme/repo/issues/123', alias: 'GitHub issue' }],
  description: 'Summary goes here.',
  dueDate: '2025-02-10T17:00:00Z'
});

if (!res.ok) console.error(res.error);
```

### Examples

Create a task with explicit IDs:

```js
const planner = require('ms-planner@latest');

const res = await planner.createTask({
  planLink: 'https://teams.microsoft.com/l/entity/com.microsoft.teamspace.tab.planner/mytasks?...',
  title: 'Draft release notes',
  assignTo: ['00000000-0000-0000-0000-000000000000']
});

console.log(res.ok, res.data && res.data.id);
```

Assign additional users to an existing task:

```js
const planner = require('ms-planner@latest');

const res = await planner.assignTask({
  taskId: 'TASK_ID',
  assignTo: ['sam@example.com', 'jordan@example.com'],
  resolveUsers: true
});

console.log(res.ok, res.error);
```
