# module/ms-planner - V8 module

Microsoft Planner keeps teams aligned on work that actually gets shipped. This module is a small helper for creating and assigning Planner tasks from your Instago flows without turning every step into a long Graph call. Learn more about Instago at https://instago.ai.

It focuses on a simple, reliable path: create tasks in a plan and assign people by user ID or email, with defensive defaults so missing data fails softly.

## Technical reference

### API

- `configure(opts)`
  - Optional defaults: `planId`, `bucketId`, `bucketName`, `defaultAssignees`, `resolveUsers`, `debug`, `auth`.
- `createTask(opts) -> { ok, data, error, status }`
  - Required: `title`, `planId` (or `configure()`/env).
  - Optional: `bucketId`, `bucketName`, `assignTo`/`assignees`, `dueDate`, `startDate`, `priority`, `labels`/`categories`, `description`, `references`, `resolveUsers`, `auth`, `debug`.
- `assignTask(opts) -> { ok, data, error, status }`
  - Required: `taskId`, `assignTo`/`assignees`.
  - Optional: `resolveUsers`, `auth`, `debug`.

### Configuration

`configure()` and per-call options accept:

- `planId`: Planner plan ID.
- `bucketId`: Planner bucket ID.
- `bucketName`: Bucket name to resolve within the plan.
- `defaultAssignees`: Array of user IDs or emails to apply when `assignTo` is omitted.
- `resolveUsers`: Boolean. When true, resolves emails/UPNs to user IDs.
- `auth`: Graph auth overrides (access token or OAuth details used by `graph@latest`).
- `debug`: Boolean to emit extra logs.

Env keys:

- `ms-planner.planLink`
- `ms-planner.bucket` (optional)
- `ms-planner.assignTo` (optional; comma- or semicolon-separated emails or user IDs)
- `ms-planner.resolveUsers` (optional; `1` to resolve emails/UPNs into user IDs)

Note: the module accepts `ms-planner.planLink` and resolves `planId` automatically when provided.

### Usage

```js
const planner = require('ms-planner@latest');

const planLink = 'https://teams.microsoft.com/l/entity/com.microsoft.teamspace.tab.planner/mytasks?...';
const planId = (String(planLink).match(/\\/plan\\/([A-Za-z0-9_-]+)/) || [])[1] || '';

planner.configure({
  planId,
  bucketName: 'Backlog',
  resolveUsers: true
});

const res = await planner.createTask({
  title: 'Prepare kickoff deck',
  assignTo: ['alex@contoso.com'],
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
  planId: 'PLAN_ID',
  bucketId: 'BUCKET_ID',
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
  assignTo: ['sam@contoso.com', 'jordan@contoso.com'],
  resolveUsers: true
});

console.log(res.ok, res.error);
```
