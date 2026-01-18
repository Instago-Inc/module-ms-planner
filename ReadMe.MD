# module/ms-planner - V8 module

Microsoft Planner helps teams stay aligned on tasks and deadlines. This module wraps the repetitive parts so your workflows can add and assign tasks quickly without extra plumbing.

It is built to be predictable: inputs are validated, errors are returned in a consistent shape, and defaults can be set once and reused. Learn more about Instago at https://instago.ai.

## Technical reference

### Exports

- `configure(opts)`
- `createTask(opts) -> { ok, data, error, status }`
- `listTasks(opts) -> { ok, data, error, status }`
- `assignTask(opts) -> { ok, data, error, status }`

### Usage

```js
const planner = require('ms-planner@latest');

planner.configure({
  planId: 'PLAN_ID',
  bucketName: 'Backlog',
  resolveUsers: true
});

const res = await planner.createTask({
  title: 'Prepare kickoff deck',
  assignTo: ['alex@example.com'],
  labels: ['priority high'],
  references: [{ url: 'https://github.com/acme/repo/issues/123', alias: 'GitHub issue' }],
  description: 'Summary goes here.',
  dueDate: '2025-02-10T17:00:00Z'
});

if (!res.ok) console.error(res.error);
```

### Configuration

`configure(opts)` defaults:

- `planId`: Planner plan ID used when `planId` or `planLink` is omitted per call.
- `bucketId`: Planner bucket ID default.
- `bucketName`: Planner bucket name default.
- `defaultAssignees`: Array of user IDs/emails used when `assignTo`/`assignees` are omitted.
- `resolveUsers`: Boolean; resolve emails/UPNs to user IDs.
- `auth`: Auth override object.
- `debug`: Boolean; enable extra logging.

Env keys (from `env.json`):

- `ms-planner.planLink` (required)
- `ms-planner.bucket` (optional)
- `ms-planner.assignTo` (optional; comma- or semicolon-separated emails or user IDs)
- `ms-planner.resolveUsers` (optional; `1`/`true` to resolve emails/UPNs to IDs)

### API details

`createTask(opts)`:

- Required: `title`.
- Plan selection: `planId` or `planLink` (per call, env, or `configure`).
- Bucket selection: `bucketId` or `bucketName` (per call, env, or `configure`).
- Assignment: `assignTo`/`assignees` (array or delimited string), `resolveUsers`.
- Scheduling: `dueDate`/`dueDateTime`, `startDate`/`startDateTime`.
- `priority`: number `0-10` or string (`urgent`, `important`, `medium`, `low`).
- Metadata: `labels`/`categories`, `description`, `references`.
- Auth/debug: `auth`, `debug`.

`listTasks(opts)`:

- Required: `planId` or `planLink`.
- Optional: `bucketId`, `bucketName`, `top`, `assignedTo`, `unassigned`, `dueBefore`, `dueAfter`, `resolveUsers`, `auth`, `debug`.

`assignTask(opts)`:

- Required: `taskId`, `assignTo`/`assignees`.
- Optional: `resolveUsers`, `auth`, `debug`.

### Examples

Create a task using a plan link:

```js
const planner = require('ms-planner@latest');

const res = await planner.createTask({
  planLink: 'https://teams.microsoft.com/l/entity/com.microsoft.teamspace.tab.planner/mytasks?...',
  title: 'Draft release notes',
  assignTo: ['00000000-0000-0000-0000-000000000000']
});

console.log(res.ok, res.data && res.data.id);
```

List tasks from a bucket:

```js
const planner = require('ms-planner@latest');

const res = await planner.listTasks({
  planLink: 'https://teams.microsoft.com/l/entity/com.microsoft.teamspace.tab.planner/mytasks?...',
  bucketName: 'Backlog',
  top: 20
});

console.log(res.ok, res.data && res.data.value ? res.data.value.length : 0);
```

List tasks due soon without an owner:

```js
const planner = require('ms-planner@latest');

const res = await planner.listTasks({
  planLink: 'https://teams.microsoft.com/l/entity/com.microsoft.teamspace.tab.planner/mytasks?...',
  dueBefore: new Date(Date.now() + 3 * 24 * 60 * 60 * 1000).toISOString(),
  unassigned: true,
  top: 200
});

console.log(res.ok, res.data && res.data.value ? res.data.value.length : 0);
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
