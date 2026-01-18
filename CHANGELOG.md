## v1.1.0 - Autoparse Planner links
ms-planner now extracts plan IDs from Planner links and treats `ms-planner.planLink` as a primary configuration entry so task flows require less manual input.

### Added
- Added `planLink` support and plan-id parsing helpers so `ms-planner` can resolve plans directly from Planner URLs or contexts in both createTask and bucket resolution logic.
- Updated the environment manifest to expose `ms-planner.planLink` as a required field while keeping bucket/assignment overrides optional, simplifying deployment documentation.

## v1.0.1 - Streamline Microsoft Planner automation
Microsoft Planner helper centralizes bucket/user resolution, assignment building, task detail updates, and category syncing so Instago flows can create Planner tasks via Graph with fewer moving pieces.

### Added
- Added configuration defaults plus bucket/bucket-name resolution, assignment building, label/category syncing, and reference merging to simplify Planner task creation and detail updates.
- Added `assignTask` along with robust priority/date parsing and bucket/user resolvers so Planner work stays synced even when inputs are partial.
