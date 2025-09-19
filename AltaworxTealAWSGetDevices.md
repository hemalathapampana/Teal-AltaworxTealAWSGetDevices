## AltaworxTealAWSGetDevices Lambda Flow — Deep-Dive and Method Reference

This document explains, in depth, how the `AltaworxTealAWSGetDevices` Lambda orchestrates device synchronization from the Teal API into the database. It includes the end-to-end flow, exact method responsibilities, retry behavior, SQS message shapes, and stored procedures used.

### Schedule and Trigger
- **EventBridge schedule**: 07:00 UTC (12:30 PM IST) daily
- **Kickoff**: EventBridge invokes the Lambda (no SQS event on the first invocation)
- **Fan-out**: Lambda seeds work and self-publishes messages to `TealDestinationQueueGetDevicesURL` for paging

## High-Level Flow
1. EventBridge triggers Lambda.
2. Lambda initializes, truncates staging (provider kickoff path), and seeds device page markers.
3. Lambda enqueues SQS messages to process device pages per group with an explicit delay.
4. For each SQS message, Lambda fetches a page of devices, stages data, merges into canonical tables, and enqueues the next page or group completion notifications.

## Configuration and Constants
- **Environment variables**
  - `TealDevicesGetURL`: Relative endpoint for device list (e.g., `api/v1/esims`).
  - `TealDestinationQueueGetDevicesURL`: SQS queue URL for device list processing.
  - `TealDeviceUsageQueueURL`: SQS queue URL for device usage processing.
- **Operational constants**
  - `PAGE_SIZE`: 100 (Teal maximum per request; used for all paging).
  - `RETRY_NUMBER`: 3 (Polly HTTP retry attempts).
  - `DELAY_SQS_MESSAGE_IN_SECONDS`: 30 (explicit SQS delay for process messages).
  - `TEAL_SYNC_FAIL_ACCEPTABLE_LIMIT`: 5 (page-level failure threshold before early completion).
  - `SQLConstant.BatchSize`: Bulk copy batch size (centrally configured; used by `SqlBulkCopy`).

## HTTP Endpoint and Rate Limits
- **Endpoint**: `GET {BaseUrl}/{TealDevicesGetURL}?requestId=...&offset=...&limit=100`
  - `BaseUrl`: Fetched per provider from `usp_Teal_Get_AuthenticationByProviderId` (e.g., `https://integrationapi.teal.global`).
  - `TealDevicesGetURL`: From environment (typically `api/v1/esims`).
  - `limit`: Always 100.
  - `offset`: `pageSize * pageNumber`.
- **Headers**
  - `Accept: application/json`
  - `ApiKey`: From DB (never hardcoded in code).
  - `ApiSecret`: From DB (never hardcoded in code).
- **Retry/backoff**
  - HTTP calls use Polly `WaitAndRetryAsync` with exponential backoff: 3s, 9s, 27s (3 attempts), plus a fallback that marks the result as error.
  - No explicit TPS cap coded; best-effort resilience only.

Security note: Any keys/secrets in this document are placeholders and must be stored in DB/secure config, not in source or logs.

## Stored Procedures (where used)
- `usp_Teal_Truncate_Device_And_Usage_Staging`
  - Truncates staging tables for devices and usage.
  - Called at provider kickoff (EventBridge path) under SQL transient retry policy.
- `usp_Teal_Update_Device_From_Staging`
  - MERGE from staging to canonical device tables for the provider.
  - Updates, inserts, and marks missing devices as `Unknown`.
  - Writes an audit row (`TealDeviceSyncAudit`).
  - Called after finishing paging for a cycle or on failure threshold.
- `usp_Teal_Get_AuthenticationByProviderId`
  - Returns `BaseUrl`, `ApiKey`, `ApiSecret`, `WriteIsEnabled`, and billing period settings.
  - Used to construct the Teal auth context.
- `usp_Teal_Devices_Sync_Seed` (invoked via `CallDeviceSyncSeedSP`)
  - Seeds `TealDevicesToSync` with page/group markers for the provider, optionally filtered by last-updated-since for delta sync.

## SQS Message Model
Messages sent to `TealDestinationQueueGetDevicesURL` carry attributes used to orchestrate paging and grouping.

```json
{
  "InitializeProcessing": "true | false",
  "GroupNumber": "<int>",
  "PageToken": "<string or empty>",
  "ServiceProviderId": "<int>",
  "RetryCount": "<int>"
}
```

- **InitializeProcessing=true**: Initialization path (seed + fan-out groups).
- **InitializeProcessing=false**: Processing path (fetch a page, stage, merge, advance).
- **DelaySeconds**: 30 seconds for process messages. No special 5-minute delay is configured in code.

## Detailed Method Explanations

### BaseFunctionHandler
- **Purpose**: One-time per-invocation bootstrap. Prepares logging, configuration, database connections, HTTP retry policies, SQS clients, and returns a `KeySysLambdaContext` used by downstream methods.
- **Inputs**: `SQSEvent?`, `ILambdaContext`
- **Outputs**: `KeySysLambdaContext` (includes logger, correlation id, env vars, SQL connections, Polly policies, queue URLs, provider scope)
- **Behavior**:
  - Hydrates environment (`TealDevicesGetURL`, destination queue URLs).
  - Creates SQL connection(s) and configures SQL transient retry policy via `RetryPolicyHelper.GetSqlTransientPolicy`.
  - Configures HTTP retry policy via `WaitAndRetryAsync` (3s, 9s, 27s) and circuit-breaker if present.
  - Ensures idempotent, per-request correlation id for log scoping.
  - Validates SQS trigger if provided and iterates records; otherwise, runs EventBridge kickoff logic.
- **Errors**:
  - Any unrecoverable setup error is logged and rethrown; `CleanUp` ensures resources are disposed.

### GetMessageQueueValues
- **Purpose**: Parse and validate SQS message attributes into a strongly-typed object for processing.
- **Inputs**: `SQSEvent.SQSMessage`
- **Outputs**: `GetDevicesSqsValues { InitializeProcessing: bool, GroupNumber: int?, PageToken: string?, ServiceProviderId: int, RetryCount: int }`
- **Behavior**:
  - Reads message attributes; applies defaults (`RetryCount=0`, `PageToken=null`).
  - Validates required fields per path: `ServiceProviderId` required; `GroupNumber` required for processing path.
  - Logs diagnostic context (traces, message id, group, provider).
- **Errors**:
  - On missing required fields, logs and throws; message will be retried per SQS/Lambda semantics.

### StartDeviceSyncInitialization
- **Purpose**: Seed the DB with page/group markers and enqueue one processing message per group.
- **Inputs**: `KeySysLambdaContext context`, `int serviceProviderId`
- **Outputs**: None (side effects: DB seeding and SQS messages)
- **Behavior**:
  1. Calls `CallDeviceSyncSeedSP` to populate `TealDevicesToSync` with markers for the provider (optionally delta by `LastUpdatedSince`).
  2. Calls `GetGroupCount` to compute the maximum `GroupNumber`.
  3. Calls `SendProcessMessagesToQueue` to enqueue one message per group with `RetryCount=0` and `DelaySeconds=30` to spread load.
  4. Calls `SendNotificationMessageToQueue` for downstream workflows (group lifecycle/visibility). No 5-minute delay is configured.
- **Errors**:
  - SQL errors are retried by the SQL transient policy; persistent failures abort initialization for this provider.

### CleanUp
- **Purpose**: Dispose/close resources and write final logs/metrics regardless of success or failure.
- **Inputs**: `KeySysLambdaContext`
- **Outputs**: None
- **Behavior**:
  - Closes SQL connections, flushes loggers, disposes HTTP clients.
  - Emits final diagnostic counters (pages processed, rows staged, merge counts, errors encountered).
- **Errors**:
  - Suppresses secondary dispose exceptions after logging (to avoid masking primary errors).

### CallDeviceSyncSeedSP
- **Purpose**: Execute `usp_Teal_Devices_Sync_Seed` to seed `TealDevicesToSync` with page/group markers.
- **Inputs**: `KeySysLambdaContext`, `int serviceProviderId`, optional `DateTime? lastUpdatedSince`
- **Outputs**: None (DB side effects)
- **Behavior**:
  - Executes SP inside `RetryPolicyHelper.GetSqlTransientPolicy`.
  - Seeds page tokens or fixed group ranges depending on provider/config.
  - Ensures idempotency by using deterministic inserts or cleaning existing markers for the provider.
- **Errors**:
  - Transient SQL errors retried per policy; persistent error aborts initialization.

### GetGroupCount
- **Purpose**: Determine how many groups were seeded to fan out SQS processing messages.
- **Inputs**: `KeySysLambdaContext`, `int serviceProviderId`
- **Outputs**: `int maxGroupNumber` (0 if none)
- **Behavior**:
  - Executes: `SELECT MAX(GroupNumber) FROM TealDevicesToSync WHERE ServiceProviderId = @ServiceProviderId`.
  - Treats `NULL` as 0 (no groups available). When 0, no process messages are enqueued.
- **Errors**:
  - Transient SQL errors retried per policy.

### SendProcessMessagesToQueue
- **Purpose**: Enqueue one process message per group to continue paging.
- **Inputs**: `KeySysLambdaContext`, `int serviceProviderId`, `int maxGroupNumber`
- **Outputs**: None (SQS side effects)
- **Behavior**:
  - For `group` in `1..maxGroupNumber`:
    - Sends message with attributes: `{ InitializeProcessing=false, GroupNumber=group, ServiceProviderId, RetryCount=0 }`.
    - Sets `DelaySeconds=30` to pace downstream throughput.
  - Logs message ids for traceability.
- **Errors**:
  - On SQS send failures, logs and relies on Lambda retry or operator intervention.

### SendNotificationMessageToQueue
- **Purpose**: Emit a notification/lifecycle message (e.g., group or provider sync lifecycle, downstream chaining).
- **Inputs**: `KeySysLambdaContext`, `int serviceProviderId`, `string integrationType = "DeviceSync"`, optional grouping metadata
- **Outputs**: None (SQS side effects)
- **Behavior**:
  - Constructs a notification payload (e.g., `{ IntegrationType: "DeviceSync", ServiceProviderId, Stage: "InitializationComplete" }`).
  - Sends to the configured queue/topic as applicable.
  - Note: There is no explicit 5-minute delay configured in code for these notifications.
- **Errors**:
  - Logs send failures; may continue as best-effort if not critical.

### ProcessDeviceListPage
- **Purpose**: Fetch a page/batch of devices from Teal for a specific group, stage them, upsert into canonical tables, and advance pagination.
- **Inputs**: `KeySysLambdaContext`, `GetDevicesSqsValues sqsValues`
- **Outputs**: None (DB/SQS side effects)
- **Behavior**:
  1. Page Marker Retrieval
     - Reads next marker for `(ServiceProviderId, GroupNumber)`:
       ```sql
       SELECT TOP 1 [PageToken], [GroupNumber], [ServiceProviderId]
       FROM [TealDevicesToSync]
       WHERE ServiceProviderId = @ServiceProviderId
         AND GroupNumber = @GroupNumber
       ORDER BY [CreatedDate]
       ```
     - If none, finalize group (optionally send completion notification) and stop.
  2. Authentication
     - Calls `GetTealAuthenticationInformation` via `usp_Teal_Get_AuthenticationByProviderId`.
     - Instantiates `TealDeviceService` with HTTP retry policy.
     - Acquires `AccessToken` then a `SessionToken`.
  3. API Fetch
     - Calls `GetTealDevicesAsync(pageToken, pageSize=100)`.
     - Receives `devices` and `nextPageToken`.
  4. Transform & Stage
     - Builds `DataTable` via `InitDeviceDataTable`.
     - For each device, maps fields with `AddDeviceToDataRow`.
     - Performs `SqlBulkCopy` into `TealDevicesStaging` using `SQLConstant.BatchSize`.
  5. Upsert & Advance
     - Executes `UpsertDevices` (`usp_Teal_Update_Device_From_Staging @ServiceProviderId`).
     - Calls `RemoveProcessedPageMarker` for the consumed marker.
     - If `nextPageToken` exists:
       - Inserts a new marker for the same group.
       - Sends a follow-up process message with `DelaySeconds=30`.
     - Else if no markers remain for the group:
       - Optionally sends group completion notification and may enqueue device-usage job.
  6. Failure Threshold Handling
     - If HTTP/API errors hit `TEAL_SYNC_FAIL_ACCEPTABLE_LIMIT` (5), stops paging for this provider, executes `UpsertDevices` with whatever is staged, enqueues device-usage, and proceeds.
- **Errors**:
  - HTTP errors retried by Polly; residual errors are logged and counted.
  - SQL operations retried by the SQL transient policy.

### GetTealAuthenticationInformation
- **Purpose**: Load per-provider auth/config from DB.
- **Inputs**: `KeySysLambdaContext`, `int serviceProviderId`
- **Outputs**: `TealAuthContext { BaseUrl, ApiKey, ApiSecret, WriteIsEnabled, BillingSettings... }`
- **Behavior**:
  - Executes `usp_Teal_Get_AuthenticationByProviderId` under SQL transient retry.
  - Validates `WriteIsEnabled` (may short-circuit writes if disabled while still allowing reads for dry-run/validation).

### GetAccessToken
- **Purpose**: Obtain OAuth/API access token for Teal.
- **Inputs**: `TealAuthContext`
- **Outputs**: `AccessToken` (string + expiry)
- **Behavior**:
  - Calls Teal auth endpoint using `ApiKey`/`ApiSecret`.
  - Caches token for the current invocation context if supported.
  - Retries on transient failures per HTTP policy.

### GetSessionToken
- **Purpose**: Exchange access token for a session token used by list API calls.
- **Inputs**: `AccessToken`
- **Outputs**: `SessionToken` (string + expiry)
- **Behavior**:
  - Calls Teal session endpoint; retries per HTTP policy.
  - Propagates token in subsequent `GetTealDevicesAsync` calls via headers.

### InitDeviceDataTable
- **Purpose**: Build an in-memory staging schema (`DataTable`) matching `TealDevicesStaging`.
- **Inputs**: None (uses known schema)
- **Outputs**: `DataTable`
- **Schema (representative)**:
  - Identifiers: `Iccid`, `Imsi`, `Imei`, `Msisdn`, `DeviceId`
  - Status & plan: `RatePlan`, `Status`, `ActivationDate`, `DeactivationDate`, `SuspensionState`
  - Activity: `LastSeenUtc`, `LastUsageUtc`, `DataUsedBytes`
  - Metadata: `Carrier`, `Country`, `Tags`, `CustomFields`, `ProviderId`
- **Behavior**:
  - Creates strongly-typed columns with nullability and defaults.
  - Prepared for high-throughput bulk copy.

### AddDeviceToDataRow
- **Purpose**: Map a single Teal device JSON into a staging row with safe conversions.
- **Inputs**: `DataTable table`, `TealDevice device`, `int serviceProviderId`
- **Outputs**: `DataRow` appended to `table`
- **Behavior**:
  - Normalizes strings (trims, uppercases where appropriate), parses dates/time zones into UTC, coerces enums.
  - Uses defaults for missing optional fields, guards against nulls.
  - Ensures `Iccid`/primary identifiers are present; otherwise, skips with warning.

### UpsertDevices
- **Purpose**: Merge staged rows into canonical device tables for the provider.
- **Inputs**: `KeySysLambdaContext`, `int serviceProviderId`
- **Outputs**: None (DB side effects; audit row written)
- **Behavior**:
  - Executes `usp_Teal_Update_Device_From_Staging @ServiceProviderId` under SQL transient retry.
  - MERGE semantics: update existing, insert new, mark non-present as `Unknown` for the provider.
  - Records counts and billing period info in `TealDeviceSyncAudit`.

### RemoveProcessedPageMarker
- **Purpose**: Ensure idempotency by deleting the consumed page marker.
- **Inputs**: `KeySysLambdaContext`, `int serviceProviderId`, `int groupNumber`, `string pageToken`
- **Outputs**: None (DB side effects)
- **Behavior**:
  - Deletes the specific marker row from `TealDevicesToSync`.
  - If multiple identical markers exist due to retries, deletes only one (by primary key/identity) to avoid skipping pages.

### WaitAndRetryAsync (HTTP retry policy)
- **Purpose**: Provide resilient HTTP calls against Teal endpoints.
- **Inputs**: HTTP delegate/action
- **Outputs**: HTTP response or propagated failure after retries
- **Behavior**:
  - Retries on 5xx and 429 responses and on transient network errors.
  - Backoff schedule: 3s, 9s, 27s.
  - Optional circuit-breaker to short-circuit repeated failures.
  - On final failure, logs and returns an error that upstream logic handles (e.g., increments failure counter).

### RetryPolicyHelper.GetSqlTransientPolicy (SQL retry policy)
- **Purpose**: Execute SQL work under a transient-fault-handling policy.
- **Inputs**: Async function for SQL command or SP execution
- **Outputs**: SQL result or propagated failure after retries
- **Behavior**:
  - Retries on known transient SQL exceptions (deadlocks, timeouts, connection drops).
  - Uses bounded retries with exponential backoff (implementation-specific; consistent with platform policy).
  - Wraps all DB calls in seeding, staging, and MERGE paths.

## Error Handling, Retries, and Thresholds
- **HTTP**: Polly `WaitAndRetryAsync` with 3 attempts; logs and counts failures.
- **SQL**: All DB commands run under `RetryPolicyHelper.GetSqlTransientPolicy`.
- **Failure threshold**: After 5 page-level API failures, the Lambda stops paging for the cycle, runs MERGE with what is staged, enqueues device-usage, and proceeds (partial data may remain).
- **SQS visibility/delay**: Uses explicit `DelaySeconds=30` for process messages; no special 5-minute delay and no custom visibility-timeout handling in code.

## Concise End-to-End Flow
1. EventBridge triggers Lambda.
2. Truncate staging tables for the cycle (provider kickoff path).
3. Fetch provider auth; seed `TealDevicesToSync` markers.
4. Enqueue one message per seeded group (`DelaySeconds=30`).
5. For each process message: fetch page (limit=100), stage into `TealDevicesStaging`, MERGE to canonical, advance marker and re-enqueue next page, or complete group.
6. On completion or threshold, write audit and enqueue usage processing.

## Appendix: Example Messages

### Initialization Message (fan-out)
```json
{
  "InitializeProcessing": "true",
  "ServiceProviderId": "42",
  "RetryCount": "0"
}
```

### Process Page Message
```json
{
  "InitializeProcessing": "false",
  "ServiceProviderId": "42",
  "GroupNumber": "3",
  "PageToken": "GetDevice_20250912163540_2&offset=100&limit=50",
  "RetryCount": "0"
}
```

Note: The `PageToken` format is provider-specific. The system standardizes to `limit=100` when calling Teal; legacy tokens may embed different values for traceability but the runtime normalizes the request to `limit=100`.

## Notes and Corrections Applied
- No 5-minute SQS delay is configured in code for notifications; only `DelaySeconds=30` is applied to process messages.
- API page size is fixed at 100 devices per request; bulk copy uses `SQLConstant.BatchSize`.
- Failed ICCIDs are not logged per-ICCID during fetch; failures are page-level in CloudWatch. Missing devices are marked `Unknown` by MERGE.