## Teal Device Sync Flow — Q&A and Stored Procedure Mapping

### Q&A

- **Who publishes the first SQS message to `TealDestinationQueueGetDevicesURL`?**
  - **AltaworxTealAWSGetDevices Lambda self-publishes.** The flow is kicked off by EventBridge (no SQS event). The function computes the first work unit and then enqueues a message to the destination queue itself using the configured `TealDestinationQueueGetDevicesURL`.

- **What is the configured batch size limit for device groups?**
  - **API page size**: 100 devices per page. All list requests use `limit=100` (Teal’s documented max).
  - **Database bulk insert batch**: Uses `SQLConstant.BatchSize` for `SqlBulkCopy` (value configured centrally). No additional “device group” size beyond API paging and bulk copy batching.

- **AltaworxTealAWSGetDevices — exact API endpoint and rate limits**
  - **Endpoint**: `GET {BaseUrl}/{TealDevicesGetURL}?requestId=...&offset=...&limit=100`. `BaseUrl` comes from `usp_Teal_Get_AuthenticationByProviderId`; `TealDevicesGetURL` is from env and typically points to `api/v1/esims`.
  - **Rate limits/backoff**: HTTP calls use Polly with **3 retries** and **exponential backoff**: wait times are 3s, 9s, 27s per attempt. There is no explicit TPS cap in code; the policy is best-effort resilience.

- **How are delayed messages (30s, 5min) handled—CloudWatch visibility or explicit delay?**
  - **Explicit SQS delay**: Messages to `TealDestinationQueueGetDevicesURL` are sent with `DelaySeconds=30`.
  - **No 5-minute delay configured** and no custom visibility-timeout handling is present in the provided code.

- **Where are failed ICCIDs logged if device fetch fails?**
  - **Page-level logging to CloudWatch**: Failures while fetching device pages or operation results are logged via the Lambda logger; there is no per-ICCID failure logging during fetch.
  - **Merge behavior**: During SQL MERGE, devices missing from staging for a provider are marked with status `Unknown`. No DLQ-specific logging was found for ICCIDs.

- **Retry configuration for Polly SQL/HTTP retries**
  - **HTTP**: Polly `WaitAndRetryAsync` with 3 attempts and exponential backoff (3s, 9s, 27s) plus a fallback that marks the result as error.
  - **SQL**: Stored procedure executions are wrapped in a SQL transient retry policy via `RetryPolicyHelper.GetSqlTransientPolicy(...)`.

- **What happens if a group remains partially unprocessed after retries?**
  - If API errors reach a threshold (**5 failures**), the Lambda stops paging for that provider, treats the device step as completed, runs the SQL MERGE from staging, triggers device-usage fetch, and proceeds to the next provider. Partial data may remain for that cycle.

### Stored Procedures (SPs) — What they do and where used

- **`usp_Teal_Truncate_Device_And_Usage_Staging`**
  - **What**: Truncates `TealDeviceStaging`, `TealDeviceUsageStaging`, `TealDeviceUsageDailyStaging`, and `TealDeviceSMSUsageStaging`.
  - **Where**: Called at the start of a provider sync (EventBridge kickoff path), wrapped in SQL transient retry policy within the Lambda.

- **`usp_Teal_Update_Device_From_Staging`**
  - **What**: MERGE from staging into `TealDevice` for the provider. Updates existing devices; inserts new ones; marks non-present devices as `Unknown`. Inserts an audit row in `TealDeviceSyncAudit` with counts and billing period info.
  - **Where**: Invoked after finishing device paging (or hitting failure/timeout thresholds) before moving to device-usage and the next provider; wrapped in SQL transient retry policy.

- **`usp_Teal_Get_AuthenticationByProviderId`**
  - **What**: Returns `BaseUrl`, `APIKey`, `APISecret`, `WriteIsEnabled`, and billing period settings for the given provider.
  - **Where**: Used by the repository to construct the Teal API authentication context before making API calls.

### Configuration keys and constants

- **Environment variables**
  - **`TealDevicesGetURL`**: Relative endpoint segment for device list (e.g., `api/v1/esims`).
  - **`TealDestinationQueueGetDevicesURL`**: SQS queue URL for device list processing.
  - **`TealDeviceUsageQueueURL`**: SQS queue URL for device usage processing.

- **Operational constants**
  - **`PAGE_SIZE`**: 100 (request limit sent to Teal).
  - **`RETRY_NUMBER`**: 3 (Polly retry attempts).
  - **`DELAY_SQS_MESSAGE_IN_SECONDS`**: 30 (explicit SQS delay per message).
  - **`TEAL_SYNC_FAIL_ACCEPTABLE_LIMIT`**: 5 (page-level failure threshold before early completion).
  - **`SQLConstant.BatchSize`**: Bulk copy batch size (configured centrally and applied during `SqlBulkCopy`).

### End-to-end flow (concise)

- EventBridge triggers the Lambda → truncates Teal staging tables → fetches provider auth → calls Teal devices list in pages of 100 with retries/backoff.
- Accumulates page entries into staging; on completion or failure threshold, runs the MERGE to main tables and writes audit; enqueues usage job and advances to next provider.
- SQS is used to continue paging and chain steps; each device page message is delayed by 30 seconds when sent.

