## AltaworxTealAWSGetDevices — Flow, Stored Procedures, Retries, and Queues

### Overview
- **Lambda name**: `AltaworxTealAWSGetDevices`
- **Purpose**: Fetch Teal devices in pages, stage them, then merge to main tables; finally trigger device usage sync per Service Provider.
- **Triggers**:
  - EventBridge (schedule) to start a run when there is no SQS event
  - SQS (`TealDestinationQueueGetDevicesURL`) for paging and continuation

### Environment variables
- `TealDevicesGetURL` — API endpoint path (e.g., `api/v1/esims`)
- `TealDestinationQueueGetDevicesURL` — SQS URL for self-re-enqueue of next page
- `TealDeviceUsageQueueURL` — SQS URL to trigger device usage lambda after devices are complete

### Stored procedures used
1) `usp_Teal_Truncate_Device_And_Usage_Staging`
   - Purpose: Clear Teal staging tables at the start of a Service Provider sync
   - Tables truncated:
     - `[dbo].[TealDeviceStaging]`
     - `[dbo].[TealDeviceUsageStaging]`
     - `[dbo].[TealDeviceUsageDailyStaging]`
     - `[dbo].[TealDeviceSMSUsageStaging]`
   - Invocation in code:
```100:166:/workspace/AltaworxTealAWSGetDevices.cs
private void TruncateTealDeviceAndUsageStaging(KeySysLambdaContext context)
{
	LogInfo(context, LogTypeConstant.Sub, "");
	try
	{
		using (var connection = new SqlConnection(context.CentralDbConnectionString))
		{
			using (var command = new SqlCommand(AMOPSQLConstants.StoredProcedureName.usp_Teal_Truncate_Device_And_Usage_Staging, connection))
			{
				command.CommandType = CommandType.StoredProcedure;
				command.CommandTimeout = AMOPSQLConstants.ShortTimeoutSeconds;
				connection.Open();

				command.ExecuteNonQuery();
			}
		}
	}
	catch (SqlException ex)
	{
		LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_EXECUTING_SQL_COMMAND, ex.Message));
	}
	catch (InvalidOperationException ex)
	{
		LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_CONNECTING_DATABASE, ex.Message));
	}
	catch (Exception ex)
	{
		LogInfo(context, LogTypeConstant.Exception, ex.Message);
	}
}
```
   - Wrapped with SQL transient retry:
```128:135:/workspace/AltaworxTealAWSGetDevices.cs
private void TruncateTealDeviceAndUsageStagingWithPolicy(KeySysLambdaContext context)
{
	LogInfo(context, LogTypeConstant.Sub, "");

	var errorMessages = new List<string>();
	var sqlTransientRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(context.logger, errorMessages);
	sqlTransientRetryPolicy.Execute(() => TruncateTealDeviceAndUsageStaging(context));
}
```

2) `usp_Teal_Update_Device_From_Staging`
   - Purpose: Merge staged devices into `[dbo].[TealDevice]`, set Unknown for missing, add last sync and audit
   - Parameters: `@ServiceProviderId, @BillingCycleEndDay, @BillingCycleEndHour, @BillMonth, @BillYear, @NextBillCycleDate`
   - Invocation in code:
```368:409:/workspace/AltaworxTealAWSGetDevices.cs
private void UpdateTealDevicesFromStaging(KeySysLambdaContext context, int serviceProviderId, string connectionString, TealAuthentication tealAuthentication, IKeysysLogger logger)
{
	logger.LogInfo(LogTypeConstant.Sub, $"({serviceProviderId}, {tealAuthentication.BillPeriodEndDay}, {tealAuthentication.BillPeriodEndHour})");

	var billingPeriod = GetBillingPeriod(context, serviceProviderId, DateTime.UtcNow, tealAuthentication.BillPeriodEndDay, tealAuthentication.BillPeriodEndHour);
	try
	{
		using (var connection = new SqlConnection(connectionString))
		{
			using (var command = new SqlCommand(AMOPSQLConstants.StoredProcedureName.usp_Teal_Update_Device_From_Staging, connection))
			{
				command.CommandType = CommandType.StoredProcedure;
				connection.Open();
				command.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
				command.Parameters.AddWithValue("@BillingCycleEndDay", tealAuthentication.BillPeriodEndDay);
				command.Parameters.AddWithValue("@BillingCycleEndHour", tealAuthentication.BillPeriodEndHour);
				command.Parameters.AddWithValue("@BillMonth", billingPeriod.BillingPeriodMonth);
				command.Parameters.AddWithValue("@BillYear", billingPeriod.BillingPeriodYear);
				command.Parameters.AddWithValue("@NextBillCycleDate", billingPeriod.BillingPeriodEnd);
				command.CommandTimeout = AMOPSQLConstants.TimeoutSeconds;

				var affectedRows = command.ExecuteNonQuery();
				if (affectedRows > 0)
				{
					LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.DEVICE_WAS_UPDATED_SUCCESSFULLY, CommonConstants.TEAL_CARRIER_NAME));
				}
			}
		}
	}
	catch (SqlException ex)
	{
		LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_EXECUTING_SQL_COMMAND, ex.Message));
	}
	catch (InvalidOperationException ex)
	{
		LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_CONNECTING_DATABASE, ex.Message));
	}
	catch (Exception ex)
	{
		LogInfo(context, LogTypeConstant.Exception, ex.Message);
	}
}
```
   - Wrapped with SQL transient retry:
```360:366:/workspace/AltaworxTealAWSGetDevices.cs
private void UpdateTealDevicesWithPolicy(KeySysLambdaContext context, int serviceProviderId, string connectionString, TealAuthentication tealAuthentication, IKeysysLogger logger)
{
	logger.LogInfo(LogTypeConstant.Sub, $"{serviceProviderId}");
	var errorMessages = new List<string>();
	var sqlTransientRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(logger, errorMessages);
	sqlTransientRetryPolicy.Execute(() => UpdateTealDevicesFromStaging(context, serviceProviderId, connectionString, tealAuthentication, logger));
}
```

3) `usp_Teal_Get_AuthenticationByProviderId`
   - Purpose: Fetch base URL, API key/secret, and billing config for a Service Provider
   - Invocation in code:
```24:47:/workspace/TealRepository.cs
using (var sqlCommand = new SqlCommand(Amop.Core.Constants.SQLConstant.StoredProcedureName.usp_Teal_Get_AuthenticationByProviderId, sqlConnection))
{
	sqlCommand.CommandType = CommandType.StoredProcedure;
	sqlCommand.Parameters.AddWithValue("@providerId", serviceProviderId);
	sqlCommand.CommandTimeout = Amop.Core.Constants.SQLConstant.ShortTimeoutSeconds;
	SqlDataReader authenticationDataReader = sqlCommand.ExecuteReader();
	while (authenticationDataReader.Read())
	{
		tealAuthentication = new TealAuthentication()
		{
			IntegrationAuthenticationId = int.Parse(authenticationDataReader[CommonColumnNames.IntegrationAuthenticationId].ToString()),
			BaseUrl = authenticationDataReader[CommonColumnNames.BaseUrl].ToString(),
			APIKey = authenticationDataReader[CommonColumnNames.APIKey].ToString(),
			APISecret = authenticationDataReader[CommonColumnNames.APISecret].ToString(),
			WriteIsEnabled = Convert.ToBoolean(authenticationDataReader.GetOrdinal(CommonColumnNames.WriteIsEnabled)),
			BillPeriodEndDay = int.Parse(authenticationDataReader[CommonColumnNames.BillPeriodEndDay].ToString()),
			BillPeriodEndHour = int.Parse(authenticationDataReader[CommonColumnNames.BillPeriodEndHour].ToString())
		};
	}
}
```

### Device fetch and staging
- Page size: `100` (`TealHelper.CommonConfig.PAGE_SIZE`)
- Listing call: `TealAPIService.RequestTealListAsync(TealDevicesGetURL, "GetDevice", pageNumber, pageSize)`
- Then follow-up operation result call: `GetTealOperationResultAsync(operationResultLink)`
- Data is accumulated into a DataTable and bulk-copied to `[dbo].[TealDeviceStaging]`:
```252:256:/workspace/AltaworxTealAWSGetDevices.cs
if (tealDeviceDataTable.Rows.Count > 0)
{
	LogInfo(context, LogTypeConstant.Status, LogCommonStrings.SQL_BULK_COPY_START);
	SqlBulkCopy(context, context.CentralDbConnectionString, tealDeviceDataTable, DatabaseTableNames.TealDeviceStaging);
}
```

### Pagination and completion
- Continues incrementing `PageNumber` while entries are returned
- Stops when `Entries` empty (after first page) or when failures reach acceptable limit (5) or near-timeout
- On completion: runs update SP and enqueues usage sync; if more Service Providers exist, enqueues next SP at page 0

### Queues and messages
- Re-enqueue devices:
  - Queue: `TealDestinationQueueGetDevicesURL`
  - Attributes: `PageNumber`, `CurrentServiceProviderId`
  - Delay: `30s` (`DelaySeconds = 30`)
- Trigger device usage:
  - Queue: `TealDeviceUsageQueueURL`
  - Attributes: `ServiceProviderId`

### Retry policies
- SQL: `RetryPolicyHelper.GetSqlTransientPolicy(...)` wraps truncate and update SP executions
- HTTP: `RetryPolicyHelper.PollyRetryHttpRequestAsync(logger, 3)` inside `TealAPIService` for GET/POST
- Additional wrapper: `TealHelper.BuildRetryPolicy(...)` (3 retries with 3s/9s/27s backoff) around initial list request

### Billing period calculation (for update SP)
- Uses `GetBillingPeriod(...)` to compute `@BillMonth`, `@BillYear`, `@NextBillCycleDate` from SP’s configured end day/hour

### Summary of SQL side-effects
- Truncate SP clears all Teal staging tables at start
- Update SP merges latest staged rows (by most recent CreatedDate per EID), inserts new, marks missing as Unknown, records last sync and audit rows
- Auth SP supplies credentials and billing cycle inputs for the run

---

## Q&A: Detailed answers to all earlier questions

- **What triggers this Lambda—another Lambda, schedule, or manual?**
  - Schedule via EventBridge starts a run with no SQS record; subsequent paging is triggered by SQS messages on `TealDestinationQueueGetDevicesURL`.

- **Why is SQL retry done first, and what issue does it prevent?**
  - The initial truncate and final update are wrapped by a SQL transient retry policy to avoid failing a run due to transient DB errors and to ensure staging is reliably cleared before new data is written.

- **Are device and BAN staging tables cleared at the start?**
  - Device and usage staging are cleared (`TealDeviceStaging`, `TealDeviceUsageStaging`, `TealDeviceUsageDailyStaging`, `TealDeviceSMSUsageStaging`). BAN is not part of this Lambda.

- **Aren’t staging tables already cleared after the previous run?**
  - The Lambda does not assume that; it explicitly truncates at the start of each Service Provider sync.

- **Which staging table stores BAN, FAN, and Number statuses?**
  - Not applicable. This Lambda only writes to `TealDeviceStaging`; BAN/FAN/Number statuses are not handled here.

- **Are BAN list statuses read from BillingAccountNumberStatusStaging or elsewhere?**
  - Not used. No BAN list status reads occur in this flow.

- **What is the page size/limit?**
  - 100 (`TealHelper.CommonConfig.PAGE_SIZE = 100`).

- **How does the system know all API pages are processed?**
  - When a page returns no `Entries` (after page 0), it marks `isLastPage = true`. It also stops if fail count hits 5 or remaining Lambda time is too low.

- **What parameters are used in GetTelegenceDeviceBySubscriberNumber?**
  - Not part of this Lambda.

- **What happens to devices that fail validation?**
  - The Lambda does not perform device-level validation. All fetched devices are staged. Validation/merge logic is executed within `usp_Teal_Update_Device_From_Staging` (e.g., mapping statuses and marking missing as Unknown).

- **What is the retry setup for Polly (attempts, delay)?**
  - HTTP: `RetryPolicyHelper.PollyRetryHttpRequestAsync(logger, 3)` inside `TealAPIService` (3 attempts; helper defines delay strategy).
  - Wrapper: `TealHelper.BuildRetryPolicy` provides 3 retries with exponential delays 3s, 9s, 27s, with a fallback that marks the operation as error.

- **How is re-enqueuing handled for incomplete or timed-out device lists?**
  - If more pages remain or nearing timeout, it sends a new SQS message to `TealDestinationQueueGetDevicesURL` with attributes `PageNumber` and `CurrentServiceProviderId`, delayed by 30s.

- **How do the stored procedures work in the flow?**
  - Start-of-SP sync: truncate SP clears staging.
  - After paging: bulk copy to staging.
  - Completion: update-from-staging SP merges into main tables, marks missing as Unknown, updates bill cycle metadata and writes audit/last-sync rows.

- **What details are captured in the summary logs?**
  - Start/end, page number and size, device count per page, bulk copy start, SQL/HTTP errors, not-enough-time messages, failure-count stop, enqueue details (message body and queue URL), completion message, next Service Provider id.

- **How are reference items (functions, queues, procedures) used in the flow?**
  - Functions/classes: `TealAPIService`, `TealRepository`, `AwsFunctionBase.SqlBulkCopy`, `ServiceProviderCommon.GetNextServiceProviderId`.
  - Queues: `TealDestinationQueueGetDevicesURL` for paging; `TealDeviceUsageQueueURL` for usage step.
  - Procedures: as detailed above.

- **Can you detail all Lambdas covering these points for all carriers?**
  - Scope here is only Teal (`AltaworxTealAWSGetDevices`). Other carriers are not part of this answer.

- **Who publishes the first SQS message to ThingSpaceDeviceQueueURL?**
  - Not applicable. ThingSpace is Verizon. Teal flow uses `TealDestinationQueueGetDevicesURL` and `TealDeviceUsageQueueURL`.

- **What is the configured batch size limit for device groups?**
  - Not applicable to Teal device fetch. SQL bulk copy batch size is controlled by `SQLConstant.BatchSize` (shared constant).

- **For AltaworxTealAWSGetDevices, provide the exact API endpoint and rate limits.**
  - Endpoint pattern: `{BaseUrl}/{TealDevicesGetURL}?requestId=...&offset=...&limit=...`.
  - `TealDevicesGetURL` typically resolves to `api/v1/esims`. Rate limits are not set here; only page-size cap (100) and retries are handled.

- **How are delayed messages (30s, 5min) handled—CloudWatch visibility or explicit delay?**
  - Explicit SQS delay: `DelaySeconds = 30` on send. No 5-minute delay logic in this Lambda.

- **Where are failed ICCIDs logged if device fetch fails?**
  - Failures are logged at the request/page level. There is no per-ICCID failure logging in this Lambda.

- **Provide retry configuration for Polly SQL/HTTP retries.**
  - SQL: `RetryPolicyHelper.GetSqlTransientPolicy(...)` wraps stored procedure calls.
  - HTTP: `RetryPolicyHelper.PollyRetryHttpRequestAsync(logger, 3)` for HTTP calls.
  - Additional wrapper: `TealHelper.BuildRetryPolicy` (3/9/27 seconds backoff; 3 attempts) on the initial list request.

- **What happens if a group remains partially unprocessed after retries?**
  - If `failCount >= 5`, the device-sync step stops early; the Lambda finalizes the run (executes the update SP), triggers the usage step, and proceeds to the next Service Provider, leaving the partial page(s) unprocessed in that run.

---

## Stored Procedure Behavior (from provided SQL)

- **[dbo].[usp_Teal_Truncate_Device_And_Usage_Staging]**
  - Truncates: `TealDeviceStaging`, `TealDeviceUsageStaging`, `TealDeviceUsageDailyStaging`, `TealDeviceSMSUsageStaging`.

- **[dbo].[usp_Teal_Update_Device_From_Staging] (@ServiceProviderId, @BillingCycleEndDay, @BillingCycleEndHour, @BillMonth, @BillYear, @NextBillCycleDate)**
  - Inserts last-sync snapshot into `TealDeviceDetailLastSyncDate` with queue count.
  - Merges from top-1-per-EID (latest CreatedDate) staging rows into `TealDevice`:
    - MATCHED: updates identifiers, plan, status, client info, SKU, billing metadata; sets `LastActivatedDate` when status changes to Activated.
    - NOT MATCHED BY TARGET (for the SP): inserts new active devices with billing metadata.
    - NOT MATCHED BY SOURCE (for the SP): marks missing devices as `Unknown` with `UnknownStatusId`.
  - Inserts an audit summary row into `TealDeviceSyncAudit` pivoting counts of `ACTIVATED` and `DEACTIVATED` per SP; computes bill month/year based on cycle end day/hour.

- **[dbo].[usp_Teal_Get_AuthenticationByProviderId] (@providerId)**
  - Returns `IntegrationAuthenticationId`, `BaseUrl`, `APIKey`, `APISecret`, `WriteIsEnabled`, `BillPeriodEndDay`, `BillPeriodEndHour` for Teal (IntegrationId 12) and the given Service Provider.