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