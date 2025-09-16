using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Helpers;
using Altaworx.AWS.Core.Helpers.Constants;
using Altaworx.AWS.Core.Models;
using Altaworx.AWS.Core.Repositories.Teal;
using Altaworx.AWS.Core.Services.Http;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amop.Core.Constants;
using Amop.Core.Helpers;
using Amop.Core.Helpers.Teal;
using Amop.Core.Logger;
using Amop.Core.Models;
using Amop.Core.Models.Teal;
using Amop.Core.Services.Base64Service;
using Amop.Core.Services.Http;
using Amop.Core.Services.Teal;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using System.Data;
using AMOPSQLConstants = Amop.Core.Constants.SQLConstant;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AltaworxTealAWSGetDevices
{
    public class Function : AwsFunctionBase
    {
        private string _tealGetDevicesPath = Environment.GetEnvironmentVariable(TealHelper.CommonString.TEAL_DEVICES_GET_URL);
        private string _tealDestinationQueueGetDevicesURL = Environment.GetEnvironmentVariable(TealHelper.CommonString.TEAL_DESTINATION_QUEUE_GET_DEVICES_URL);
        private string _tealDeviceUsageQueueURL = Environment.GetEnvironmentVariable(TealHelper.CommonString.TEAL_DEVICE_USAGE_QUEUE_URL);
        private TealRepository _tealRepository = null;
        private const int DELAY_SQS_MESSAGE_IN_SECONDS = 30;
        private const int TEAL_SYNC_FAIL_ACCEPTABLE_LIMIT = 5;
        /// <summary>
        /// In this lambda we will implement for three main steps:
        /// 1. The lambda will be trigged by EventBridge to truncate old Teal data (device information and device usage). Then get Teal ServiceProvider for sync new data
        /// 2. The lambda will be trigged by SQS to call Teal API to get all devices information. Then sync new data into the staging table
        /// 3. Retrigger this lambda to check if there still have more devices that need to be synced or not.
        /// 3.1 If Yes then back to step 2
        /// 3.2 If No then call the Teal Device Usage to sync data usage for Teal Devices
        /// </summary>
        /// <param name="sqsEvent"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
        {
            KeySysLambdaContext lambdaContext = null;
            try
            {
                lambdaContext = BaseFunctionHandler(context);
                _tealRepository = new TealRepository(lambdaContext);

                if (string.IsNullOrWhiteSpace(_tealGetDevicesPath))
                {
                    _tealGetDevicesPath = context.ClientContext.Environment[TealHelper.CommonString.TEAL_DEVICES_GET_URL];
                    LogInfo(lambdaContext, LogTypeConstant.Exception, string.Format(LogCommonStrings.ENVIRONMENT_VARIABLE_NOT_DEFINED, TealHelper.CommonString.TEAL_DEVICES_GET_URL));
                }
                if (string.IsNullOrWhiteSpace(_tealDestinationQueueGetDevicesURL))
                {
                    _tealDestinationQueueGetDevicesURL = context.ClientContext.Environment[TealHelper.CommonString.TEAL_DESTINATION_QUEUE_GET_DEVICES_URL];
                    LogInfo(lambdaContext, LogTypeConstant.Exception, string.Format(LogCommonStrings.ENVIRONMENT_VARIABLE_NOT_DEFINED, TealHelper.CommonString.TEAL_DESTINATION_QUEUE_GET_DEVICES_URL));
                }
                if (string.IsNullOrWhiteSpace(_tealDeviceUsageQueueURL))
                {
                    _tealDeviceUsageQueueURL = context.ClientContext.Environment[TealHelper.CommonString.TEAL_DEVICE_USAGE_QUEUE_URL];
                    LogInfo(lambdaContext, LogTypeConstant.Exception, string.Format(LogCommonStrings.ENVIRONMENT_VARIABLE_NOT_DEFINED, TealHelper.CommonString.TEAL_DEVICE_USAGE_QUEUE_URL));
                }

                if (sqsEvent != null && sqsEvent.Records != null)
                {
                    var processedRecordCount = sqsEvent.Records.Count;
                    LogInfo(lambdaContext, LogTypeConstant.Info, string.Format(LogCommonStrings.BEGINNING_PROCESS, processedRecordCount));
                    foreach (var record in sqsEvent.Records)
                    {
                        LogInfo(lambdaContext, LogTypeConstant.Info, $"MessageId: {record.MessageId}");
                        var sqsValues = new SqsValues(lambdaContext, record);
                        await TryProcessDeviceList(lambdaContext, sqsValues);
                    }
                }
                else
                {
                    var sqsValues = new SqsValues();
                    await TryProcessDeviceList(lambdaContext, sqsValues);
                }

                LogInfo(lambdaContext, LogTypeConstant.Info, string.Format(LogCommonStrings.PROCESSED_DEVICE, CommonConstants.TEAL_CARRIER_NAME));
            }
            catch (Exception ex)
            {
                context.Logger.LogLine($"{LogTypeConstant.Exception}: {ex.Message} - {ex.StackTrace}");
            }

            CleanUp(lambdaContext);
        }

        private async Task TryProcessDeviceList(KeySysLambdaContext context, SqsValues sqsValues)
        {
            try
            {
                if (sqsValues.CurrentServiceProviderId == 0)
                {
                    var serviceProviderId = ServiceProviderCommon.GetNextServiceProviderId(context.CentralDbConnectionString, IntegrationType.Teal, sqsValues.CurrentServiceProviderId);
                    if (serviceProviderId > 0)
                    {
                        TruncateTealDeviceAndUsageStagingWithPolicy(context);
                        sqsValues.CurrentServiceProviderId = serviceProviderId;
                    }
                    else
                    {
                        LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.ERROR_GETTING_SERVICE_PROVIDER, CommonConstants.TEAL_CARRIER_NAME));
                        return;
                    }
                }

                await ProcessDeviceList(context, sqsValues);
            }
            catch (Exception ex)
            {
                LogInfo(context, LogTypeConstant.Exception, JsonConvert.SerializeObject(ex));
            }

        }

        private void TruncateTealDeviceAndUsageStagingWithPolicy(KeySysLambdaContext context)
        {
            LogInfo(context, LogTypeConstant.Sub, "");

            var errorMessages = new List<string>();
            var sqlTransientRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(context.logger, errorMessages);
            sqlTransientRetryPolicy.Execute(() => TruncateTealDeviceAndUsageStaging(context));
        }

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

        private async Task ProcessDeviceList(KeySysLambdaContext context, SqsValues sqsValues)
        {
            LogInfo(context, LogTypeConstant.Sub, $"Page offset: {sqsValues.PageNumber}, Page size: {TealHelper.CommonConfig.PAGE_SIZE}");

            var tealAuthentication = _tealRepository.GetTealAuthenticationInformation(sqsValues.CurrentServiceProviderId);

            var base64Service = new Base64Service();
            var tealService = new TealAPIService(tealAuthentication, base64Service, new TealHttpSingleton(), new HttpRequestFactory());
            bool isLastPage = false;
            var tealDeviceDataTable = BuildTealDeviceDataTable();
            int failCount = 0;
            bool shouldStopSyncStep = false;

            while (!isLastPage)
            {
                if (context.Context.RemainingTime < TimeSpan.FromSeconds(CommonConstants.REMAINING_TIME_CUT_OFF))
                {
                    LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.NOT_ENOUGH_TIME_TO_CONTINUE_PROCESS_WITH_REMAINING_SECONDS, context.Context.RemainingTime));
                    break;
                }
                shouldStopSyncStep = failCount >= TEAL_SYNC_FAIL_ACCEPTABLE_LIMIT;
                if (shouldStopSyncStep)
                {
                    LogInfo(context, CommonConstants.ERROR, string.Format(LogCommonStrings.CALLING_API_FAILED_FOR_PAGES, CommonConstants.TEAL_CARRIER_NAME, failCount));
                    break;
                }
                var tealDeviceResult = await TealHelper.BuildRetryPolicy(string.Empty, TealHelper.CommonString.TEAL_DEVICE_INFORMATION, context.logger)
                        .ExecuteAsync(async () => await tealService.RequestTealListAsync(_tealGetDevicesPath, TealHelper.CommonString.TEAL_GET_DEVICE, sqsValues.PageNumber, TealHelper.CommonConfig.PAGE_SIZE, context.logger));
                if (tealDeviceResult.HasErrors)
                {
                    LogInfo(context, LogTypeConstant.Error, string.Format(LogCommonStrings.ERROR_REQUESTING_DEVICES, CommonConstants.TEAL_CARRIER_NAME));
                    failCount++;
                }
                else
                {
                    var tealApiResponse = JsonConvert.DeserializeObject<TealAPIResponse>(tealDeviceResult.ResponseObject);
                    TealDeviceResponseRootObject tealDeviceResponse = null;

                    if (tealApiResponse.Success)
                    {
                        var operationResultLink = tealApiResponse.Link.OperationResultLink.Href;
                        operationResultLink = operationResultLink.Replace(TealHelper.CommonString.HTTP, TealHelper.CommonString.HTTPS);
                        // Retry logic inside the TealAPIService implementation
                        var tealDeviceAPIResult = await tealService.GetTealOperationResultAsync(operationResultLink, context.logger);

                        if (tealDeviceAPIResult.HasErrors)
                        {
                            LogInfo(context, LogTypeConstant.Error, string.Format(LogCommonStrings.ERROR_REQUESTING_DEVICES, CommonConstants.TEAL_CARRIER_NAME));
                            failCount++;
                        }
                        else
                        {
                            tealDeviceResponse = JsonConvert.DeserializeObject<TealDeviceResponseRootObject>(tealDeviceAPIResult.ResponseObject);
                            if (tealDeviceResponse?.Entries != null && tealDeviceResponse.Entries.Count > 0)
                            {
                                foreach (var tealDevice in tealDeviceResponse.Entries)
                                {
                                    var tealDeviceDataRow = AddToDataRow(sqsValues, tealDeviceDataTable, tealDevice);
                                    tealDeviceDataTable.Rows.Add(tealDeviceDataRow);
                                }

                                LogInfo(context, LogTypeConstant.Info, $"Page {sqsValues.PageNumber} has {tealDeviceResponse.Entries.Count} devices.");
                                sqsValues.PageNumber = sqsValues.PageNumber + 1;
                            }
                            else if ((tealDeviceResponse?.Entries == null || tealDeviceResponse.Entries.Count <= 0) && sqsValues.PageNumber == 0)
                            {
                                LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.SERVICE_PROVIDER_WITHOUT_DEVICES, sqsValues.CurrentServiceProviderId));
                                return;
                            }
                            else
                            {
                                isLastPage = true;
                                break;
                            }
                        }
                    }
                    else
                    {
                        LogInfo(context, LogTypeConstant.Error, $"{tealApiResponse.Message}");
                        failCount++;
                    }
                }
            }

            if (tealDeviceDataTable.Rows.Count > 0)
            {
                LogInfo(context, LogTypeConstant.Status, LogCommonStrings.SQL_BULK_COPY_START);
                SqlBulkCopy(context, context.CentralDbConnectionString, tealDeviceDataTable, DatabaseTableNames.TealDeviceStaging);
            }

            // The API calls fail in this instance is over a certain limit, should consider this sync instance as bad result and stop the sync
            if (shouldStopSyncStep)
            {
                // Mark as true to completed sync Teal device information
                isLastPage = true;
            }

            if (isLastPage)
            {
                await CompleteProcessGetDevice(context, sqsValues, tealAuthentication);
            }
            else
            {
                await SendMessageToQueue(context, sqsValues);
            }
        }

        private async Task CompleteProcessGetDevice(KeySysLambdaContext context, SqsValues sqsValues, TealAuthentication tealAuthentication)
        {
            LogInfo(context, LogTypeConstant.Sub, $"({sqsValues.PageNumber}, {sqsValues.CurrentServiceProviderId})");
            UpdateTealDevicesWithPolicy(context, sqsValues.CurrentServiceProviderId, context.CentralDbConnectionString, tealAuthentication, context.logger);
            await SendMessageToGetDeviceUsageQueue(context, _tealDeviceUsageQueueURL, sqsValues.CurrentServiceProviderId);

            var nextServiceProvider = ServiceProviderCommon.GetNextServiceProviderId(context.CentralDbConnectionString, IntegrationType.Teal, sqsValues.CurrentServiceProviderId);
            if (nextServiceProvider > 0)
            {
                LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.NEXT_SERVICE_PROVIDER_ID, nextServiceProvider));
                sqsValues.CurrentServiceProviderId = nextServiceProvider;
                sqsValues.PageNumber = 0;

                await SendMessageToQueue(context, sqsValues);
            }
        }

        private DataTable BuildTealDeviceDataTable()
        {
            DataTable tealDeviceTable = new DataTable();
            tealDeviceTable.Columns.Add(CommonColumnNames.Id);
            tealDeviceTable.Columns.Add(CommonColumnNames.ICCID);
            tealDeviceTable.Columns.Add(CommonColumnNames.IMSI);
            tealDeviceTable.Columns.Add(CommonColumnNames.IMEI);
            tealDeviceTable.Columns.Add(CommonColumnNames.MSISDN);
            tealDeviceTable.Columns.Add(CommonColumnNames.EID);
            tealDeviceTable.Columns.Add(CommonColumnNames.DeviceStatus);
            tealDeviceTable.Columns.Add(CommonColumnNames.PlanId);
            tealDeviceTable.Columns.Add(CommonColumnNames.PlanUuid);
            tealDeviceTable.Columns.Add(CommonColumnNames.PlanName);
            tealDeviceTable.Columns.Add(CommonColumnNames.PlanVolumeUnit);
            tealDeviceTable.Columns.Add(CommonColumnNames.PlanPrice);
            tealDeviceTable.Columns.Add(CommonColumnNames.Suspended);
            tealDeviceTable.Columns.Add(CommonColumnNames.DeviceName);
            tealDeviceTable.Columns.Add(CommonColumnNames.PlanChangeStatus);
            tealDeviceTable.Columns.Add(CommonColumnNames.ClientId);
            tealDeviceTable.Columns.Add(CommonColumnNames.ClientName);
            tealDeviceTable.Columns.Add(CommonColumnNames.ClientTitle);
            tealDeviceTable.Columns.Add(CommonColumnNames.ClientUuid);
            tealDeviceTable.Columns.Add(CommonColumnNames.Sku);
            tealDeviceTable.Columns.Add(CommonColumnNames.BatchNumber);
            tealDeviceTable.Columns.Add(CommonColumnNames.DeviceGroupName);
            tealDeviceTable.Columns.Add(CommonColumnNames.ClientChangeTimestamp);
            tealDeviceTable.Columns.Add(CommonColumnNames.ProfileChangeStatus);
            tealDeviceTable.Columns.Add(CommonColumnNames.FlowType);
            tealDeviceTable.Columns.Add(CommonColumnNames.ServiceProviderId);
            tealDeviceTable.Columns.Add(CommonColumnNames.CreatedDate);

            return tealDeviceTable;
        }

        private DataRow AddToDataRow(SqsValues sqsValues, DataTable table, TealDevice device)
        {
            var tealDeviceDataRow = table.NewRow();

            tealDeviceDataRow[CommonColumnNames.ICCID] = device.ICCID;
            tealDeviceDataRow[CommonColumnNames.IMSI] = device.IMSI;
            tealDeviceDataRow[CommonColumnNames.IMEI] = device.IMSI;
            tealDeviceDataRow[CommonColumnNames.MSISDN] = device.MSISDN;
            tealDeviceDataRow[CommonColumnNames.EID] = device.EID;
            tealDeviceDataRow[CommonColumnNames.DeviceStatus] = device.DeviceStatus;
            tealDeviceDataRow[CommonColumnNames.PlanId] = device.PlanId;
            tealDeviceDataRow[CommonColumnNames.PlanUuid] = device.PlanUuid;
            tealDeviceDataRow[CommonColumnNames.PlanName] = device.PlanName;
            tealDeviceDataRow[CommonColumnNames.PlanVolumeUnit] = device.PlanVolumeUnit;
            tealDeviceDataRow[CommonColumnNames.PlanPrice] = device.PlanPrice;
            tealDeviceDataRow[CommonColumnNames.Suspended] = device.Suspended;
            tealDeviceDataRow[CommonColumnNames.DeviceName] = device.DeviceName;
            tealDeviceDataRow[CommonColumnNames.PlanChangeStatus] = device.PlanChangeStatus;
            tealDeviceDataRow[CommonColumnNames.ClientId] = device.ClientId;
            tealDeviceDataRow[CommonColumnNames.ClientName] = device.ClientName;
            tealDeviceDataRow[CommonColumnNames.ClientTitle] = device.ClientTitle;
            tealDeviceDataRow[CommonColumnNames.ClientUuid] = device.ClientUuid;
            tealDeviceDataRow[CommonColumnNames.Sku] = device.Sku;
            tealDeviceDataRow[CommonColumnNames.BatchNumber] = device.BatchNumber;
            tealDeviceDataRow[CommonColumnNames.DeviceGroupName] = device.DeviceGroupName;
            tealDeviceDataRow[CommonColumnNames.ClientChangeTimestamp] = device.ClientChangeTimestamp;
            tealDeviceDataRow[CommonColumnNames.ProfileChangeStatus] = device.ProfileChangeStatus;
            tealDeviceDataRow[CommonColumnNames.FlowType] = device.FlowType;
            tealDeviceDataRow[CommonColumnNames.ServiceProviderId] = sqsValues.CurrentServiceProviderId;
            tealDeviceDataRow[CommonColumnNames.CreatedDate] = DateTime.UtcNow;

            return tealDeviceDataRow;
        }

        private void UpdateTealDevicesWithPolicy(KeySysLambdaContext context, int serviceProviderId, string connectionString, TealAuthentication tealAuthentication, IKeysysLogger logger)
        {
            logger.LogInfo(LogTypeConstant.Sub, $"{serviceProviderId}");
            var errorMessages = new List<string>();
            var sqlTransientRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(logger, errorMessages);
            sqlTransientRetryPolicy.Execute(() => UpdateTealDevicesFromStaging(context, serviceProviderId, connectionString, tealAuthentication, logger));
        }

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

        private async Task SendMessageToQueue(KeySysLambdaContext context, SqsValues sqsValues)
        {
            LogInfo(context, LogTypeConstant.Sub, $"({sqsValues.PageNumber}, {sqsValues.CurrentServiceProviderId})");

            //Verify the value of the URL used for send SQS message to the lambda GetTealDevices.
            if (string.IsNullOrWhiteSpace(_tealDestinationQueueGetDevicesURL))
            {
                return;
            }

            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
            {
                var requestMsgBody = string.Format(LogCommonStrings.GETTING_TEAL_DEVICES_WITH_PAGE_NUMBER_AND_SERVICE_PROVIDER, sqsValues.PageNumber, sqsValues.CurrentServiceProviderId);
                LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.SENDING_MESSAGE_TO_URL, _tealDestinationQueueGetDevicesURL));

                var request = new SendMessageRequest
                {
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {
                            "PageNumber", new MessageAttributeValue
                            { DataType = "String", StringValue = sqsValues.PageNumber.ToString()}
                        },
                        {
                            "CurrentServiceProviderId", new MessageAttributeValue
                            { DataType = "String", StringValue = sqsValues.CurrentServiceProviderId.ToString()}
                        }
                    },
                    DelaySeconds = (int)TimeSpan.FromSeconds(DELAY_SQS_MESSAGE_IN_SECONDS).TotalSeconds,
                    MessageBody = requestMsgBody,
                    QueueUrl = _tealDestinationQueueGetDevicesURL
                };

                LogInfo(context, LogTypeConstant.Status, LogCommonStrings.SEND_MESSAGE_REQUEST_READY);
                LogInfo(context, LogTypeConstant.Info, $"MessageBody: {request.MessageBody}");
                LogInfo(context, LogTypeConstant.Info, $"QueueURL: {request.QueueUrl}");

                var response = await client.SendMessageAsync(request);
                LogInfo(context, LogTypeConstant.Info, $"{response.HttpStatusCode} - {response.HttpStatusCode}");
            }
        }

        private async Task SendMessageToGetDeviceUsageQueue(KeySysLambdaContext context, string deviceUsageQueueURL, int serviceProviderId)
        {
            LogInfo(context, LogTypeConstant.Sub, $"({deviceUsageQueueURL}, {serviceProviderId})");

            //Verify the value of the URL used for send SQS message to the lambda get TealDeviceUsage.
            if (string.IsNullOrWhiteSpace(deviceUsageQueueURL))
            {
                return;
            }

            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
            {
                var requestMsgBody = string.Format(LogCommonStrings.GET_TEAL_DEVICE_USAGE_FOR_SERVICE_PROVIDER_ID, serviceProviderId);
                LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.SENDING_MESSAGE_TO_URL, deviceUsageQueueURL));

                var request = new SendMessageRequest
                {
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {
                            "ServiceProviderId", new MessageAttributeValue
                            {
                                DataType = "String", StringValue = serviceProviderId.ToString()
                            }
                        }
                    },
                    MessageBody = requestMsgBody,
                    QueueUrl = deviceUsageQueueURL
                };

                LogInfo(context, LogTypeConstant.Info, LogCommonStrings.SEND_MESSAGE_REQUEST_READY);
                LogInfo(context, LogTypeConstant.Info, $"MessageBody: {request.MessageBody}");
                LogInfo(context, LogTypeConstant.Info, $"QueueURL: {request.QueueUrl}");

                var response = await client.SendMessageAsync(request);
                LogInfo(context, LogTypeConstant.Info, $"{response.HttpStatusCode} - {response.HttpStatusCode}");
            }
        }

        public static BillingPeriod GetBillingPeriod(KeySysLambdaContext context, int serviceProviderId, DateTime currentDateTime, int billCycleEndDay, int billCycleEndHour)
        {
            LogInfo(context, LogTypeConstant.Sub, $"({serviceProviderId}, {billCycleEndHour}, {billCycleEndDay})");

            var centralDbConnectionString = context.CentralDbConnectionString;
            var billingPeriodYear = currentDateTime.Year;
            var billingPeriodMonth = currentDateTime.Month;

            //get billing end day and bill end hour in serviceProvider 
            var billingPeriod = BillingPeriodHelper.GetBillingPeriodForServiceProvider(centralDbConnectionString, serviceProviderId, billingPeriodYear,
                            billingPeriodMonth, null, billCycleEndDay, billCycleEndHour);

            // if billing end day in service provider < current day, + 1 month
            if (billingPeriod.BillingPeriodEndDay < currentDateTime.Day)
            {
                var endDate = billingPeriod.BillingPeriodEnd.AddMonths(1);
                billingPeriodYear = endDate.Year;
                billingPeriodMonth = endDate.Month;
                billingPeriod = BillingPeriodHelper.GetBillingPeriodForServiceProvider(centralDbConnectionString, serviceProviderId, billingPeriodYear,
                    billingPeriodMonth, null, billCycleEndDay, billCycleEndHour);
            }

            LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.NEXT_BILLING_DATE, billingPeriod.BillingPeriodEnd));
            return billingPeriod;
        }
    }
}
