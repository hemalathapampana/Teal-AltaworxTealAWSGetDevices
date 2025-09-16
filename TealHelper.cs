using System;
using System.Net.Http;
using Amop.Core.Constants;
using Amop.Core.Logger;
using Amop.Core.Models.DeviceBulkChange;
using Polly;

namespace Amop.Core.Helpers.Teal
{
    public class TealHelper
    {
        public static class CommonConfig
        {
            public const int PAGE_SIZE = 100;
            public const int RETRY_NUMBER = 3;
        }
        public static class CommonString
        {
            public const string TEAL_DEVICES_GET_URL = "TealDevicesGetURL";
            public const string TEAL_RATE_PLANS_URL = "TealRatePlansGetURL";
            public const string TEAL_ASSIGN_RATE_PLAN_URL = "TealAssignRatePlanURL";
            public const string TEAL_DESTINATION_QUEUE_GET_DEVICES_URL = "TealDestinationQueueGetDevicesURL";
            public const string TEAL_DESTINATION_QUEUE_GET_RATE_PLANS_URL = "TealDestinationQueueGetRatePlansURL";
            public const string TEAL_DEVICE_USAGE_QUEUE_URL = "TealDeviceUsageQueueURL";
            public const string TEAl_DEVICE_API = "api/v1/esims";
            public const string TEAl_DEVICE_DATA_USAGE_API = "api/v1/data-consumption/data";
            public const string TEAl_DEVICE_SMS_USAGE_API = "api/v1/data-consumption/sms";
            public const string TEAl_CARRIER_RATE_PLAN_API = "api/v1/plans";
            public const string TEAL_DEVICES_DATA_USAGE_URL = "TealDeviceDataUsageURL";
            public const string TEAL_DEVICES_SMS_DATA_USAGE_URL = "TealDeviceSMSDataUsageURL";
            public const string CLEAN_UP_QUEUE_URL = "CleanUpQueueURL";
            public const string TEAL_GET_DEVICE = "GetDevice";
            public const string TEAL_GET_DEVICE_USAGE = "GetUsage";
            public const string TEAL_GET_RATE_PLAN = "GetRatePlan";
            public const string TEAL_ASSIGN_RATE_PLAN = "AssignRatePlan";
            public const string TEAL_GET_DEVICE_SMS_USAGE = "GetSMSUsage";
            public const string TEAL_UPDATE_DEVICE_STATUS = "UpdateStatus";
            public const string DATE_TIME_FORMAT_FOR_BUILD_REQUEST_ID = "yyyyMMddHHmmss";
            public const string REQUEST_ID = "requestId";
            public const string ENTRIES = "entries";
            public const string OFFSET = "offset";
            public const string LIMIT = "limit";
            public const string API_KEY = "ApiKey";
            public const string API_SECRET = "ApiSecret";
            public const string PUT = "PUT";
            public const string GET = "GET";
            public const string POST = "POST";
            public const string DATA_TYPE = "dataType";
            public const string PERIOD_START = "periodStart";
            public const string PERIOD_END = "periodEnd";
            public const string TEAL_PERIOD_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
            public const string WRITES_DISABLE_FOR_THIS_SERVICE_PROVIDER = "{\"errorMessage\": \"Writes are disabled for this service provider\"}";
            public const string REQUEST_DEVICE_LIST_TO_BE_FETCHED_FROM_TEAL_PLATFORM = "Request device list to be fetched from Teal platform.";
            public const string REQUEST_UPDATE_DEVICE_STATUS_TO_TEAL_PLATFORM = "Request update device status to Teal platform.";
            public const string TEAL_DATA_TYPE_DAILY = "DAILY";
            public const string TEAL_DATA_TYPE_MONTHLY = "MONTHLY";
            public const string TEAL_DATA_TYPE_DAILY_PROVIDER_NETWORK = "DAILY_PROVIDER_NETWORK";
            public const string TEAL_DATA_TYPE_MONTH_PROVIDER_NETWORK = "MONTHLY_PROVIDER_NETWORK";
            public const string HTTP = "http:";
            public const string HTTPS = "https:";
            public const string ERROR_WHEN_GETTING_EXEPTION = "Error when getting {0}. Exception: {1}";
            public const string TEAL_DEVICE_INFORMATION = "Teal Device Information";
            public const string TEAL_CARRIER_RATE_PLAN = "Teal Carrier Rate Plan";
            public const string TEAL_DEVICE_USAGE = "Teal Device Usage";
            public const string OPERATION_RESULT = "Operation Result";
            public const string APPLICATION_ACCEPTED = "Accept";
        }
        public static IAsyncPolicy<DeviceChangeResult<string, string>> BuildRetryPolicy(string requestUrl, string requestType, IKeysysLogger logger = null)
        {
            var retryNumber = CommonConfig.RETRY_NUMBER;
            var fallbackPolicy = Policy<DeviceChangeResult<string, string>>
                .Handle<Exception>()
                .FallbackAsync(async cancellationToken =>
                {
                    var errorMessage = string.Format(LogCommonStrings.ERROR_WHEN_GETTING_SOMETHING, requestType);
                    if (!string.IsNullOrWhiteSpace(requestUrl))
                    {
                        errorMessage = $"{errorMessage} URL: {requestUrl}";
                    }
                    logger?.LogError(CommonConstants.EXCEPTION, $"{errorMessage} URL: {requestUrl}");
                    return new DeviceChangeResult<string, string>()
                    {
                        HasErrors = true
                    };
                });
            var httpRequestPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(
                    retryNumber,
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(retryNumber, retryAttempt)),
                    (exception, timeSpan, retryCount, httpContext) =>
                    {
                        logger?.LogInfo(CommonConstants.INFO, $"Retry Number: {retryCount}. Message: {exception.Message} + {exception?.InnerException?.Message}");
                    });

            return fallbackPolicy.WrapAsync(httpRequestPolicy);
        }
    }
}
