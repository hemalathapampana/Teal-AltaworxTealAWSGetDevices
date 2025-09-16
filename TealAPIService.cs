using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Amop.Core.Constants;
using Amop.Core.Helpers;
using Amop.Core.Helpers.Teal;
using Amop.Core.Logger;
using Amop.Core.Models.DeviceBulkChange;
using Amop.Core.Models.Teal;
using Amop.Core.Services.Base64Service;
using Amop.Core.Services.Http;
using Newtonsoft.Json;

namespace Amop.Core.Services.Teal
{
    public class TealAPIService
    {
        private readonly TealAuthentication _tealAuthentication;
        private readonly IBase64Service _base64Service;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly IHttpRequestFactory _httpRequestFactory;

        public TealAPIService(TealAuthentication tealAuthentication, IBase64Service base64Service, IHttpClientFactory httpClientFactory, IHttpRequestFactory httpRequestFactory)
        {
            _tealAuthentication = tealAuthentication;
            _base64Service = base64Service;
            _httpClientFactory = httpClientFactory;
            _httpRequestFactory = httpRequestFactory;
        }

        public async Task<DeviceChangeResult<string, string>> RequestTealListAsync(string tealEndpoint, string requestType, int pageNumber, int pageSize = 100, IKeysysLogger logger = null)
        {
            // In the swagger of Teal said Maximum value of the PageSize is 100. Link swagger https://integrationapi.teal.global/swagger-ui.html#/Esim%20Operations/INT-002
            // Link API document: https://tealcommunications.atlassian.net/wiki/spaces/TD/overview
            logger?.LogInfo(CommonConstants.SUB, $"({tealEndpoint}, {pageNumber}, {pageSize})");

            var requestId = BuildTealRequestId(requestType, pageNumber);
            var offset = BuildOffset(pageSize, pageNumber);
            var queryParameters = BuildQueryParamForTeal(requestId, offset, pageSize);
            var dictFormUrlEncoded = new FormUrlEncodedContent(queryParameters);
            var queryString = await dictFormUrlEncoded.ReadAsStringAsync();
            return await RequestTealAPIAsync(tealEndpoint, queryString, logger);
        }

        public async Task<DeviceChangeResult<string, string>> GetTealOperationResultAsync(string tealOperationResultUrl, IKeysysLogger logger = null)
        {
            logger?.LogInfo(CommonConstants.SUB, $"({tealOperationResultUrl})");
            var responseBody = string.Empty;
            var decodedSecret = _base64Service.Base64Decode(_tealAuthentication.APISecret);
            var response = await RetryPolicyHelper.PollyRetryHttpRequestAsync(logger, TealHelper.CommonConfig.RETRY_NUMBER)
                .ExecuteAsync(async () =>
                {
                    logger?.LogInfo(CommonConstants.INFO, string.Format(LogCommonStrings.REQUEST_LOG_TEMPLATE, TealHelper.CommonString.GET, tealOperationResultUrl));
                    var requestMessage = _httpRequestFactory.BuildRequestMessage(null, new HttpMethod(TealHelper.CommonString.GET), new Uri(tealOperationResultUrl), BuildRequestHeader(decodedSecret));
                    return await _httpClientFactory.GetClient().SendAsync(requestMessage);
                });

            if (response != null)
            {
                responseBody = await response.Content.ReadAsStringAsync();

                if (response.IsSuccessStatusCode)
                {
                    logger?.LogInfo(CommonConstants.INFO, string.Format(LogCommonStrings.REQUEST_URL_SUCCESS, tealOperationResultUrl));
                    return BuildAPIResult(TealHelper.CommonString.GET, tealOperationResultUrl, responseBody, false);
                }
            }
            logger?.LogInfo(CommonConstants.ERROR, responseBody);
            return BuildAPIResult(TealHelper.CommonString.GET, tealOperationResultUrl, responseBody, true);
        }

        public async Task<DeviceChangeResult<string, string>> RequestTealDeviceUsageAsync(string tealGetDeviceUsageEndpoint, int pageNumber, string dataType, DateTime startDate, DateTime endDate, int pageSize = 100, IKeysysLogger logger = null)
        {
            logger?.LogInfo(CommonConstants.SUB, $"({tealGetDeviceUsageEndpoint}, {pageNumber}, {dataType}, {startDate}, {endDate}, {pageSize})");
            if (!_tealAuthentication.WriteIsEnabled)
            {
                return new DeviceChangeResult<string, string>()
                {
                    ActionText = TealHelper.CommonString.REQUEST_DEVICE_LIST_TO_BE_FETCHED_FROM_TEAL_PLATFORM,
                    HasErrors = true,
                    RequestObject = string.Empty,
                    ResponseObject = TealHelper.CommonString.WRITES_DISABLE_FOR_THIS_SERVICE_PROVIDER
                };
            }

            var decodedSecret = _base64Service.Base64Decode(_tealAuthentication.APISecret);
            var requestId = BuildTealRequestId(TealHelper.CommonString.TEAL_GET_DEVICE_USAGE, pageNumber);
            var offset = BuildOffset(pageSize, pageNumber);
            var queryParameters = BuildQueryParamForGetDeviceUsage(requestId, offset, pageSize, dataType, startDate, endDate);
            var dictFormUrlEncoded = new FormUrlEncodedContent(queryParameters);
            var queryString = await dictFormUrlEncoded.ReadAsStringAsync();
            var baseAddress = $"{_tealAuthentication.BaseUrl.TrimEnd('/')}/{tealGetDeviceUsageEndpoint}?{queryString}";

            var requestMessage = _httpRequestFactory.BuildRequestMessage(null, new HttpMethod(TealHelper.CommonString.GET), new Uri(baseAddress), BuildRequestHeader(decodedSecret));

            var response = await _httpClientFactory.GetClient().SendAsync(requestMessage);
            var responseBody = await response.Content.ReadAsStringAsync();

            if (response.IsSuccessStatusCode)
            {
                return BuildAPIResult(TealHelper.CommonString.GET, baseAddress, responseBody, false);
            }
            logger?.LogInfo(CommonConstants.ERROR, responseBody);
            return BuildAPIResult(TealHelper.CommonString.GET, baseAddress, responseBody, true);
        }

        public async Task<DeviceChangeResult<string, string>> ProcessTealUpdateAsync(string tealEndpoint, string requestType, string tealRequestJson, IKeysysLogger logger = null)
        {
            logger?.LogInfo(CommonConstants.SUB, $"({tealEndpoint})");
            var tealDeviceResult = await UpdateTealDeviceAsync(tealEndpoint, requestType, tealRequestJson, logger);
            var tealAPIResponse = JsonConvert.DeserializeObject<TealAPIResponse>(tealDeviceResult.ResponseObject);
            if (tealAPIResponse.Success)
            {
                var operationResultLink = tealAPIResponse.Link?.OperationResultLink?.Href;
                if (!string.IsNullOrWhiteSpace(operationResultLink))
                {
                    operationResultLink = operationResultLink.Replace(TealHelper.CommonString.HTTP, TealHelper.CommonString.HTTPS);
                    var tealReponseRootObject = await GetTealOperationResultAsync(operationResultLink);

                    if (!tealReponseRootObject.HasErrors)
                    {
                        return BuildAPIResult(requestType, tealEndpoint, tealReponseRootObject.ResponseObject, false, tealRequestJson);
                    }
                }
            }
            logger?.LogInfo(CommonConstants.EXCEPTION, LogCommonStrings.ERROR_WHILE_CALLING_TEAL_API);
            return BuildAPIResult(requestType, tealEndpoint, LogCommonStrings.ERROR_WHILE_CALLING_TEAL_API, false, tealRequestJson);
        }

        public async Task<DeviceChangeResult<string, string>> UpdateTealDeviceAsync(string tealEndpoint, string requestType, string request, IKeysysLogger logger = null)
        {
            logger?.LogInfo(CommonConstants.SUB, $"({tealEndpoint})");

            var requestId = BuildTealRequestId(requestType);
            var queryParameters = BuildQueryParamForTeal(requestId);
            var dictFormUrlEncoded = new FormUrlEncodedContent(queryParameters);
            var queryString = await dictFormUrlEncoded.ReadAsStringAsync();
            return await UpdateTealDeviceByMethodPostAsync(tealEndpoint, queryString, request, logger);
        }

        private Dictionary<string, string> BuildRequestHeader(string decodedSecret)
        {
            return new Dictionary<string, string> {
                { TealHelper.CommonString.APPLICATION_ACCEPTED, CommonConstants.APPLICATION_JSON },
                { TealHelper.CommonString.API_KEY, _tealAuthentication.APIKey},
                { TealHelper.CommonString.API_SECRET, decodedSecret}
            };
        }

        private string BuildTealRequestId(string requestType, int? pageNumber = null)
        {
            var requestId = $"{requestType}_{DateTime.UtcNow.ToString(TealHelper.CommonString.DATE_TIME_FORMAT_FOR_BUILD_REQUEST_ID)}";
            if (pageNumber != null)
            {
                return $"{requestId}_{pageNumber}";
            }
            return requestId;
        }

        private Dictionary<string, string> BuildQueryParamForGetDeviceUsage(string requestId, int offset, int pageSize, string dataType, DateTime startDate, DateTime endDate)
        {
            return new Dictionary<string, string>
                {
                    { TealHelper.CommonString.REQUEST_ID, requestId },
                    { TealHelper.CommonString.OFFSET, offset.ToString() },
                    { TealHelper.CommonString.LIMIT, pageSize.ToString() },
                    { TealHelper.CommonString.DATA_TYPE, dataType },
                    { TealHelper.CommonString.PERIOD_START, startDate.ToString(TealHelper.CommonString.TEAL_PERIOD_DATE_TIME_FORMAT) },
                    { TealHelper.CommonString.PERIOD_END, endDate.ToString(TealHelper.CommonString.TEAL_PERIOD_DATE_TIME_FORMAT) },
                };
        }

        private Dictionary<string, string> BuildQueryParamForTeal(string requestId, int? offset = null, int? pageSize = null)
        {
            var paramDictionary = new Dictionary<string, string>
            {
                { TealHelper.CommonString.REQUEST_ID, requestId },
            };

            if (offset != null)
            {
                paramDictionary.Add(TealHelper.CommonString.OFFSET, offset.ToString());
            }
            if (pageSize != null)
            {
                paramDictionary.Add(TealHelper.CommonString.LIMIT, pageSize.ToString());
            }
            return paramDictionary;
        }

        private DeviceChangeResult<string, string> BuildAPIResult(string httpRequestType, string baseAddress, string responseBody, bool hasError, string requestText = null)
        {
            return new DeviceChangeResult<string, string>()
            {
                ActionText = $"{httpRequestType} {baseAddress}",
                HasErrors = hasError,
                RequestObject = requestText,
                ResponseObject = responseBody
            };
        }

        private int BuildOffset(int pageSize, int pageNumber)
        {
            return pageSize * pageNumber;
        }

        public async Task<DeviceChangeResult<string, string>> RequestTealAPIAsync(string tealGetDeviceEndpoint, string queryString, IKeysysLogger logger = null)
        {
            if (!_tealAuthentication.WriteIsEnabled)
            {
                return BuildAPIResult(TealHelper.CommonString.GET, tealGetDeviceEndpoint, TealHelper.CommonString.WRITES_DISABLE_FOR_THIS_SERVICE_PROVIDER, true);
            }
            var decodedSecret = _base64Service.Base64Decode(_tealAuthentication.APISecret);
            var baseAddress = $"{_tealAuthentication.BaseUrl.TrimEnd('/')}/{tealGetDeviceEndpoint}?{queryString}";
            var response = await RetryPolicyHelper.PollyRetryHttpRequestAsync(logger, TealHelper.CommonConfig.RETRY_NUMBER)
                .ExecuteAsync(async () =>
                {
                    logger?.LogInfo(CommonConstants.INFO, string.Format(LogCommonStrings.REQUEST_LOG_TEMPLATE, TealHelper.CommonString.GET, baseAddress));
                    var requestMessage = _httpRequestFactory.BuildRequestMessage(null, new HttpMethod(TealHelper.CommonString.GET), new Uri(baseAddress), BuildRequestHeader(decodedSecret));
                    return await _httpClientFactory.GetClient().SendAsync(requestMessage);
                });

            var responseBody = await response.Content.ReadAsStringAsync();
            if (response.IsSuccessStatusCode)
            {
                logger?.LogInfo(CommonConstants.INFO, LogCommonStrings.REQUEST_URL_SUCCESS);
                return BuildAPIResult(TealHelper.CommonString.GET, baseAddress, responseBody, false);
            }
            logger?.LogInfo(CommonConstants.ERROR, responseBody);
            return BuildAPIResult(TealHelper.CommonString.GET, baseAddress, responseBody, true);
        }

        public async Task<DeviceChangeResult<string, string>> UpdateTealDeviceByMethodPostAsync(string endpoint, string queryString, string request, IKeysysLogger logger = null)
        {
            logger?.LogInfo(CommonConstants.SUB, $"({endpoint})");
            if (!_tealAuthentication.WriteIsEnabled)
            {
                return new DeviceChangeResult<string, string>()
                {
                    ActionText = TealHelper.CommonString.REQUEST_DEVICE_LIST_TO_BE_FETCHED_FROM_TEAL_PLATFORM,
                    HasErrors = true,
                    RequestObject = string.Empty,
                    ResponseObject = TealHelper.CommonString.WRITES_DISABLE_FOR_THIS_SERVICE_PROVIDER
                };
            }

            var decodedSecret = _base64Service.Base64Decode(_tealAuthentication.APISecret);
            var baseAddress = $"{_tealAuthentication.BaseUrl.TrimEnd('/')}/{endpoint}?{queryString}";
            var requestContent = new StringContent(request, Encoding.UTF8, CommonConstants.APPLICATION_JSON);
            var response = await RetryPolicyHelper.PollyRetryHttpRequestAsync(logger, TealHelper.CommonConfig.RETRY_NUMBER)
                .ExecuteAsync(async () =>
                {
                    logger?.LogInfo(CommonConstants.INFO, string.Format(LogCommonStrings.REQUEST_LOG_TEMPLATE, TealHelper.CommonString.GET, baseAddress));
                    var requestMessage = _httpRequestFactory.BuildRequestMessage(null, new HttpMethod(CommonConstants.METHOD_POST), new Uri(baseAddress), BuildRequestHeader(decodedSecret), requestContent);
                    return await _httpClientFactory.GetClient().SendAsync(requestMessage);
                });

            var responseBody = await response.Content.ReadAsStringAsync();
            if (response.IsSuccessStatusCode)
            {
                return BuildAPIResult(CommonConstants.METHOD_POST, baseAddress, responseBody, false);
            }
            logger?.LogInfo(CommonConstants.ERROR, responseBody);
            return BuildAPIResult(CommonConstants.METHOD_POST, baseAddress, responseBody, true);
        }
    }
}
