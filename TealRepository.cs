using System;
using System.Data;
using Altaworx.AWS.Core.Helpers.Constants;
using Altaworx.AWS.Core.Models;
using Amop.Core.Constants;
using Amop.Core.Models.Teal;
using Microsoft.Data.SqlClient;

namespace Altaworx.AWS.Core.Repositories.Teal
{
    public class TealRepository
    {
        private readonly KeySysLambdaContext _context;
        public TealRepository(KeySysLambdaContext context)
        {
            _context = context;
        }

        public TealAuthentication GetTealAuthenticationInformation(int serviceProviderId)
        {
            TealAuthentication tealAuthentication = null;
            try
            {
                using (var sqlConnection = new SqlConnection(_context.CentralDbConnectionString))
                {
                    sqlConnection.Open();
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
                }
            }
            catch (SqlException ex)
            {
                _context.LogInfo(LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_EXECUTING_SQL_COMMAND, ex.Message));
            }
            catch (InvalidOperationException ex)
            {
                _context.LogInfo(LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_CONNECTING_DATABASE, ex.Message));
            }
            catch (Exception ex)
            {
                _context.LogInfo(LogTypeConstant.Exception, ex.Message);
            }

            return tealAuthentication;
        }
    }
}
