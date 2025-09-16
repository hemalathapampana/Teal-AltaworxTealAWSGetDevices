using System;
using System.Data;
using Microsoft.Data.SqlClient;
using Amop.Core.Helpers;
using Amop.Core.Constants;

namespace Altaworx.AWS.Core.Helpers
{
    public static class BillingPeriodHelper
    {
        public static BillingPeriod GetBillingPeriodForServiceProvider(string connectionString, int serviceProviderId, int billingPeriodYear, int billingPeriodMonth, TimeZoneInfo billingTimeZone, int billingPeriodEndDay = 18, int billingPeriodEndHour = 18)
        {
            int id = 0;

            using (var Conn = new SqlConnection(connectionString))
            {
                using (var cmd = Conn.CreateCommand())
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = "dbo.usp_Service_Provider_Get_Bill_Period_Day_And_Hour";
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    cmd.Parameters.AddWithValue("@BillMonth", billingPeriodMonth);
                    cmd.Parameters.AddWithValue("@BillYear", billingPeriodYear);
                    Conn.Open();

                    SqlDataReader rdr = cmd.ExecuteReader();

                    while (rdr.Read())
                    {
                        int billPeriodEndDayIndex = rdr.GetOrdinal("BillPeriodEndDay");
                        if (!rdr.IsDBNull(billPeriodEndDayIndex))
                        {
                            billingPeriodEndDay = (int)rdr["BillPeriodEndDay"];
                        }

                        int billPeriodEndHourIndex = rdr.GetOrdinal("BillPeriodEndHour");
                        if (!rdr.IsDBNull(billPeriodEndHourIndex))
                        {
                            billingPeriodEndHour = (int)rdr["BillPeriodEndHour"];
                        }

                        int billPeriodIdIndex = rdr.GetOrdinal("Id");
                        if (!rdr.IsDBNull(billPeriodIdIndex))
                        {
                            id = (int)rdr["Id"];
                        }
                    }

                    Conn.Close();
                }
            }

            return new BillingPeriod(id, serviceProviderId, billingPeriodYear, billingPeriodMonth, billingPeriodEndDay, billingPeriodEndHour, billingTimeZone);
        }

        public static BillingPeriod GetBillingPeriodCurrentMonth(string connectionString, int serviceProviderId, int billingPeriodYear, int billingPeriodMonth)
        {
            int id = 0;
            var hasData = false;
            int billingPeriodEndHour = 0;
            int billingPeriodEndDay = 0;
            var billingCycleStartDate = new DateTime();
            var billingCycleEndDate = new DateTime();

            using (var Conn = new SqlConnection(connectionString))
            {
                using (var cmd = Conn.CreateCommand())
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = "dbo.usp_Service_Provider_Get_Bill_Period_Day_And_Hour";
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    cmd.Parameters.AddWithValue("@BillMonth", billingPeriodMonth);
                    cmd.Parameters.AddWithValue("@BillYear", billingPeriodYear);
                    Conn.Open();

                    SqlDataReader rdr = cmd.ExecuteReader();

                    while (rdr.Read())
                    {
                        int billPeriodEndDayIndex = rdr.GetOrdinal("BillPeriodEndDay");
                        if (!rdr.IsDBNull(billPeriodEndDayIndex))
                        {
                            billingPeriodEndDay = (int)rdr["BillPeriodEndDay"];
                        }

                        int billPeriodEndHourIndex = rdr.GetOrdinal("BillPeriodEndHour");
                        if (!rdr.IsDBNull(billPeriodEndHourIndex))
                        {
                            billingPeriodEndHour = (int)rdr["BillPeriodEndHour"];
                        }

                        int billPeriodIdIndex = rdr.GetOrdinal("Id");
                        if (!rdr.IsDBNull(billPeriodIdIndex))
                        {
                            id = (int)rdr["Id"];
                        }

                        if (!rdr.IsDBNull(rdr.GetOrdinal("BillingCycleStartDate")))
                        {
                            billingCycleStartDate = DateTime.Parse(rdr["BillingCycleStartDate"].ToString());
                        }

                        if (!rdr.IsDBNull(rdr.GetOrdinal("BillingCycleEndDate")))
                        {
                            billingCycleEndDate = DateTime.Parse(rdr["BillingCycleEndDate"].ToString());
                        }

                        hasData = true;
                    }

                    Conn.Close();
                }
            }
            if (hasData)
            {
                return new BillingPeriod(id, serviceProviderId, billingPeriodYear, billingPeriodMonth, billingPeriodEndDay, billingPeriodEndHour, billingCycleStartDate, billingCycleEndDate);
            }
            else return null;
        }

        public static BillingPeriod GetBillingPeriodForServiceProviderByCurrentDate(Action<string, string> logFunction, string connectionString, int serviceProviderId, DateTime currentDateTime, TimeZoneInfo billingTimeZone, int defaultBillingPeriodYear, int defaultBillingPeriodMonth, int billingPeriodEndDay, int billingPeriodEndHour)
        {
            int id = 0;
            DateTime billPeriodEndDate;
            try
            {
                // Default billing period end date
                // This is also to check all the parameter values if they are valid values (month is 1 -> 12, hour is 0 -> 23,...)
                billPeriodEndDate = new DateTime(defaultBillingPeriodYear, defaultBillingPeriodMonth, billingPeriodEndDay, billingPeriodEndHour, minute: 0, second: 0);
            }
            catch (Exception)
            {
                logFunction(CommonConstants.ERROR, string.Format(LogCommonStrings.INVALID_DATE_TIME_FORMAT, defaultBillingPeriodYear, defaultBillingPeriodMonth, billingPeriodEndDay, billingPeriodEndHour));
                return null;
            }

            try
            {
                using (var connection = new SqlConnection(connectionString))
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.CommandTimeout = SQLConstant.ShortTimeoutSeconds;
                        command.CommandText = SQLConstant.StoredProcedureName.SERVICE_PROVIDER_GET_EXISTING_BILL_PERIOD_BY_CURRENT_DATE_TIME;
                        command.Parameters.AddWithValue(CommonSQLParameterNames.SERVICE_PROVIDER_ID, serviceProviderId);
                        command.Parameters.AddWithValue(CommonSQLParameterNames.CURRENT_DATE_TIME, currentDateTime);
                        command.Parameters.AddWithValue(CommonSQLParameterNames.BILL_PERIOD_END_DAY, billingPeriodEndDay);
                        command.Parameters.AddWithValue(CommonSQLParameterNames.BILL_PERIOD_END_HOUR, billingPeriodEndHour);
                        connection.Open();

                        SqlDataReader dataReader = command.ExecuteReader();

                        while (dataReader.Read())
                        {
                            var columns = dataReader.GetColumnsFromReader();
                            id = dataReader.IntFromReader(columns, CommonColumnNames.Id);
                            logFunction(CommonConstants.INFO, string.Format(LogCommonStrings.BILLING_PERIOD_ID_VALUE, id));
                            if (id == 0)
                            {
                                logFunction(CommonConstants.ERROR, string.Format(LogCommonStrings.INVALID_VALUE_ERROR_TEMPLATE, id, nameof(id)));
                                break;
                            }
                            billPeriodEndDate = dataReader.DateTimeFromReader(columns, CommonColumnNames.BillingCycleEndDate);
                        }
                    }
                }
            }
            catch (SqlException ex)
            {
                logFunction(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.EXCEPTION_WHEN_EXECUTING_SQL_COMMAND, ex.Message));
            }
            catch (InvalidOperationException ex)
            {
                logFunction(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.EXCEPTION_WHEN_CONNECTING_DATABASE, ex.Message));
            }
            catch (Exception ex)
            {
                logFunction(CommonConstants.EXCEPTION, ex.Message);
            }

            if (id != 0)
            {
                return new BillingPeriod(id, serviceProviderId, billPeriodEndDate.Year, billPeriodEndDate.Month, billPeriodEndDate.Day, billPeriodEndDate.Hour, billingTimeZone);
            }
            else
            {
                logFunction(CommonConstants.INFO, string.Format(LogCommonStrings.NO_EXISTING_BILLING_PERIOD_FOUND, defaultBillingPeriodYear, defaultBillingPeriodMonth, billingPeriodEndDay, billingPeriodEndHour));
                return null;
            }
        }
    }
}
