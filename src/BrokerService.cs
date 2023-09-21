using MetaFrm.Database;
using System.Text.Json;

namespace MetaFrm.Service
{
    /// <summary>
    /// BrokerService
    /// </summary>
    public class BrokerService : IBrokerService
    {
        private readonly string Login;
        private readonly string AccessCode;
        private readonly string Join;
        private readonly string PasswordReset;

        /// <summary>
        /// BrokerService
        /// </summary>
        public BrokerService()
        {
            this.Login = this.GetAttribute(nameof(this.Login));
            this.AccessCode = this.GetAttribute(nameof(this.AccessCode));
            this.Join = this.GetAttribute(nameof(this.Join));
            this.PasswordReset = this.GetAttribute(nameof(this.PasswordReset));
        }

        Response IBrokerService.Request(BrokerData brokerData)
        {
            Response response;

            response = new();

            if (brokerData == null)
                return response;

            if (brokerData.ServiceData == null || brokerData.Response == null)
                return response;

            foreach (var key in brokerData.ServiceData.Commands.Keys)
            {
                for (int i = 0; i < brokerData.ServiceData.Commands[key].Values.Count; i++)
                {
                    switch (brokerData.ServiceData.Commands[key].CommandText)
                    {
                        case nameof(this.Login):
                            string? email = brokerData.ServiceData.Commands[key].Values[i]["EMAIL"].StringValue;

                            this.PushNotification(nameof(this.Login)
                                , email
                                , $"Login {(brokerData.Response.Status == Status.OK ? "OK" : "Fail")}"
                                , $"{(brokerData.Response.Status == Status.OK ? email : brokerData.Response.Message)}"
                                , brokerData.DateTime
                                , brokerData.Response.Status
                                , null);
                            break;

                        case string tmp when tmp == this.AccessCode || tmp == this.Join || tmp == this.PasswordReset:
                            if (brokerData.Response.Status == Status.OK && brokerData.Response.DataSet != null && brokerData.Response.DataSet.DataTables.Count > 0 && brokerData.Response.DataSet.DataTables[0].DataRows.Count > 0)//AccessCode
                            {
                                this.SandEmail(tmp
                                    , brokerData.Response.DataSet.DataTables[0].DataRows[0].String("SUBJECT")
                                    , brokerData.Response.DataSet.DataTables[0].DataRows[0].String("BODY")
                                    , brokerData.Response.DataSet.DataTables[0].DataRows[0].String("EMAIL"));
                            }
                            break;
                    }
                }
            }

            response.Status = Status.OK;

            return response;
        }


        private void PushNotification(string ACTION, string? EMAIL, string Title, string? Body, DateTime dateTime, Status status, Dictionary<string, string>? data)
        {
            IService service;
            Data.DataTable? dataTable;

            dataTable = this.GetFirebaseFCM_Token(ACTION, EMAIL);

            if (dataTable == null)
                return;

            ServiceData serviceData = new()
            {
                ServiceName = this.GetAttribute("PushNotificationServiceName"),//"MetaFrm.Service.FirebaseAdminService"
                TransactionScope = false,
            };

            serviceData["1"].CommandText = this.GetAttribute("PushNotificationService");//"FirebaseAdminService"
            serviceData["1"].AddParameter("Token", DbType.NVarChar, 4000);
            serviceData["1"].AddParameter(nameof(Title), DbType.NVarChar, 4000);
            serviceData["1"].AddParameter(nameof(Body), DbType.NVarChar, 4000);
            serviceData["1"].AddParameter("ImageUrl", DbType.NVarChar, 4000);
            serviceData["1"].AddParameter("Data", DbType.NVarChar, 4000);

            foreach (var item in dataTable.DataRows)
            {
                serviceData["1"].NewRow();
                serviceData["1"].SetValue("Token", item.String("TOKEN_STR"));
                serviceData["1"].SetValue(nameof(Title), $"{Title} {dateTime:dd HH:mm:ss}");
                serviceData["1"].SetValue(nameof(Body), $"{Body}");
                serviceData["1"].SetValue("ImageUrl", status == Status.OK ? "Complete" : "Fail");
                serviceData["1"].SetValue("Data", data != null ? JsonSerializer.Serialize(data) : null);
            }

            service = (IService)Factory.CreateInstance(serviceData.ServiceName);
            //service = (IService)new MetaFrm.Service.FirebaseAdminService();
            _ = service.Request(serviceData);
        }
        private Data.DataTable? GetFirebaseFCM_Token(string ACTION, string? EMAIL)
        {
            IService service;
            Response response;

            ServiceData serviceData = new()
            {
                TransactionScope = false,
            };
            serviceData["1"].CommandText = this.GetAttribute("SearchToken");
            serviceData["1"].CommandType = System.Data.CommandType.StoredProcedure;
            serviceData["1"].AddParameter("TOKEN_TYPE", DbType.NVarChar, 50, "Firebase.FCM");
            serviceData["1"].AddParameter(nameof(ACTION), DbType.NVarChar, 50, ACTION);
            serviceData["1"].AddParameter(nameof(EMAIL), DbType.NVarChar, 100, EMAIL);

            service = (IService)Factory.CreateInstance(serviceData.ServiceName);
            response = service.Request(serviceData);

            if (response.Status == Status.OK)
            {
                if (response.DataSet != null && response.DataSet.DataTables.Count > 0)
                {
                    Console.WriteLine("Get FirebaseFCM Token Completed !!");
                    return response.DataSet.DataTables[0];
                }
            }
            else
            {
                if (response.Message != null)
                    throw new Exception(response.Message);
            }

            throw new Exception("Get FirebaseFCM Token  Fail !!");
        }
        private void SandEmail(string ACTION, string? SUBJECT, string? BODY, string? EMAIL)
        {
            IService service;
            Response response;

            ServiceData serviceData = new()
            {
                TransactionScope = false,
            };
            serviceData["1"].CommandText = this.GetAttribute("SandEmail");
            serviceData["1"].CommandType = System.Data.CommandType.StoredProcedure;
            serviceData["1"].AddParameter(nameof(ACTION), DbType.NVarChar, 50, ACTION);
            serviceData["1"].AddParameter(nameof(SUBJECT), DbType.NVarChar, 4000, SUBJECT);
            serviceData["1"].AddParameter(nameof(BODY), DbType.NVarChar, 4000, BODY);
            serviceData["1"].AddParameter(nameof(EMAIL), DbType.NVarChar, 100, EMAIL);

            service = (IService)Factory.CreateInstance(serviceData.ServiceName);
            response = service.Request(serviceData);

            if (response.Status == Status.OK)
            {
                if (response.DataSet != null && response.DataSet.DataTables.Count > 0)
                    Console.WriteLine("SandEmail Completed !!");
            }
            else
            {
                if (response.Message != null)
                    throw new Exception(response.Message);
            }

            throw new Exception("SandEmail Fail !!");
        }
    }
}