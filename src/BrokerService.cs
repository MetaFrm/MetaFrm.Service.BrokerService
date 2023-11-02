using MetaFrm.Database;
using MetaFrm.Models;
using System.Text.Json;

namespace MetaFrm.Service
{
    /// <summary>
    /// BrokerService
    /// </summary>
    public class BrokerService : IBrokerService
    {
        private readonly Dictionary<string, object> keyValues = new();
        private const string EmailNotification = nameof(EmailNotification);
        private const string PushNotification = nameof(PushNotification);
        private readonly int ReflashSeconds;

        /// <summary>
        /// BrokerService
        /// </summary>
        public BrokerService()
        {
            this.ReflashSeconds = -this.GetAttributeInt("ReflashSeconds");
        }

        Response IBrokerService.Request(BrokerData brokerData)
        {
            Response response;
            List<SandEmailModel> sandEmailList = new();
            List<PushModel> pushModelList = new();
            TokenDataTable? tokenDataTable;

            response = new();

            if (brokerData == null)
                return response;

            if (brokerData.ServiceData == null || brokerData.Response == null)
                return response;

            foreach (var commandKey in brokerData.ServiceData.Commands.Keys)
            {
                for (int i = 0; i < brokerData.ServiceData.Commands[commandKey].Values.Count; i++)
                {
                    switch (brokerData.ServiceData.Commands[commandKey].CommandText)
                    {
                        case nameof(EmailNotification):
                            var valuesEmail = brokerData.ServiceData.Commands[commandKey].Values[i];

                            if (valuesEmail.TryGetValue(nameof(SandEmailModel.EMAIL), out _)
                                && valuesEmail.TryGetValue(nameof(SandEmailModel.SUBJECT), out _)
                                && valuesEmail.TryGetValue(nameof(SandEmailModel.BODY), out _))
                                if (!valuesEmail[nameof(SandEmailModel.EMAIL)].StringValue.IsNullOrEmpty())
                                {
                                    sandEmailList.Add(new()
                                    {
                                        ACTION = brokerData.ServiceData.Commands[commandKey].CommandText,
                                        EMAIL = valuesEmail[nameof(SandEmailModel.EMAIL)].StringValue,
                                        SUBJECT = valuesEmail[nameof(SandEmailModel.SUBJECT)].StringValue,
                                        BODY = valuesEmail[nameof(SandEmailModel.BODY)].StringValue,
                                    });
                                }

                            break;

                        case nameof(PushNotification):
                            var valuesPush = brokerData.ServiceData.Commands[commandKey].Values[i];

                            if (valuesPush.TryGetValue(nameof(PushModel.Email), out _)
                                && valuesPush.TryGetValue(nameof(PushModel.Title), out _)
                                && valuesPush.TryGetValue(nameof(PushModel.Body), out _)
                                && valuesPush.TryGetValue(nameof(PushModel.ImageUrl), out _)
                                && valuesPush.TryGetValue(nameof(PushModel.Data), out _))
                                if (!valuesPush[nameof(PushModel.Email)].StringValue.IsNullOrEmpty())
                                {
                                    tokenDataTable = this.GetFirebaseFCM_Token(brokerData.ServiceData.Commands[commandKey].CommandText, valuesPush[nameof(PushModel.Email)].StringValue);

                                    if (tokenDataTable != null && tokenDataTable.DataTable != null)
                                        foreach (var item in tokenDataTable.DataTable.DataRows)
                                            pushModelList.Add(new()
                                            {
                                                Action = brokerData.ServiceData.Commands[commandKey].CommandText,
                                                Email = valuesPush[nameof(PushModel.Email)].StringValue,
                                                Token = item.String("TOKEN_STR"),
                                                Title = valuesPush[nameof(PushModel.Title)].StringValue,
                                                Body = valuesPush[nameof(PushModel.Body)].StringValue,
                                                ImageUrl = valuesPush[nameof(PushModel.ImageUrl)].StringValue,
                                                Data = valuesPush[nameof(PushModel.Data)].StringValue,
                                            });
                                }
                            break;
                        case string tmp when tmp.StartsWith("Batch."):
                            brokerData.ServiceData.ServiceName = Factory.ProjectService.ServiceNamespace;
                            brokerData.ServiceData.Commands[commandKey].CommandText = brokerData.ServiceData.Commands[commandKey].CommandText.Replace("Batch.", "");

                            brokerData.Response = ((IService)Factory.CreateInstance(brokerData.ServiceData.ServiceName)).Request(brokerData.ServiceData);

                            this.RequestDefault(brokerData, commandKey, i, sandEmailList, pushModelList);

                            return brokerData.Response;

                        default:
                            this.RequestDefault(brokerData, commandKey, i, sandEmailList, pushModelList);
                            break;
                    }
                }
            }

            if (sandEmailList.Count > 0) this.SandEmailAsync(sandEmailList);
            if (pushModelList.Count > 0) this.SandPushAsync(pushModelList, brokerData.DateTime);

            response.Status = Status.OK;

            return response;
        }
        private void RequestDefault(BrokerData brokerData, string commandKey, int index, List<SandEmailModel> sandEmailList, List<PushModel> pushModelList)
        {
            string? MESSAGE_TITLE = null;
            string? MESSAGE_BODY = null;
            string? IMAGE_URL = null;
            TokenDataTable? tokenDataTable;

            if (brokerData == null)
                return;

            if (brokerData.ServiceData == null || brokerData.Response == null)
                return;

            if (brokerData.Response.Status == Status.OK && brokerData.Response.DataSet != null && brokerData.Response.DataSet.DataTables.Count > 0)
                foreach (var table in brokerData.Response.DataSet.DataTables)
                    if (table.DataColumns.Any(x => x.FieldName == nameof(MESSAGE_TITLE))
                        && table.DataColumns.Any(x => x.FieldName == nameof(MESSAGE_BODY))
                        && table.DataColumns.Any(x => x.FieldName == nameof(IMAGE_URL))
                        && table.DataRows.Count > 0)
                    {
                        MESSAGE_TITLE = table.DataRows[0].String(nameof(MESSAGE_TITLE));
                        MESSAGE_BODY = table.DataRows[0].String(nameof(MESSAGE_BODY));
                        IMAGE_URL = table.DataRows[0].String(nameof(IMAGE_URL));
                        break;
                    }

            var values = brokerData.ServiceData.Commands[commandKey].Values[index];
            string? name = null;
            string? key = null;
            IEnumerable<PreferencesModel>? preferencesModel = null;

            if (values.TryGetValue("USER_ID", out _))
            {
                name = "USER_ID";
                key = $"Preferences.{values[name].IntValue}";
                this.LoadPreferences(values[name].IntValue, null);
            }
            else if (values.TryGetValue("EMAIL", out _))
            {
                name = "EMAIL";
                key = $"Preferences.{values[name].StringValue}";
                this.LoadPreferences(null, values[name].StringValue);
            }

            if (name != null && key != null)
            {
                Preferences? preferences1;

                preferences1 = null;

                lock (this.keyValues)
                    if (key != null && this.keyValues.TryGetValue(key, out object? obj) && obj is Preferences preferences2)
                        preferences1 = preferences2;

                if (preferences1 != null)
                    lock (preferences1)
                    {
                        if (name == "USER_ID")
                            preferencesModel = preferences1?.PreferencesList.Where(x => x.USER_ID == values["USER_ID"].IntValue && x.PREFERENCES_KEY == brokerData.ServiceData.Commands[commandKey].CommandText);
                        else
                            preferencesModel = preferences1?.PreferencesList.Where(x => x.EMAIL == values["EMAIL"].StringValue && x.PREFERENCES_KEY == brokerData.ServiceData.Commands[commandKey].CommandText);

                        if (preferencesModel != null)
                            preferencesModel = JsonSerializer.Deserialize<IEnumerable<PreferencesModel>?>(JsonSerializer.Serialize(preferencesModel));
                    }
            }

            if (preferencesModel != null)
                foreach (var item in preferencesModel)
                {
                    if (brokerData.Response.Status == Status.OK)
                    {
                        if (!MESSAGE_TITLE.IsNullOrEmpty() && !item.OK_TITLE.IsNullOrEmpty() && item.OK_TITLE.Contains("{0}"))
                            MESSAGE_TITLE = string.Format(item.OK_TITLE, MESSAGE_TITLE);
                        else
                            MESSAGE_TITLE = item.OK_TITLE;

                        if (!MESSAGE_BODY.IsNullOrEmpty() && !item.OK_BODY.IsNullOrEmpty() && item.OK_BODY.Contains("{0}"))
                            MESSAGE_BODY = string.Format(item.OK_BODY, MESSAGE_BODY);
                        else
                            MESSAGE_BODY = item.OK_BODY;
                    }
                    else
                    {
                        if (!MESSAGE_TITLE.IsNullOrEmpty() && !item.FAILED_TITLE.IsNullOrEmpty() && item.FAILED_TITLE.Contains("{0}"))
                            MESSAGE_TITLE = string.Format(item.FAILED_TITLE, MESSAGE_TITLE);
                        else
                            MESSAGE_TITLE = item.FAILED_TITLE;

                        if (brokerData.Response.Message != null)
                        {
                            if (!item.FAILED_BODY.IsNullOrEmpty() && item.FAILED_BODY.Contains("{0}"))
                                MESSAGE_BODY = string.Format(item.FAILED_BODY, brokerData.Response.Message);
                            else
                                MESSAGE_BODY = brokerData.Response.Message;
                        }
                        else
                        {
                            if (!MESSAGE_BODY.IsNullOrEmpty() && !item.FAILED_BODY.IsNullOrEmpty() && item.FAILED_BODY.Contains("{0}"))
                                MESSAGE_BODY = string.Format(item.FAILED_BODY, MESSAGE_BODY);
                            else
                                MESSAGE_BODY = item.FAILED_BODY;
                        }
                    }

                    if (item.PREFERENCES_TYPE == nameof(EmailNotification))
                    {
                        sandEmailList.Add(new()
                        {
                            ACTION = brokerData.ServiceData.Commands[commandKey].CommandText,
                            EMAIL = item.EMAIL,
                            SUBJECT = MESSAGE_TITLE,
                            BODY = MESSAGE_BODY,
                        });
                    }
                    else if (item.PREFERENCES_TYPE == nameof(PushNotification))
                    {
                        tokenDataTable = this.GetFirebaseFCM_Token(brokerData.ServiceData.Commands[commandKey].CommandText, item.EMAIL);

                        if (tokenDataTable != null && tokenDataTable.DataTable != null)
                            lock (tokenDataTable)
                                foreach (var itemToken in tokenDataTable.DataTable.DataRows)
                                    pushModelList.Add(new()
                                    {
                                        Action = brokerData.ServiceData.Commands[commandKey].CommandText,
                                        Email = item.EMAIL,
                                        Token = itemToken.String("TOKEN_STR"),
                                        Title = MESSAGE_TITLE,
                                        Body = MESSAGE_BODY,
                                        ImageUrl = !IMAGE_URL.IsNullOrEmpty() ? IMAGE_URL : brokerData.Response.Status.ToString(),
                                        Data = null,
                                    });
                    }
                }

        }

        private TokenDataTable? GetFirebaseFCM_Token(string ACTION, string? EMAIL)
        {
            IService service;
            Response response;
            string key;
            TokenDataTable? tokenDataTable;

            key = $"Token.{EMAIL}";
            tokenDataTable = null;

            lock (this.keyValues)
            {
                if (!this.keyValues.TryGetValue(key, out _))
                    this.keyValues.Add(key, new TokenDataTable(DateTime.Now.AddSeconds(this.ReflashSeconds)));//1분   2분5초-1분=>1분5초

                if (this.keyValues[key] is TokenDataTable tokenDataTable1)
                    tokenDataTable = tokenDataTable1;
            }

            if (tokenDataTable != null && (tokenDataTable.DateTime <= DateTime.Now.AddSeconds(this.ReflashSeconds) || tokenDataTable.DataTable == null))
            {
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

                        lock (tokenDataTable)
                        {
                            tokenDataTable.DataTable = response.DataSet.DataTables[0];
                            tokenDataTable.DateTime = DateTime.Now;
                        }
                        return tokenDataTable;
                    }
                }
                else
                {
                    if (response.Message != null)
                        Console.WriteLine(response.Message);
                }

                Console.WriteLine("Get FirebaseFCM Token  Fail !!");
            }
            else if (this.keyValues[key] is TokenDataTable tokenDataTable2)
                return tokenDataTable2;

            return null;
        }

        private void SandPushAsync(List<PushModel> pushModelList, DateTime dateTime)
        {
            IService service;
            Response response;
            string key;
            Preferences? preferences;

            ServiceData serviceData = new()
            {
                ServiceName = this.GetAttribute("PushNotificationServiceName"),//"MetaFrm.Service.FirebaseAdminService"
                TransactionScope = false,
            };

            serviceData["1"].CommandText = this.GetAttribute("PushNotificationService");//"FirebaseAdminService"
            serviceData["1"].AddParameter(nameof(PushModel.Token), DbType.NVarChar, 4000);
            serviceData["1"].AddParameter(nameof(PushModel.Title), DbType.NVarChar, 4000);
            serviceData["1"].AddParameter(nameof(PushModel.Body), DbType.NVarChar, 4000);
            serviceData["1"].AddParameter(nameof(PushModel.ImageUrl), DbType.NVarChar, 4000);
            serviceData["1"].AddParameter(nameof(PushModel.Data), DbType.NVarChar, 4000);

            key = $"Preferences.{pushModelList[0].Email}";
            preferences = null;

            lock (this.keyValues)
                if (pushModelList[0].Action != nameof(PushNotification) && this.keyValues.TryGetValue(key, out object? obj) && obj is Preferences preferences1)
                    preferences = preferences1;

            foreach (var item in pushModelList)
            {
                if (item.Action != nameof(PushNotification) && preferences != null)
                    lock (preferences)
                    {
                        var itemPreferences = preferences.PreferencesList.SingleOrDefault(x => x.EMAIL == item.Email && x.PREFERENCES_TYPE == nameof(PushNotification) && x.PREFERENCES_KEY == item.Action);

                        if (itemPreferences == null || itemPreferences.PREFERENCES_VALUE == "N")
                            continue;
                    }

                serviceData["1"].NewRow();
                serviceData["1"].SetValue(nameof(PushModel.Token), item.Token);
                serviceData["1"].SetValue(nameof(PushModel.Title), $"{item.Title} {dateTime:dd HH:mm:ss}");
                serviceData["1"].SetValue(nameof(PushModel.Body), item.Body);
                serviceData["1"].SetValue(nameof(PushModel.ImageUrl), item.ImageUrl);
                serviceData["1"].SetValue(nameof(PushModel.Data), item.Data);
            }

            service = (IService)Factory.CreateInstance(serviceData.ServiceName);
            //service = (IService)new MetaFrm.Service.FirebaseAdminService();
            response = service.Request(serviceData);

            if (response.Status != Status.OK && response.Message != null)
                Console.WriteLine(response.Message);
        }
        private void SandEmailAsync(List<SandEmailModel> sandEmailList)
        {
            IService service;
            Response response;
            string key;
            Preferences? preferences;

            ServiceData serviceData = new()
            {
                TransactionScope = false,
            };
            serviceData["1"].CommandText = this.GetAttribute("SandEmail");
            serviceData["1"].CommandType = System.Data.CommandType.StoredProcedure;
            serviceData["1"].AddParameter(nameof(SandEmailModel.ACTION), DbType.NVarChar, 50);
            serviceData["1"].AddParameter(nameof(SandEmailModel.SUBJECT), DbType.NVarChar, 4000);
            serviceData["1"].AddParameter(nameof(SandEmailModel.BODY), DbType.NVarChar, 4000);
            serviceData["1"].AddParameter(nameof(SandEmailModel.EMAIL), DbType.NVarChar, 100);

            key = $"Preferences.{sandEmailList[0].EMAIL}";
            preferences = null;

            lock (this.keyValues)
                if (sandEmailList[0].ACTION != nameof(EmailNotification) && this.keyValues.TryGetValue(key, out object? obj) && obj is Preferences preferences1)
                    preferences = preferences1;

            foreach (var item in sandEmailList)
            {
                if (item.ACTION != nameof(EmailNotification) && preferences != null)
                    lock (preferences)
                    {
                        var itemPreferences = preferences.PreferencesList.SingleOrDefault(x => x.EMAIL == item.EMAIL && x.PREFERENCES_TYPE == nameof(EmailNotification) && x.PREFERENCES_KEY == item.ACTION);

                        if (itemPreferences == null || itemPreferences.PREFERENCES_VALUE == "N")
                            continue;
                    }

                serviceData["1"].NewRow();
                serviceData["1"].SetValue(nameof(SandEmailModel.ACTION), item.ACTION);
                serviceData["1"].SetValue(nameof(SandEmailModel.SUBJECT), item.SUBJECT);
                serviceData["1"].SetValue(nameof(SandEmailModel.BODY), item.BODY);
                serviceData["1"].SetValue(nameof(SandEmailModel.EMAIL), item.EMAIL);
            }

            service = (IService)Factory.CreateInstance(serviceData.ServiceName);
            response = service.Request(serviceData);

            if (response.Status != Status.OK && response.Message != null)
                Console.WriteLine(response.Message);
        }

        private void LoadPreferences(int? USER_ID, string? EMAIL)
        {
            string key;
            object? obj;
            List<PreferencesModel> preferencesModelList;

            if (USER_ID != null)
                key = $"Preferences.{USER_ID}";
            else
                key = $"Preferences.{EMAIL}";

            obj = null;

            lock (this.keyValues)
                if (!this.keyValues.TryGetValue(key, out obj))
                {
                    obj = new Preferences(DateTime.Now.AddSeconds(this.ReflashSeconds));//1분   2분5초-1분=>1분5초
                    this.keyValues.Add(key, obj);
                }

            if (obj != null && obj is Preferences preferences && (preferences.DateTime <= DateTime.Now.AddSeconds(this.ReflashSeconds) || preferences.PreferencesList.Count < 1))
            {
                preferencesModelList = this.LoadPreferencesDB(USER_ID, EMAIL);

                lock (preferences)
                {
                    preferences.PreferencesList.Clear();
                    preferences.PreferencesList = preferencesModelList;
                    preferences.DateTime = DateTime.Now;
                }

                if (USER_ID != null)
                {
                    if (preferencesModelList.Count > 0)
                    {
                        key = $"Preferences.{preferencesModelList[0].EMAIL}";

                        lock (this.keyValues)
                            if (!this.keyValues.TryGetValue(key, out obj))
                                this.keyValues.Add(key, preferences);
                    }
                }
                else
                {
                    if (preferencesModelList.Count > 0 && preferencesModelList[0].USER_ID > 0)
                    {
                        key = $"Preferences.{preferencesModelList[0].USER_ID}";

                        lock (this.keyValues)
                            if (!this.keyValues.TryGetValue(key, out obj))
                                this.keyValues.Add(key, preferences);
                    }
                }
            }
        }
        private List<PreferencesModel> LoadPreferencesDB(int? USER_ID, string? EMAIL)
        {
            IService service;
            Response response;
            List<PreferencesModel> preferences = new();

            ServiceData serviceData = new()
            {
                TransactionScope = false,
            };
            serviceData["1"].CommandText = this.GetAttribute("SearchPreferences");
            serviceData["1"].CommandType = System.Data.CommandType.StoredProcedure;
            serviceData["1"].AddParameter(nameof(USER_ID), DbType.Int, 3, USER_ID);
            serviceData["1"].AddParameter(nameof(EMAIL), DbType.NVarChar, 50, EMAIL);

            service = (IService)Factory.CreateInstance(serviceData.ServiceName);
            response = service.Request(serviceData);

            if (response.Status == Status.OK)
            {
                if (response.DataSet != null && response.DataSet.DataTables.Count > 0 && response.DataSet.DataTables[0].DataRows.Count > 0)
                    foreach (var item in response.DataSet.DataTables[0].DataRows)
                        preferences.Add(new()
                        {
                            USER_ID = item.Int(nameof(PreferencesModel.USER_ID)),
                            EMAIL = item.String(nameof(PreferencesModel.EMAIL)),
                            PLATFORM = item.String(nameof(PreferencesModel.PLATFORM)),
                            DEVICE_MODEL = item.String(nameof(PreferencesModel.DEVICE_MODEL)),
                            DEVICE_NAME = item.String(nameof(PreferencesModel.DEVICE_NAME)),
                            PREFERENCES_TYPE = item.String(nameof(PreferencesModel.PREFERENCES_TYPE)),
                            PREFERENCES_KEY = item.String(nameof(PreferencesModel.PREFERENCES_KEY)),
                            PREFERENCES_VALUE = item.String(nameof(PreferencesModel.PREFERENCES_VALUE)),
                            OK_TITLE = item.String(nameof(PreferencesModel.OK_TITLE)),
                            OK_BODY = item.String(nameof(PreferencesModel.OK_BODY)),
                            FAILED_TITLE = item.String(nameof(PreferencesModel.FAILED_TITLE)),
                            FAILED_BODY = item.String(nameof(PreferencesModel.FAILED_BODY)),
                        });
            }
            else
            {
                if (response.Message != null)
                    Console.WriteLine(response.Message);
            }

            return preferences;
        }
    }
}