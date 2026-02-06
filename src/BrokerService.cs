using MetaFrm.Database;
using MetaFrm.Models;
using System.Collections.Concurrent;
using System.Text.Json;

namespace MetaFrm.Service
{
    /// <summary>
    /// BrokerService
    /// </summary>
    public class BrokerService : IServiceString
    {
        private readonly ConcurrentDictionary<string, object> keyValues = [];
        private readonly string EmailNotification = nameof(EmailNotification);
        private readonly string PushNotification = nameof(PushNotification);
        private readonly string USER_ID = nameof(USER_ID);
        private readonly string EMAIL = nameof(EMAIL);
        private readonly int ReflashSeconds;

        /// <summary>
        /// BrokerService
        /// </summary>
        public BrokerService()
        {
            this.ReflashSeconds = -this.GetAttributeInt("ReflashSeconds");
        }

        string IServiceString.Request(string data)
        {
            List<SandEmailModel> sandEmailList = [];
            List<PushModel> pushModelList = [];
            TokenDataTable? tokenDataTable;

            BrokerData? brokerData = JsonSerializer.Deserialize<BrokerData?>(data);

            if (brokerData == null)
                return "";

            if (brokerData.ServiceData == null)
                return "";

            foreach (var commandKey in brokerData.ServiceData.Commands.Keys)
            {
                Command command = brokerData.ServiceData.Commands[commandKey];
                for (int i = 0; i < command.Values.Count; i++)
                {
                    switch (command.CommandText)
                    {
                        case nameof(this.EmailNotification):
                            var valuesEmail = command.Values[i];

                            if (valuesEmail.TryGetValue(nameof(SandEmailModel.EMAIL), out Data.DataValue? email1)
                                && valuesEmail.TryGetValue(nameof(SandEmailModel.SUBJECT), out Data.DataValue? subject1)
                                && valuesEmail.TryGetValue(nameof(SandEmailModel.BODY), out Data.DataValue? body1))
                                if (!string.IsNullOrEmpty(email1.StringValue))
                                {
                                    sandEmailList.Add(new()
                                    {
                                        ACTION = command.CommandText,
                                        EMAIL = email1.StringValue,
                                        SUBJECT = subject1.StringValue,
                                        BODY = body1.StringValue,
                                    });
                                }

                            break;

                        case nameof(this.PushNotification):
                            var valuesPush = command.Values[i];

                            if (valuesPush.TryGetValue(nameof(PushModel.Email), out Data.DataValue? email2)
                                && valuesPush.TryGetValue(nameof(PushModel.Title), out Data.DataValue? title2)
                                && valuesPush.TryGetValue(nameof(PushModel.Body), out Data.DataValue? body2)
                                && valuesPush.TryGetValue(nameof(PushModel.ImageUrl), out Data.DataValue? imageUrl2)
                                && valuesPush.TryGetValue(nameof(PushModel.Data), out Data.DataValue? data2))
                                if (!string.IsNullOrEmpty(email2.StringValue))
                                {
                                    tokenDataTable = this.GetFirebaseFCM_Token(command.CommandText, email2.StringValue);

                                    if (tokenDataTable != null && tokenDataTable.DataTable != null)
                                        foreach (var item in tokenDataTable.DataTable.DataRows)
                                            pushModelList.Add(new()
                                            {
                                                Action = command.CommandText,
                                                Email = email2.StringValue,
                                                Token = item.String("TOKEN_STR"),
                                                Title = title2.StringValue,
                                                Body = body2.StringValue,
                                                ImageUrl = imageUrl2.StringValue,
                                                Data = data2.StringValue,
                                            });
                                }
                            break;
                        case string tmp when tmp.StartsWith("Batch."):
                            brokerData.ServiceData.ServiceName = Factory.ProjectService.ServiceNamespace;
                            command.CommandText = command.CommandText.Replace("Batch.", "");

                            brokerData.Response = ((IService)Factory.CreateInstance(brokerData.ServiceData.ServiceName)).Request(brokerData.ServiceData);

                            this.RequestDefault(brokerData, commandKey, i, ref sandEmailList, ref pushModelList);
                            break;

                        default:
                            this.RequestDefault(brokerData, commandKey, i, ref sandEmailList, ref pushModelList);
                            break;
                    }
                }
            }

            if (sandEmailList.Count > 0) this.SandEmail(sandEmailList);
            if (pushModelList.Count > 0) this.SandPush(pushModelList, brokerData.DateTime);

            return "";
        }
        private void RequestDefault(BrokerData brokerData, string commandKey, int index, ref List<SandEmailModel> sandEmailList, ref List<PushModel> pushModelList)
        {
            string? MESSAGE_TITLE = null;
            string? MESSAGE_BODY = null;
            string? IMAGE_URL = null;
            TokenDataTable? tokenDataTable;
            Command command;

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

            command = brokerData.ServiceData.Commands[commandKey];

            var values = command.Values[index];
            string? name = null;
            string? key = null;

            if (values.TryGetValue(this.USER_ID, out Data.DataValue? _userID))
            {
                name = this.USER_ID;
                key = $"Preferences.{_userID.IntValue}";
                this.LoadPreferences(_userID.IntValue, null);
            }
            else if (values.TryGetValue(this.EMAIL, out Data.DataValue? _email))
            {
                name = this.EMAIL;
                key = $"Preferences.{_email.StringValue}";
                this.LoadPreferences(null, _email.StringValue);
            }

            if (name != null && key != null)
            {
                if (this.keyValues.TryGetValue(key, out object? obj) && obj is Preferences preferences2)

                {
                    IEnumerable<PreferencesModel>? preferencesModel = null;

                    if (name == this.USER_ID)
                        preferencesModel = preferences2?.PreferencesList.Where(x => x.USER_ID == values[this.USER_ID].IntValue && x.PREFERENCES_KEY == command.CommandText);
                    else
                        preferencesModel = preferences2?.PreferencesList.Where(x => x.EMAIL == values[this.EMAIL].StringValue && x.PREFERENCES_KEY == command.CommandText);

                    if (preferencesModel != null)
                        foreach (var item in preferencesModel)
                        {
                            if (brokerData.Response.Status == Status.OK)
                            {
                                if (!string.IsNullOrEmpty(MESSAGE_TITLE) && !string.IsNullOrEmpty(item.OK_TITLE) && item.OK_TITLE.Contains("{0}"))
                                    MESSAGE_TITLE = string.Format(item.OK_TITLE, MESSAGE_TITLE);
                                else
                                    MESSAGE_TITLE = item.OK_TITLE;

                                if (!string.IsNullOrEmpty(MESSAGE_BODY) && !string.IsNullOrEmpty(item.OK_BODY) && item.OK_BODY.Contains("{0}"))
                                    MESSAGE_BODY = string.Format(item.OK_BODY, MESSAGE_BODY);
                                else
                                    MESSAGE_BODY = item.OK_BODY;
                            }
                            else
                            {
                                if (!string.IsNullOrEmpty(MESSAGE_TITLE) && !string.IsNullOrEmpty(item.FAILED_TITLE) && item.FAILED_TITLE.Contains("{0}"))
                                    MESSAGE_TITLE = string.Format(item.FAILED_TITLE, MESSAGE_TITLE);
                                else
                                    MESSAGE_TITLE = item.FAILED_TITLE;

                                if (brokerData.Response.Message != null)
                                {
                                    if (!string.IsNullOrEmpty(item.FAILED_BODY) && item.FAILED_BODY.Contains("{0}"))
                                        MESSAGE_BODY = string.Format(item.FAILED_BODY, brokerData.Response.Message);
                                    else
                                        MESSAGE_BODY = brokerData.Response.Message;
                                }
                                else
                                {
                                    if (!string.IsNullOrEmpty(MESSAGE_BODY) && !string.IsNullOrEmpty(item.FAILED_BODY) && item.FAILED_BODY.Contains("{0}"))
                                        MESSAGE_BODY = string.Format(item.FAILED_BODY, MESSAGE_BODY);
                                    else
                                        MESSAGE_BODY = item.FAILED_BODY;
                                }
                            }

                            if (MESSAGE_TITLE == "{0}" && MESSAGE_BODY == "{0}")
                                return;

                            if (string.IsNullOrEmpty(MESSAGE_TITLE) && string.IsNullOrEmpty(MESSAGE_BODY))
                                return;

                            if (item.PREFERENCES_TYPE == this.EmailNotification)
                            {
                                sandEmailList.Add(new()
                                {
                                    ACTION = command.CommandText,
                                    EMAIL = item.EMAIL,
                                    SUBJECT = MESSAGE_TITLE,
                                    BODY = MESSAGE_BODY,
                                });
                            }
                            else if (item.PREFERENCES_TYPE == this.PushNotification)
                            {
                                tokenDataTable = this.GetFirebaseFCM_Token(command.CommandText, item.EMAIL);

                                if (tokenDataTable != null && tokenDataTable.DataTable != null)
                                    foreach (var itemToken in tokenDataTable.DataTable.DataRows)
                                        pushModelList.Add(new()
                                        {
                                            Action = command.CommandText,
                                            Email = item.EMAIL,
                                            Token = itemToken.String("TOKEN_STR"),
                                            Title = MESSAGE_TITLE,
                                            Body = MESSAGE_BODY,
                                            ImageUrl = !string.IsNullOrEmpty(IMAGE_URL) ? IMAGE_URL : brokerData.Response.Status.ToString(),
                                            Data = null,
                                        });
                            }
                        }
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

            if (!this.keyValues.TryGetValue(key, out object? obj1))
            {
                tokenDataTable = new TokenDataTable(DateTime.Now.AddSeconds(this.ReflashSeconds));//1분   2분5초-1분=>1분5초
                if (!this.keyValues.TryAdd(key, tokenDataTable))
                    Factory.Logger.Warning("GetFirebaseFCM_Token TryAdd Fail : {0}", key);
            }
            else if (obj1 is TokenDataTable tokenDataTable1)
                tokenDataTable = tokenDataTable1;

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
                        tokenDataTable.DataTable = response.DataSet.DataTables[0];
                        tokenDataTable.DateTime = DateTime.Now;

                        return tokenDataTable;
                    }
                }
                else
                    Factory.Logger.Error("GetFirebaseFCM_Token Request Fail : {0} {1}", key, response.Message);

                Factory.Logger.Error("Get FirebaseFCM Token Fail !! : {0}", key);
            }
            else
                return tokenDataTable;

            return null;
        }

        private void SandPush(List<PushModel> pushModelList, DateTime dateTime)
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

            if (pushModelList[0].Action != this.PushNotification && this.keyValues.TryGetValue(key, out object? obj) && obj is Preferences preferences1)
                preferences = preferences1;

            foreach (var item in pushModelList)
            {
                if (item.Action != this.PushNotification && preferences != null)
                {
                    var itemPreferences = preferences.PreferencesList.SingleOrDefault(x => x.EMAIL == item.Email && x.PREFERENCES_TYPE == this.PushNotification && x.PREFERENCES_KEY == item.Action);

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
                Factory.Logger.Error("SandPush Request Fail : {0} {1}", key, response.Message);
        }
        private void SandEmail(List<SandEmailModel> sandEmailList)
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

            if (sandEmailList[0].ACTION != this.EmailNotification && this.keyValues.TryGetValue(key, out object? obj) && obj is Preferences preferences1)
                preferences = preferences1;

            foreach (var item in sandEmailList)
            {
                if (item.ACTION != this.EmailNotification && preferences != null)
                {
                    var itemPreferences = preferences.PreferencesList.SingleOrDefault(x => x.EMAIL == item.EMAIL && x.PREFERENCES_TYPE == this.EmailNotification && x.PREFERENCES_KEY == item.ACTION);

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
                Factory.Logger.Error("SandEmail Request Fail : {0} {1}", key, response.Message);
        }

        private void LoadPreferences(int? USER_ID, string? EMAIL)
        {
            string key;

            if (USER_ID != null)
                key = $"Preferences.{USER_ID}";
            else
                key = $"Preferences.{EMAIL}";

            if (!this.keyValues.TryGetValue(key, out object? obj))
            {
                obj = new Preferences(DateTime.Now.AddSeconds(this.ReflashSeconds));//1분   2분5초-1분=>1분5초
                if (!this.keyValues.TryAdd(key, obj))
                    Factory.Logger.Warning("LoadPreferences TryAdd Fail : {0}", key);
            }

            if (obj != null && obj is Preferences preferences && (preferences.DateTime <= DateTime.Now.AddSeconds(this.ReflashSeconds) || preferences.PreferencesList.Count < 1))
            {
                preferences.PreferencesList.Clear();
                preferences.PreferencesList = this.LoadPreferencesDB(USER_ID, EMAIL);
                preferences.DateTime = DateTime.Now;

                if (USER_ID != null)
                {
                    if (preferences.PreferencesList.Count > 0)
                    {
                        key = $"Preferences.{preferences.PreferencesList[0].EMAIL}";

                        if (!this.keyValues.TryGetValue(key, out var _) && !this.keyValues.TryAdd(key, preferences))
                            Factory.Logger.Warning("LoadPreferences keyValues TryAdd Fail : {0}", key);
                    }
                }
                else
                {
                    if (preferences.PreferencesList.Count > 0 && preferences.PreferencesList[0].USER_ID > 0)
                    {
                        key = $"Preferences.{preferences.PreferencesList[0].USER_ID}";

                        if (!this.keyValues.TryGetValue(key, out var _) && !this.keyValues.TryAdd(key, preferences))
                            Factory.Logger.Warning("LoadPreferences keyValues TryAdd Fail : {0}", key);
                    }
                }
            }
        }
        private List<PreferencesModel> LoadPreferencesDB(int? USER_ID, string? EMAIL)
        {
            IService service;
            Response response;
            List<PreferencesModel> preferences = [];

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
                            OK_TITLE = item.String(nameof(PreferencesModel.OK_TITLE)) ?? "",
                            OK_BODY = item.String(nameof(PreferencesModel.OK_BODY)) ?? "",
                            FAILED_TITLE = item.String(nameof(PreferencesModel.FAILED_TITLE)) ?? "",
                            FAILED_BODY = item.String(nameof(PreferencesModel.FAILED_BODY)) ?? "",
                        });
            }
            else
                Factory.Logger.Error("LoadPreferencesDB Request Fail : {0} {1} {2}", USER_ID, EMAIL, response.Message);

            return preferences;
        }

        Task<string> IServiceString.RequestAsync(string data)
        {
            string result = ((IServiceString)this).Request(data);

            return Task.FromResult(result);
        }
    }
}