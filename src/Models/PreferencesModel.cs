namespace MetaFrm.Models
{
    internal class PreferencesModel : ICore
    {
        public int? USER_ID { get; set; }
        public string? EMAIL { get; set; }
        public string? PLATFORM { get; set; }
        public string? DEVICE_MODEL { get; set; }
        public string? DEVICE_NAME { get; set; }
        public string? PREFERENCES_TYPE { get; set; }
        public string? PREFERENCES_KEY { get; set; }
        public string? PREFERENCES_VALUE { get; set; }
        public string? OK_TITLE { get; set; }
        public string? OK_BODY { get; set; }
        public string? FAILED_TITLE { get; set; }
        public string? FAILED_BODY { get; set; }
    }
}