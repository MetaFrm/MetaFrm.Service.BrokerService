namespace MetaFrm.Models
{
    internal class Preferences(DateTime dateTime) : ICore
    {
        public DateTime DateTime { get; set; } = dateTime;

        public List<PreferencesModel> PreferencesList { get; set; } = [];
    }
}