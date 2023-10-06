namespace MetaFrm.Models
{
    internal class Preferences : ICore
    {
        public DateTime DateTime { get; set; }

        public List<PreferencesModel> PreferencesList { get; set; } = new();

        public Preferences(DateTime dateTime)
        {
            DateTime = dateTime;
        }
    }
}