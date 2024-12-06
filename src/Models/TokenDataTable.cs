namespace MetaFrm.Models
{
    internal class TokenDataTable(DateTime dateTime) : ICore
    {
        public DateTime DateTime { get; set; } = dateTime;

        public Data.DataTable? DataTable { get; set; }
    }
}