namespace MetaFrm.Models
{
    internal class TokenDataTable : ICore
    {
        public DateTime DateTime { get; set; }

        public Data.DataTable? DataTable { get; set; }

        public TokenDataTable(DateTime dateTime)
        {
            DateTime = dateTime;
        }
    }
}