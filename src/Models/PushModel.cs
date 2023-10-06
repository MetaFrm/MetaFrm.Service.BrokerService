namespace MetaFrm.Models
{
    internal class PushModel : ICore
    {
        public string? Action { get; set; }
        public string? Email { get; set; }
        public string? Token { get; set; }
        public string? Title { get; set; }
        public string? Body { get; set; }
        public string? ImageUrl { get; set; }
        public string? Data { get; set; }
    }
}