using System;

namespace Azure.EntityServices.Tables.Extensions
{
    public static class GuidExtensions
    {  
        public static string ToShortGuid(this Guid newGuid)
        {
            return Convert.ToBase64String(newGuid.ToByteArray())
                .Replace('+', '-').Replace('/', '_')[..22]; // avoid invalid URL characters
        } 
        public static Guid ParseShortGuid(string shortGuid)
        {
            return new Guid(Convert.FromBase64String($"{shortGuid.Replace('-', '+').Replace('_', '/')}=="));
        }
        public static string NewShortGuid()
        {
             return Guid.NewGuid().ToShortGuid();
        } 
    }
}