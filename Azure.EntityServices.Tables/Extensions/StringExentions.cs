namespace Azure.EntityServices.Tables.Extensions
{
    internal static class StringExentions
    {
        public static string ToShortHash(this string value)
        {
            var allowedSymbols = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".ToCharArray();
            var hash = new char[6];

            for (int i = 0; i < value.Length; i++)
            {
                hash[i % 6] = (char)(hash[i % 6] ^ value[i]);
            }

            for (int i = 0; i < 6; i++)
            {
                hash[i] = allowedSymbols[hash[i] % allowedSymbols.Length];
            }

            return new string(hash);
        }
    }
}