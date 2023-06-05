using System;
using System.Text;

namespace Azure.EntityServices.Blobs.Extensions
{
    public static class BinaryDataExtensions
    {
        public static string ToMD5(this BinaryData input)
        {
            // Use input string to calculate MD5 hash
            using var md5 = System.Security.Cryptography.MD5.Create();
            var hashBytes = md5.ComputeHash(input.ToArray());

            // Convert the byte array to hexadecimal string
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < hashBytes.Length; i++)
            {
                sb.Append(hashBytes[i].ToString("X2"));
            }
            return sb.ToString();
        }
    }
}