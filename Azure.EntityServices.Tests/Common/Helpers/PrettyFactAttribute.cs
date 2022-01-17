using System;
using System.Linq;
using System.Runtime.CompilerServices;
using Xunit;

namespace Azure.EntityServices.Tests.Common.Helpers
{
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    public class PrettyFactAttribute : FactAttribute
    {
        public PrettyFactAttribute([CallerMemberName] string caller = null)
        {
            DisplayName = Prettify(caller);
        }

        protected virtual string Prettify(string displayName) =>
            string.Join("",
                displayName.Split("_")
                .SelectMany(d => d
                .Select(c => (char.IsUpper(c)) ? $" {char.ToLowerInvariant(c)}" : $"{c}"))
                .ToList()
                );

        public new string DisplayName { get; }
    }
}