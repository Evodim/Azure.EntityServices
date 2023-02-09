namespace Azure.EntityServices.Queries.Core
{
    public abstract class InstructionsProviderBase : IQueryInstructionsProvider
    {
        public virtual string Get(string instruction)
        {
            if (string.IsNullOrEmpty(instruction))
            {
                return string.Empty;
            }
            var type = GetType();
            var value = type.GetProperty(instruction)?.GetValue(this) as string;
            return value;
        }
    }
}