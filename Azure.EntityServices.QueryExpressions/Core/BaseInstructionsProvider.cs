namespace Azure.EntityServices.Queries.Core
{
    public abstract class InstructionsProviderBase : IQueryInstructionsProvider
    {
        public virtual string Get(string instruction)
        {
            if (instruction == null) return string.Empty;
            var type = GetType();
            var value = type.GetProperty(instruction)?.GetValue(this) as string;
            return value;
        }
    }
}