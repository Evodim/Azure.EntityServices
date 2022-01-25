namespace Azure.EntityServices.Queries.Core
{
    public interface IQueryInstructionsProvider
    {
        string Get(string instruction);
    }
}