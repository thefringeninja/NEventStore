namespace NEventStore.Persistence.Sql
{
    using System;
    using System.Threading.Tasks;

    public interface IConnectionFactory
    {
        Task<IDbConnectionAsync> Open();

        Type GetDbProviderFactoryType();
    }
}