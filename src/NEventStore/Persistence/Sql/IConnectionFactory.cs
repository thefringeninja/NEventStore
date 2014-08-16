namespace NEventStore.Persistence.Sql
{
    using System;
    using System.Data.Common;
    using System.Threading.Tasks;

    public interface IConnectionFactory
    {
        Task<DbConnection> Open();

        Type GetDbProviderFactoryType();
    }
}