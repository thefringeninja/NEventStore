namespace NEventStore.Persistence.Sql
{
    using System;
    using System.Data;
    using System.Threading.Tasks;
    using NEventStore.Persistence.Sql.SqlDialects;

    public interface IDbStatement : IDisposable
    {
        int PageSize { get; set; }

        void AddParameter(string name, object value, DbType? parameterType = null);

        int ExecuteNonQuery(string commandText);

        Task<int> ExecuteNonQueryAsync(string commandText);

        int ExecuteWithoutExceptions(string commandText);

        Task<int> ExecuteWithoutExceptionsAsync(string commandText);

        object ExecuteScalar(string commandText);

        Task<object> ExecuteScalarAsync(string commandText);

        IObservable<IDataRecord> ExecuteWithQuery(string queryText);

        IObservable<IDataRecord> ExecutePagedQuery(string queryText, NextPageDelegate nextPage = null);
    }
}