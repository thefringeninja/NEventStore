namespace NEventStore.Persistence.AcceptanceTests
{
    using NEventStore.Persistence.Sql;
    using NEventStore.Persistence.Sql.SqlDialects;
    using NEventStore.Serialization;

    public partial class PersistenceEngineFixture
    {
        public PersistenceEngineFixture()
        {
            _createPersistence = pageSize =>
                new SqlPersistenceFactory(new ConfigurationConnectionFactory("MsSql", "System.Data.SqlClient", "Server=(localdb)\\v11.0;Integrated Security=true;"), 
                    new BinarySerializer(),
                    new MsSqlDialect(),
                    pageSize: pageSize).Build();
        }
    }
}