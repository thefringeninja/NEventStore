namespace NEventStore.Persistence.Sql.SqlDialects
{
    using System;
    using System.Data;
    using System.Data.Common;
    using System.Reflection;
    using System.Transactions;
    using NEventStore.Persistence.Sql;

    public class OracleDbStatement : CommonDbStatement
    {
        private readonly ISqlDialect _dialect;

        public OracleDbStatement(
            ISqlDialect dialect,
            TransactionScope scope,
            DbConnection connection,
            IDbTransaction transaction)
            : base(dialect, scope, connection, transaction)
        {
            _dialect = dialect;
        }

        public override void AddParameter(string name, object value, DbType? dbType = null)
        {
            name = name.Replace('@', ':');

            if (value is Guid)
            {
                base.AddParameter(name, ((Guid) value).ToByteArray(), null);
            }
            else
            {
                base.AddParameter(name, value, dbType);
            }
        }

        protected override DbCommand BuildCommand(string statement)
        {
            DbCommand command = base.BuildCommand(statement);
            PropertyInfo pi = command.GetType().GetProperty("BindByName");
            if (pi != null)
            {
                pi.SetValue(command, true, null);
            }
            return command;
        }

        protected override void BuildParameter(IDbCommand command, string name, object value, DbType? dbType)
        {
            //HACK
            if (name == _dialect.Payload && value is DbParameter)
            {
                command.Parameters.Add(value);
                return;
            }
            base.BuildParameter(command, name, value, dbType);
        }
    }
}