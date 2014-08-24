namespace NEventStore.Persistence.AcceptanceTests
{
    using System;
    using System.Threading.Tasks;
    using FluentAssertions;
    using NEventStore.Persistence.AcceptanceTests.BDD;
    using NEventStore.Persistence.Sql;
    using NEventStore.Persistence.Sql.SqlDialects;
    using Xunit;

    public class when_specifying_a_hasher : SpecificationBase
    {
        private bool _hasherInvoked;
        private IStoreEvents _eventStore;

        protected override void Context()
        {
            _eventStore = Wireup
                .Init()
                .UsingSqlPersistence(new EnviromentConnectionFactory("MsSql", "System.Data.SqlClient"))
                .WithDialect(new MsSqlDialect())
                .WithStreamIdHasher(streamId =>
                {
                    _hasherInvoked = true;
                    return new Sha1StreamIdHasher().GetHash(streamId);
                })
                .InitializeStorageEngine()
                .UsingBinarySerialization()
                .Build();
        }

        protected override async Task CleanupAsync()
        {
            if (_eventStore != null)
            {
                await _eventStore.Advanced.Drop();
                _eventStore.Dispose();
            }
        }

        protected override Task BecauseAsync()
        {
            using (var stream = _eventStore.OpenStream(Guid.NewGuid()))
            {
                stream.Add(new EventMessage{ Body = "Message" });
                return stream.CommitChanges(Guid.NewGuid());
            }
        }

        [Fact]
        public void should_invoke_hasher()
        {
            _hasherInvoked.Should().BeTrue();
        }
    }
}