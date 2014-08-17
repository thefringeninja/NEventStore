namespace CommonDomain.Persistence
{
	using System;
	using System.Collections.Generic;
	using System.Threading.Tasks;

    public interface ISagaRepository
	{
		TSaga GetById<TSaga>(string bucketId, string sagaId) where TSaga : class, ISaga, new();

		Task Save(string bucketId, ISaga saga, Guid commitId, Action<IDictionary<string, object>> updateHeaders);
	}
}