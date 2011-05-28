using System;
using System.Collections.Generic;

namespace EventStore.Persistence.FileSystemPersistence
{
	public class FileSystemPersistenceEngine : IPersistStreams
	{
		#region IPersistStreams Members

		public void Dispose()
		{
			// no op
		}

		public IEnumerable<Commit> GetFrom(Guid streamId, int minRevision, int maxRevision)
		{
			throw new NotImplementedException();
		}

		public void Commit(Commit attempt)
		{
			throw new NotImplementedException();
		}

		public Snapshot GetSnapshot(Guid streamId, int maxRevision)
		{
			throw new NotImplementedException();
		}

		public bool AddSnapshot(Snapshot snapshot)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<StreamHead> GetStreamsToSnapshot(int maxThreshold)
		{
			throw new NotImplementedException();
		}

		public void Initialize()
		{
			throw new NotImplementedException();
		}

		public IEnumerable<Commit> GetFrom(DateTime start)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<Commit> GetUndispatchedCommits()
		{
			throw new NotImplementedException();
		}

		public void MarkCommitAsDispatched(Commit commit)
		{
			throw new NotImplementedException();
		}

		#endregion
	}
}