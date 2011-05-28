using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using EventStore.Serialization;

namespace EventStore.Persistence.FileSystemPersistence
{
	public class FileSystemPersistenceEngine : IPersistStreams
	{
		private readonly DirectoryInfo dataStorage;
		private readonly ISerialize serializer;
		private int initialized;

		public FileSystemPersistenceEngine(string directory, ISerialize serializer)
		{
			this.dataStorage = new DirectoryInfo(directory);
			this.serializer = serializer;
		}

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
			attempt.ToFileSystemCommit(serializer)
				.Write(dataStorage, MD5.Create());
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
			if (Interlocked.Increment(ref this.initialized) > 1)
				return;

			try
			{
				if (false == dataStorage.Exists)
				{
					dataStorage.Create();
				}
			}
			catch (Exception e)
			{
				throw new StorageUnavailableException(e.Message, e);
			}

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