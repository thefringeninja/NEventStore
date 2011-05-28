using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace EventStore.Persistence.FileSystemPersistence
{
	public struct FileSystemCommit
	{
		public Guid CommitId;
		public DateTime CommitStamp;
		public byte[] Headers;
		public byte[] Blob;
		public Guid StreamId;
		public int CommitSequence;
		public int StreamRevision;
	}
}
