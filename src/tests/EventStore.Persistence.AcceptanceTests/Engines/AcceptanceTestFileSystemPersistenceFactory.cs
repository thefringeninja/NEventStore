using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using EventStore.Persistence.FileSystemPersistence;
using EventStore.Serialization;

namespace EventStore.Persistence.AcceptanceTests.Engines
{
	public class AcceptanceTestFileSystemPersistenceFactory : FileSystemPersistenceFactory
	{
		public AcceptanceTestFileSystemPersistenceFactory()
			: base("data", new BinarySerializer())
		{
			
		}
	}
}
