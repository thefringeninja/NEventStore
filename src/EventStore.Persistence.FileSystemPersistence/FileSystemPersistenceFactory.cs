namespace EventStore.Persistence.FileSystemPersistence
{
	public class FileSystemPersistenceFactory : IPersistenceFactory
	{
		private readonly ISerialize serializer;
		private readonly string directory;

		public FileSystemPersistenceFactory(string directory, ISerialize serializer)
		{
			this.serializer = serializer;
			this.directory = directory;
		}

		#region IPersistenceFactory Members

		public IPersistStreams Build()
		{
			return new FileSystemPersistenceEngine(this.directory, this.serializer);
		}

		#endregion
	}
}