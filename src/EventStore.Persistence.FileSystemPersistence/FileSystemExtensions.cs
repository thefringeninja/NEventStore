using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using EventStore.Serialization;

namespace EventStore.Persistence.FileSystemPersistence
{
	public static class FileSystemExtensions
	{
		public static FileSystemCommit ToFileSystemCommit(this Commit commit, ISerialize serializer)
		{
			return new FileSystemCommit
			       	{
			       		CommitId = commit.CommitId,
						CommitSequence = commit.CommitSequence,
			       		CommitStamp = commit.CommitStamp,
						StreamId = commit.StreamId,
						StreamRevision = commit.StreamRevision,
						Headers = serializer.Serialize(commit.Headers),
						Blob = serializer.Serialize(commit.Events)
			       	};
		}
		public static FileSystemCommit? Read(this FileStream fileStream, HashAlgorithm hashAlgorithm)
		{
			var reader = new BinaryReader(fileStream);

			var hashSize = hashAlgorithm.HashSize / 8;
			while (fileStream.Position <= fileStream.Length)
			{
				var length = reader.ReadInt32();
				var data = reader.ReadBytes(length);
				var hashCode = reader.ReadBytes(hashSize);

				if (false == hashAlgorithm.ComputeHash(data).SequenceEqual(hashCode))
				{
					// didn't happen. move onto the next one.
					continue;
				}

				using (var stream = new MemoryStream(data))
				using (reader = new BinaryReader(stream))
				{
					var streamRevision = reader.ReadInt32();
					var commitId = new Guid(reader.ReadBytes(16));
					var commitSequence = reader.ReadInt32();
					var commitStamp = new DateTime(reader.ReadInt64());
					var headers = reader.ReadBytes(reader.ReadInt32());
					var blob = reader.ReadBytes(reader.ReadInt32());

					return new FileSystemCommit
					       	{
					       		Blob = blob,
					       		CommitId = commitId,
					       		Headers = headers,
					       		CommitStamp = commitStamp,
								CommitSequence = commitSequence,
					       		StreamId = Guid.Parse(Path.GetFileNameWithoutExtension(fileStream.Name)),
								StreamRevision = streamRevision
					       	};
				}
			}

			return default(FileSystemCommit?);
		}
		public static void Write(this FileSystemCommit commit, DirectoryInfo dataStorage, HashAlgorithm hashAlgorithm)
		{
			var streamId = commit.StreamId;
			var path = streamId.GetStreamLocation(dataStorage);
			try
			{
				using (var fs = File.Open(path, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read))
				using (var stream = new MemoryStream())
				using (var writer = new BinaryWriter(stream))
				{
					writer.Write(commit.StreamRevision);
					writer.Write(commit.CommitId.ToByteArray());
					writer.Write(commit.CommitSequence);
					writer.Write(commit.CommitStamp.Ticks);
					writer.Write(commit.Headers.Length);
					writer.Write(commit.Headers);
					writer.Write(commit.Blob.Length);
					writer.Write(commit.Blob);

					stream.Position = 0;

					var length = (int) stream.Length;

					fs.Write(BitConverter.GetBytes(length), 0, 4);
					stream.CopyTo(fs);
					var hash = hashAlgorithm.ComputeHash(stream.ToArray());
					fs.Write(hash, 0, hashAlgorithm.HashSize / 8);
				}
			}
			catch (IOException e)
			{
				throw new StorageException(e.Message, e);
			}
		}
	}
}