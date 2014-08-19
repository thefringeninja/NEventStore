namespace NEventStore.ClientExample
{
    using System;
    using NEventStore.Client;
    using NEventStore.Persistence.Sql.SqlDialects;

    internal static class MainProgram
    {
        private static readonly byte[] EncryptionKey = { 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf };

        private static void Main()
        {
            using (var store = WireupEventStore())
            {
                using (var client = new EventStoreClient(store.Advanced))
                {
                    Action<ICommit> onCommit = commit =>
                    {
                        Console.WriteLine(Resources.CommitInfo, commit.BucketId, commit.StreamId, commit.CommitSequence);
                        SaveCheckpoint(commit.CheckpointToken);
                    };
                    using (client.Subscribe(LoadCheckpoint(), onCommit))
                    {
                        Console.WriteLine(Resources.PressAnyKey);
                        Console.ReadKey();
                    }
                }
            }
        }

        private static string LoadCheckpoint()
        {
            // Load the checkpoint value from disk / local db/ etc
            return null;
        }

        private static void SaveCheckpoint(string checkpointToken)
        {
            //Save checkpointValue to disk / whatever.
        }

        private static IStoreEvents WireupEventStore()
        {
            return
                Wireup.Init()
                    .LogToOutputWindow()
                    .UsingInMemoryPersistence()
                    .UsingSqlPersistence("NEventStore") // Connection string is in app.config
                        .WithDialect(new MsSqlDialect())
                        .InitializeStorageEngine()
                        .TrackPerformanceInstance("example")
                        .UsingJsonSerialization()
                        .Compress()
                        .EncryptWith(EncryptionKey)
                    .Build();
        }
    }
}