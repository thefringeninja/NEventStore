namespace NEventStore.ClientPerfTests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using MarkdownLog;
    using NEventStore.Client;
    using NEventStore.Persistence.Sql.SqlDialects;

    internal class MainProgram
    {
        private static void Main(string[] args)
        {
           Task.Run(async () => await MainAsync()).Wait();
        }

        private static async Task MainAsync()
        {
            if (File.Exists("NEventStore.db"))
            {
                File.Delete("NEventStore.db");
            }
            const int pageSize = 10;
            
            using (IStoreEvents store = Wireup.Init()
                .LogToOutputWindow()
                .UsingSqlPersistence("NEventStore") // Connection string is in app.config
                .WithDialect(new SqliteDialect())
                    .PageEvery(pageSize)
                .InitializeStorageEngine()
                .UsingJsonSerialization()
                .Build())
            {
                const int streamTotal = 20;
                const int commitsPerStream = 20;
                const int totalCommits = streamTotal * commitsPerStream;
                await SeedStore(store, streamTotal, commitsPerStream);

                using (var client = new EventStoreClient(store.Advanced))
                {
                    using (client.Statistics.Subscribe(stat => Console.Write(stat.SubscriberInfos.ToMarkdownTable())))
                    {
                        var exampleSubscribers = new List<ExampleSubscriber>();
                        for (int i = 0; i < 20; i++)
                        {
                            int delay = (i + 1) * 10;
                            exampleSubscribers.Add(new ExampleSubscriber(client, totalCommits, delay));
                        }

                        await Task.WhenAll(exampleSubscribers.Select(s => s.OnMessagesReceived));

                        Console.ReadLine();
                    }
                }
            }
        }

        private static async Task SeedStore(IStoreEvents store, int streamTotal, int commitsPerStream)
        {
            Console.Write("Seeding event store...");
            Console.CursorVisible = false;
            int totalCommits = streamTotal * commitsPerStream;
            for (int i = 0; i < streamTotal; i++)
            {
                var streamId = Guid.NewGuid();
                using (var stream = store.CreateStream(streamId))
                {
                    stream.Add(new EventMessage { Body = "message" });
                    await stream.CommitChanges(Guid.NewGuid());
                }

                for (int j = 0; j < commitsPerStream - 1; j++)
                {
                    using (var stream = store.OpenStream(streamId))
                    {
                        stream.Add(new EventMessage { Body = "message" });
                        await stream.CommitChanges(Guid.NewGuid());
                    }

                    string count = (((i * 10) + j) + "/" + totalCommits).PadRight(10, ' ');
                    Console.Write(count);
                    Console.SetCursorPosition(Console.CursorLeft - 10, Console.CursorTop);
                }
            }

            Console.WriteLine("complete");
        }
    }

    public class ExampleSubscriber
    {
        private readonly List<string> _messagesReceived = new List<string>();
        private readonly TaskCompletionSource<int> _tcs = new TaskCompletionSource<int>();
        private readonly IDisposable _subscription;

        public ExampleSubscriber(EventStoreClient client, int expectedMessageCount, int delay)
        {
            int count = 0;
            _subscription = client.Subscribe(null, async commit =>
            {
                await Task.Delay(delay);
                _messagesReceived.Add(commit.Events.First().Body.ToString());
                count++;
                if (count == expectedMessageCount)
                {
                    _tcs.SetResult(0);
                }
            });
        }

        public List<string> MessagesReceived
        {
            get { return _messagesReceived; }
        }

        public Task OnMessagesReceived
        {
            get { return _tcs.Task; }
        }
    }
}