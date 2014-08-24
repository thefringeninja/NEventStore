namespace NEventStore.Client
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using FluentAssertions;
    using Xunit;

    public class EventStoreClientTests
    {
        [Fact]
        public async Task When_commiting_events_then_should_received_them_in_same_order()
        {
            var storeEvents = Wireup.Init().UsingInMemoryPersistence().Build();

            using (var client = new EventStoreClient(storeEvents.Advanced, 25))
            {
                const int total = 100;
                int count = 0;
                var messagesStored = new List<string>();
                var messagesRecieved = new List<string>();
                var allCommitsReceived = new TaskCompletionSource<ICommit>();

                using (client.Subscribe(null, commit =>
                {
                    messagesRecieved.Add(commit.Events.Single().Body.ToString());
                    count ++;
                    if (count == total)
                    {
                        allCommitsReceived.SetResult(commit);
                    }
                }))
                {
                    for (int i = 0; i < total; i++)
                    {
                        string message = "Message " + i;
                        using (var stream = storeEvents.CreateStream(Guid.NewGuid()))
                        {
                            stream.Add(new EventMessage { Body = message });
                            await stream.CommitChanges(Guid.NewGuid());
                        }
                        messagesStored.Add(message);
                    }

                    await allCommitsReceived.Task.WithTimeout(10);
                }

                messagesRecieved.ShouldBeEquivalentTo(messagesStored);
            }
        }

        [Fact]
        public async Task With_multiple_subscribers_that_process_at_different_rates_all_messages_should_be_received()
        {
            var eventStore = Wireup.Init().UsingInMemoryPersistence().Build();
            using (var client = new EventStoreClient(eventStore.Advanced, 50, 10))
            {
                const int total = 50;
                var messagesStored = new List<string>();
                var exampleSubscribers = new List<ExampleSubscriber>();
                for (int i = 0; i < 20; i++)
                {
                    exampleSubscribers.Add(new ExampleSubscriber(client, total, 0));
                }

                for (int i = 0; i < total; i++)
                {
                    string message = "Message " + i;
                    using (var stream = eventStore.CreateStream(Guid.NewGuid()))
                    {
                        stream.Add(new EventMessage { Body = message });
                        await stream.CommitChanges(Guid.NewGuid());
                    }
                    messagesStored.Add(message);
                }

                await Task.WhenAll(exampleSubscribers.Select(s => s.OnMessagesReceived)).WithTimeout(10);

                foreach (var exampleSubscriber in exampleSubscribers)
                {
                    messagesStored.ShouldBeEquivalentTo(exampleSubscriber.MessagesReceived);
                }
            }
        }

        private class ExampleSubscriber
        {
            private readonly List<string> _messagesReceived = new List<string>();
            private readonly TaskCompletionSource<int> _tcs = new TaskCompletionSource<int>();
            private readonly IDisposable _subscription;

            public ExampleSubscriber(EventStoreClient client, int expectedMessageCount, int delay)
            {
                int count =0;
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
}