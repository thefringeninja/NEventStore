namespace NEventStore.Client
{
    using System;
    using System.Collections.Concurrent;
    using System.Linq;
    using System.Reactive.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using NEventStore.Persistence;

    public class EventStoreClient : IDisposable
    {
        private readonly IPersistStreams _persistStreams;
        private readonly int _pageSize;
        private readonly int _subscriberQueueThreshold;
        private readonly ConcurrentDictionary<Guid, Subscriber> _subscribers = new ConcurrentDictionary<Guid, Subscriber>();
        private InterlockedBoolean _isRetrieving = new InterlockedBoolean();
        private readonly IDisposable _retrieveTimer;
        private readonly IObservable<ClientStatistics> _statistics;

        public EventStoreClient(
            IPersistStreams persistStreams,
            int pollingIntervalMilliseconds = 5000,
            int pageSize = 25,
            int subscriberQueueThreshold = 10)
        {
            _persistStreams = persistStreams;
            _pageSize = pageSize;
            _subscriberQueueThreshold = subscriberQueueThreshold;
            _retrieveTimer = Observable
                .Interval(TimeSpan.FromMilliseconds(pollingIntervalMilliseconds))
                .Subscribe(_ => Retrieve());

            _statistics = Observable.Interval(TimeSpan.FromSeconds(1)).Select(_ =>
            {
                var subscrberInfos = _subscribers
                    .Select(pair => new SubscriberInfo(pair.Key, pair.Value.Checkpoint, pair.Value.QueueLength));
                return new ClientStatistics(pollingIntervalMilliseconds, pageSize, _subscriberQueueThreshold,
                    subscrberInfos);
            });
        }

        public IObservable<ClientStatistics> Statistics
        {
            get { return _statistics; }
        }

        public IDisposable Subscribe(string fromCheckpoint, Action<ICommit> onCommit)
        {
            var subscriberId = Guid.NewGuid();
            var subscriber = new Subscriber(
                fromCheckpoint,
                commit =>
                {
                    onCommit(commit);
                    return Task.FromResult(0);
                },
                _subscriberQueueThreshold,
                Retrieve,
                () =>
                {
                    Subscriber _;
                    _subscribers.TryRemove(subscriberId, out _);
                });
            _subscribers.TryAdd(subscriberId, subscriber);
            return subscriber;
        }

        public IDisposable Subscribe(string fromCheckpoint, Func<ICommit, Task> onCommit)
        {
            var subscriberId = Guid.NewGuid();
            var subscriber = new Subscriber(
                fromCheckpoint,
                onCommit,
                _subscriberQueueThreshold,
                Retrieve,
                () =>
                {
                    Subscriber _;
                    _subscribers.TryRemove(subscriberId, out _);
                });
            _subscribers.TryAdd(subscriberId, subscriber);
            return subscriber;
        }

        public void Dispose()
        {
            _retrieveTimer.Dispose();
        }

        private void Retrieve()
        {
            if (_isRetrieving.CompareExchange(true, false))
            {
                return;
            }
            
            Task.Run(() =>
            {
                foreach (var subscriber in _subscribers.Values.ToArray())
                {
                    if (subscriber.QueueLength > _subscriberQueueThreshold)
                    {
                        continue;
                    }
                    ICommit[] commits = _persistStreams //Will be async
                        .GetFrom(subscriber.Checkpoint)
                        .Take(_pageSize)
                        .ToArray();

                    foreach (var commit in commits) 
                    {
                        subscriber.Enqueue(commit);
                    }
                }
                _isRetrieving.Set(false);
            });
        }

        private class Subscriber : IDisposable
        {
            private string _checkpoint;
            private readonly Func<ICommit, Task> _onCommit;
            private readonly int _threshold;
            private readonly Action _onThreashold;
            private readonly Action _onDispose;
            private readonly ConcurrentQueue<ICommit> _commits = new ConcurrentQueue<ICommit>();
            private InterlockedBoolean _isPushing = new InterlockedBoolean();

            public Subscriber(
                string checkpoint,
                Func<ICommit, Task> onCommit,
                int threshold,
                Action onThreashold,
                Action onDispose)
            {
                _checkpoint = checkpoint;
                _onCommit = onCommit;
                _threshold = threshold;
                _onThreashold = onThreashold;
                _onDispose = onDispose;
            }

            public string Checkpoint
            {
                get { return _checkpoint; }
            }

            public void Enqueue(ICommit commit)
            {
                _commits.Enqueue(commit);
                _checkpoint = commit.CheckpointToken;
                Push();
            }

            public int QueueLength
            {
                get { return _commits.Count; }
            }

            public void Dispose()
            {
                _onDispose();
            }

            private void Push()
            {
                if (_isPushing.CompareExchange(true, false))
                {
                    return;
                }
                Task.Run(async () =>
                {
                    ICommit commit;
                    while (_commits.TryDequeue(out commit))
                    {
                        await _onCommit(commit);
                        if (_commits.Count == _threshold)
                        {
                            _onThreashold();
                        }
                    }
                    _isPushing.Set(false);
                });
            }
        }
    }
}