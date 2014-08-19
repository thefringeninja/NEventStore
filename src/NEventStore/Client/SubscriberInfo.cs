namespace NEventStore.Client
{
    using System;

    public class SubscriberInfo
    {
        private readonly Guid _subscriberId;
        private readonly string _currentCheckpoint;
        private readonly int _queueCount;

        public SubscriberInfo(Guid subscriberId, string currentCheckpoint, int queueCount)
        {
            _subscriberId = subscriberId;
            _currentCheckpoint = currentCheckpoint;
            _queueCount = queueCount;
        }

        public Guid SubscriberId
        {
            get { return _subscriberId; }
        }

        public string CurrentCheckpoint
        {
            get { return _currentCheckpoint; }
        }

        public int QueueCount
        {
            get { return _queueCount; }
        }
    }
}