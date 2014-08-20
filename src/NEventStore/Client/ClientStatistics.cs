namespace NEventStore.Client
{
    using System.Collections.Generic;
    using System.Linq;

    public class ClientStatistics
    {
        private readonly int _pollingIntervalMilliSeconds;
        private readonly int _pageSize;
        private readonly SubscriberInfo[] _subscriberInfos;

        public ClientStatistics(int pollingIntervalMilliSeconds, int pageSize, IEnumerable<SubscriberInfo> subscriberInfos )
        {
            _pollingIntervalMilliSeconds = pollingIntervalMilliSeconds;
            _pageSize = pageSize;
            _subscriberInfos = subscriberInfos.ToArray();
        }
        public int PollingIntervalMilliSeconds
        {
            get { return _pollingIntervalMilliSeconds; }
        }

        public int PageSize
        {
            get { return _pageSize; }
        }

        public SubscriberInfo[] SubscriberInfos
        {
            get { return _subscriberInfos; }
        }
    }
}