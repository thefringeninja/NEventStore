namespace NEventStore.Persistence
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reactive.Linq;
    using System.Threading.Tasks;
    using NEventStore.Logging;

    public class PipelineHooksAwarePersistanceDecorator : IPersistStreams
    {
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof (PipelineHooksAwarePersistanceDecorator));
        private readonly IPersistStreams _original;
        private readonly IEnumerable<IPipelineHook> _pipelineHooks;

        public PipelineHooksAwarePersistanceDecorator(IPersistStreams original, IEnumerable<IPipelineHook> pipelineHooks)
        {
            if (original == null)
            {
                throw new ArgumentNullException("original");
            }
            if (pipelineHooks == null)
            {
                throw new ArgumentNullException("pipelineHooks");
            }
            _original = original;
            _pipelineHooks = pipelineHooks;
        }

        public void Dispose()
        {
            _original.Dispose();
        }

        public IObservable<ICommit> GetFrom(string bucketId, string streamId, int minRevision, int maxRevision)
        {
            return ExecuteHooks(_original.GetFrom(bucketId, streamId, minRevision, maxRevision));
        }

        public Task<ICommit> Commit(CommitAttempt attempt)
        {
            return _original.Commit(attempt);
        }

        public ISnapshot GetSnapshot(string bucketId, string streamId, int maxRevision)
        {
            return _original.GetSnapshot(bucketId, streamId, maxRevision);
        }

        public bool AddSnapshot(ISnapshot snapshot)
        {
            return _original.AddSnapshot(snapshot);
        }

        public IEnumerable<IStreamHead> GetStreamsToSnapshot(string bucketId, int maxThreshold)
        {
            return _original.GetStreamsToSnapshot(bucketId, maxThreshold);
        }

        public void Initialize()
        {
            _original.Initialize();
        }

        public IObservable<ICommit> GetFrom(string checkpointToken = null)
        {
            return ExecuteHooks(_original.GetFrom(checkpointToken));
        }

        public ICheckpoint GetCheckpoint(string checkpointToken)
        {
            return _original.GetCheckpoint(checkpointToken);
        }

        public void Purge()
        {
            _original.Purge();
            foreach (var pipelineHook in _pipelineHooks)
            {
                pipelineHook.OnPurge();
            }
        }

        public void Purge(string bucketId)
        {
            _original.Purge(bucketId);
            foreach (var pipelineHook in _pipelineHooks)
            {
                pipelineHook.OnPurge(bucketId);
            }
        }

        public void Drop()
        {
            _original.Drop();
        }

        public void DeleteStream(string bucketId, string streamId)
        {
            _original.DeleteStream(bucketId, streamId);
            foreach (var pipelineHook in _pipelineHooks)
            {
                pipelineHook.OnDeleteStream(bucketId, streamId);
            }
        }

        public bool IsDisposed
        {
            get { return _original.IsDisposed; }
        }

        private ICommit Filter(ICommit commit)
        {
            var filtered = commit;

            foreach (var hook in _pipelineHooks)
            {
                filtered = hook.Select(filtered);

                if (filtered == null)
                {
                    Logger.Info(Resources.PipelineHookSkippedCommit, hook.GetType(), commit.CommitId);
                    break;
                };
            }

            if (filtered != null)
            {
                return filtered;
            }
            
            Logger.Info(Resources.PipelineHookFilteredCommit);
            return null;
        }

        private IObservable<ICommit> ExecuteHooks(IObservable<ICommit> commits)
        {
            return (from commit in commits
                let filtered = Filter(commit)
                where filtered != null
                select filtered);
        }
    }
}