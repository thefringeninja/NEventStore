namespace NEventStore.Persistence
{
    using System;
    using System.Collections.Generic;

    public static class PersistStreamsExtensions
    {
        /// <summary>
        /// Deletes a stream from the default bucket.
        /// </summary>
        /// <param name="persistStreams">The IPersistStreams instance.</param>
        /// <param name="streamId">The stream id to be deleted.</param>
        public static void DeleteStream(this IPersistStreams persistStreams, string streamId)
        {
            if (persistStreams == null)
            {
                throw new ArgumentException("persistStreams is null");
            }
            persistStreams.DeleteStream(Bucket.Default, streamId);
        }

        /// <summary>
        ///     Gets all commits after from start checkpoint.
        /// </summary>
        /// <param name="persistStreams">The IPersistStreams instance.</param>
        public static IObservable<ICommit> GetFromStart(this IPersistStreams persistStreams)
        {
            if (persistStreams == null)
            {
                throw new ArgumentException("persistStreams is null");
            }
            return persistStreams.GetFrom();
        }
    }
}