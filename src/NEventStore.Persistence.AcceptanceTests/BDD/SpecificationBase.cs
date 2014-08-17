namespace NEventStore.Persistence.AcceptanceTests.BDD
{
    using System;
    using System.Runtime.ExceptionServices;
    using System.Threading.Tasks;
    using Xunit;

    [RunWith(typeof (SpecificationBaseRunner))]
    public abstract class SpecificationBase
    {
        protected virtual void Because()
        {
            CatchAndThrow(BecauseAsync);
        }

        protected virtual Task BecauseAsync()
        {
            return Task.FromResult(0);
        }

        protected virtual void Cleanup()
        {
            CatchAndThrow(CleanupAsync);
        }

        protected virtual Task CleanupAsync()
        {
            return Task.FromResult(0);
        }

        protected virtual void Context()
        {
            CatchAndThrow(ContextAsync);
        }

        protected virtual Task ContextAsync()
        {
            return Task.FromResult(0);
        }

        public void OnFinish()
        {
            Cleanup();
        }

        public void OnStart()
        {
            Context();
            Because();
        }

        private void CatchAndThrow(Func<Task> action)
        {
            try
            {
                action().Wait();
            }
            catch (AggregateException ex)
            {
                ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
            }
        }
    }
}