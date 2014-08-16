namespace NEventStore.Persistence.AcceptanceTests.BDD
{
    using System.Threading.Tasks;
    using Xunit;

    [RunWith(typeof (SpecificationBaseRunner))]
    public abstract class SpecificationBase
    {
        protected virtual void Because()
        {
            BecauseAsync().Wait();
        }

        protected virtual Task BecauseAsync()
        {
            return Task.FromResult(0);
        }

        protected virtual void Cleanup()
        {
            CleanupAsync().Wait();
        }

        protected virtual Task CleanupAsync()
        {
            return Task.FromResult(0);
        }

        protected virtual void Context()
        {
            ContextAsync().Wait();
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
    }
}