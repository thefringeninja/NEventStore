namespace NEventStore.Client
{
    using System;
    using System.Threading.Tasks;

    internal static class TaskExtensions
    {
        internal static Task WithTimeout(this Task task, int timeoutSeconds)
        {
            return WithTimeout(task, TimeSpan.FromSeconds(timeoutSeconds));
        }

        internal static async Task WithTimeout(this Task task, TimeSpan timespan)
        {
            Task timeoutTask = Task.Delay(timespan);
            var completedTask = await Task.WhenAny(timeoutTask, task);
            if (completedTask == timeoutTask)
            {
                throw new TimeoutException();
            }
        }
    }
}