// ReSharper disable once CheckNamespace
namespace System.Threading.Tasks
{
    using System.Runtime.CompilerServices;

    public static class TaskExtensions
    {
        public static ConfiguredTaskAwaitable<T> NotOnOriginalContext<T>(this Task<T> task)
        {
            return task.ConfigureAwait(false);
        }

        public static ConfiguredTaskAwaitable NotOnOriginalContext(this Task task)
        {
            return task.ConfigureAwait(false);
        }
    }
}
