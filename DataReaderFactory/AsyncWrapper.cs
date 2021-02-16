namespace DataReaderFactory
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class AsyncWrapper<T> : IAsyncEnumerator<T>
    {
        private readonly IEnumerator<T> _enumerator;

        public AsyncWrapper(IEnumerator<T> enumerator) => _enumerator = enumerator;

        public T Current => _enumerator.Current;

        public ValueTask DisposeAsync()
        {
            _enumerator.Dispose();
            return new();
        }

        public ValueTask<bool> MoveNextAsync() => new(_enumerator.MoveNext());
    }
}
