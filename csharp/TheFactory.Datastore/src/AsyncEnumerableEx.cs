using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TheFactory.Datastore
{
    static class AsyncEnumerableEx {
        public static IAsyncEnumerable<T> Return<T>(Func<Task<T>> value)
        {
            return new ReturnEnumerable<T>(value);
        }

        class ReturnEnumerable<T> : IAsyncEnumerable<T>
        {
            private Func<Task<T>> factory;

            public ReturnEnumerable(Func<Task<T>> factory)
            {
                this.factory = factory;
            }

            public IAsyncEnumerator<T> GetEnumerator() {
                return new ReturnEnumerator<T>(factory);
            }

            class ReturnEnumerator<T> : IAsyncEnumerator<T> {
                private Func<Task<T>> factory;
                private int hasValue = 0;
                private int hasStarted = 0;
                private T value = default(T);

                public ReturnEnumerator(Func<Task<T>> factory) {
                    this.factory = factory;
                }

                public T Current {
                    get {
                        if (hasValue != 0) {
                            throw new InvalidOperationException("Call MoveNext first and await it");
                        }

                        return value;
                    }
                }

                public async Task<bool> MoveNext(CancellationToken cancellationToken) {
                    if (Interlocked.Exchange(ref hasStarted, 1) == 1) return false;

                    value = await factory();
                    hasValue = 1;

                    return false;
                }

                public void Dispose() { }
            }
        }
    }
}
