using System;

namespace TheFactory.Datastore {
    public interface IKeyValuePair {
        byte[] Key { get; }
        byte[] Value { get; }
        bool IsDeleted { get; }
    }
}
