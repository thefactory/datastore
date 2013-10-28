using System;

namespace TheFactory.Datastore {
    public interface IKeyValuePair {
        Slice Key { get; }
        Slice Value { get; }
        bool IsDeleted { get; }
    }
}
