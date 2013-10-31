using System;

namespace TheFactory.Datastore {
    public interface IKeyValuePair {
        Slice Key { get; }
        Slice Value { get; }
        bool IsDeleted { get; }
    }

    // a bare-bones Pair for use when we don't need anything fancy
    public class Pair: IKeyValuePair {
        public Slice Key { get; set; }
        public Slice Value { get; set; }
        public bool IsDeleted {get; set; }

        public void Reset() {
            Key = null;
            Value = null;
            IsDeleted = false;
        }
    }
}
