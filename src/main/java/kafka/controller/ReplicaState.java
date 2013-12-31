package kafka.controller;

public enum ReplicaState {
    NewReplica {
        @Override
        public byte state() {
            return 1;
        }
    }, OnlineReplica {
        @Override
        public byte state() {
            return 2;
        }
    }, OfflineReplica {
        @Override
        public byte state() {
            return 3;
        }
    }, NonExistentReplica {
        @Override
        public byte state() {
            return 4;
        }
    };

    public abstract byte state();
}
