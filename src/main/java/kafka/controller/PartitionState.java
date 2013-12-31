package kafka.controller;

public enum PartitionState {
    NewPartition {
        @Override
        public byte state() {
            return 0;
        }
    }, OnlinePartition {
        @Override
        public byte state() {
            return 1;
        }
    }, OfflinePartition {
        @Override
        public byte state() {
            return 2;
        }
    }, NonExistentPartition {
        @Override
        public byte state() {
            return 3;
        }
    };


    public abstract byte state();
}
