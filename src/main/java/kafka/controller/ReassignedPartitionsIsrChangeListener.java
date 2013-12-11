package kafka.controller;

import org.I0Itec.zkclient.IZkDataListener;

public class ReassignedPartitionsIsrChangeListener implements IZkDataListener {
    @Override
    public void handleDataChange(String s, Object o) throws Exception {

    }

    @Override
    public void handleDataDeleted(String s) throws Exception {

    }
}
