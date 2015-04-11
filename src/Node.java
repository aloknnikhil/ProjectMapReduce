import java.net.Socket;

/**
 * Created by alok on 4/11/15
 */
public class Node {

    enum NodeType   {
        MASTER,
        SLAVE
    }

    private NodeType type;
    private int nodeID;
    private Socket accessSocket;

    public Node(NodeType type, Socket accessSocket) {
        this.type = type;
        this.accessSocket = accessSocket;
        nodeID = Utils.getNodeID(accessSocket.getInetAddress().getCanonicalHostName());
    }

    public void startNode() {

        //If the Node is a master node, then we need additional setup procedures
        if(type == NodeType.MASTER) {
            startJobTracker();
            FileSystem.setFileSystemManager(this);
            FileSystem.startNameNodeService();
        }

        startTaskTracker();
        startDataNode();
    }

    //TODO Replace with zookeeper implementation
    private void startJobTracker()  {
        for(String slave : Session.getSlavesList()) {

        }
    }

    private void startTaskTracker() {

    }

    private void startDataNode()    {

    }
}
