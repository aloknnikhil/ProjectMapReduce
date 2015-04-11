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
    private Socket accessSocket;

    public Node(NodeType type, Socket accessSocket) {
        this.type = type;
        this.accessSocket = accessSocket;
    }

    public void startNode() {
        if(type == NodeType.MASTER) {
            startJobTracker();
            startTaskTracker();
            FileSystem.setFileSystemManager(this);
            startDataNode();
        }
    }
}
