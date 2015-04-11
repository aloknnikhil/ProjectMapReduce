import java.io.File;

/**
 * Created by Alok on 4/11/15 in ProjectMapReduce
 */
public class Session {

    private static Session sessionInstance;
    private File sessionDir;
    public static boolean flag = false;

    private Session()   {
        configureSession();
    }

    private void configureSession() {
        sessionDir = new File("session" + System.currentTimeMillis());
        sessionDir.mkdir();
    }

    public static File getRootDir() {
        return sessionInstance.sessionDir;
    }

    private static Session getSessionInstance() {
        if(sessionInstance == null)
            sessionInstance = new Session();

        return sessionInstance;
    }

    public static void startJobTracker()  {
        Node slaveNodeID = null;
        while(ResourceManager.getIdleSlavePaths().size() != ResourceManager.getAllSlavePaths().size())  {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        for (String slavePath : ResourceManager.getIdleSlavePaths())    {

        }

        TaskManager.assignTaskTo(TaskManager.getTaskFor(slaveNodeID), slaveNodeID);

    }

    public static void startTaskTracker() {

    }

    public static void startDataNode()    {

    }

    public static void exit(int status) {
        System.exit(status);
    }

}
