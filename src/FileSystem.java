/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class FileSystem {

    private static FileSystem fileSystemInstance;
    private Node masterNode;

    private FileSystem()    {
        //Placeholder to enforce Singleton Paradigm
    }

    public static void setFileSystemManager(Node masterNode)    {

        getFileSystemInstance().masterNode = masterNode;
    }

    public static void startNameNodeService()    {

    }

    private static FileSystem getFileSystemInstance() {

        if(fileSystemInstance == null) {
            fileSystemInstance = new FileSystem();
        }

        return fileSystemInstance;
    }
}
