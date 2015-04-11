import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Alok on 4/11/15 in ProjectMapReduce
 */
public class Session {

    private static Session sessionInstance;
    private File sessionDir;
    //TODO Replace local config access with zookeeper implementation
    private static File configDir;
    private List<String> slavesList;
    public static boolean flag = false;

    private Session()   {
        configureSession();
    }

    private void configureSession() {
        sessionDir = new File("session" + System.currentTimeMillis());
        sessionDir.mkdir();
        configDir = new File("config");
        if(!configDir.exists() || !configDir.isDirectory()) {
            LogFile.writeToLog("Could not find configuration files. Exiting setup");
            exit(-1);
        }

    }

    public static File getRootDir() {
        return sessionInstance.sessionDir;
    }

    private static Session getSessionInstance() {
        if(sessionInstance == null)
            sessionInstance = new Session();

        return sessionInstance;
    }

    public static List<String> getSlavesList() {
        if(getSessionInstance().slavesList == null) {
            String line;
            File slavesFile = new File(configDir, "slaves");
            getSessionInstance().slavesList = new ArrayList<>();
            if (!slavesFile.exists()) {
                LogFile.writeToLog("No slaves file! Exiting setup");
                exit(-1);
            }

            try {
                BufferedReader slaveReader = new BufferedReader(new FileReader(slavesFile));
                while ((line = slaveReader.readLine()) != null) {
                    getSessionInstance().slavesList.add(line);
                }
            } catch (java.io.IOException e) {
                e.printStackTrace();
            }

        }

        return getSessionInstance().slavesList;
    }

    public static void exit(int status) {
        System.exit(status);
    }

}
