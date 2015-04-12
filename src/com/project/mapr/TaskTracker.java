package com.project.mapr;

import com.project.ResourceManager;
import com.project.application.Mapper;
import com.project.application.Reducer;
import com.project.utils.Task;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class TaskTracker {

    Mapper mapper;
    Reducer reducer;

    public class TaskWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {

            switch (event.getType()) {
                case NodeCreated:
                    Task task = ResourceManager.getRunningTask(event.getPath());

                    switch (task.getStatus()) {
                        case INITIALIZED:
                            if(task.getType() == Task.Type.MAP)
                                runMap();
                            break;

                    }
                    break;
            }

        }
    }
}
