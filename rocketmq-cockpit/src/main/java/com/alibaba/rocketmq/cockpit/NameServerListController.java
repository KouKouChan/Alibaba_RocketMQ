package com.alibaba.rocketmq.cockpit;

import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.List;

public class NameServerListController {

    private static final Logger LOGGER = LoggerFactory.getLogger(NameServerListController.class);

    private static final String configuration = "name.server.ini";

    private final File configurationFile;

    private String nameServerList;

    private WatchKey watchKey;

    public NameServerListController(String configurationDirectory) {
        configurationFile = new File(configurationDirectory + "/" + configuration);
        FileSystem fileSystem = FileSystems.getDefault();
        Path configPath = fileSystem.getPath(configurationDirectory);
        try {
            WatchService watchService = fileSystem.newWatchService();

            watchKey = configPath.register(watchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_DELETE);
        } catch (IOException e) {
            LOGGER.error("IO Error", e);
            throw new RuntimeException(e);
        }

        refreshConfigurationFile();
    }

    private void refreshConfigurationFile() {
        LOGGER.debug("Refresh configuration file");
        if (configurationFile.exists()) {
            try (BufferedReader bufferedReader = new BufferedReader(new FileReader(configurationFile))) {
                String updated = null;
                String line = null;
                while ((line = bufferedReader.readLine()) != null) {
                    if (!line.trim().isEmpty() && !line.trim().startsWith("#")) {
                        if (null == updated) {
                            updated = line;
                        } else {
                            updated += ";" + line;
                        }
                    }
                }

                if (null != updated) {
                    nameServerList = updated;
                } else {
                    nameServerList = getDefaultNameServer();
                }
            } catch (IOException e) {
                LOGGER.error("IO error while loading configuration file", e);
            }
        } else {
            nameServerList = getDefaultNameServer();
        }
    }

    private String getDefaultNameServer() {
        String publicIP = RemotingUtil.getLocalAddress(true);
        return publicIP + ":9876";
    }

    public ByteBuf getNameServerAddressList() {
        List<WatchEvent<?>> watchEvents = watchKey.pollEvents();
        if (null != watchEvents && !watchEvents.isEmpty()) {
            for (WatchEvent<?> watchEvent : watchEvents) {
                Path path = (Path) watchEvent.context();
                if (configuration.equals(path.toFile().getName())) {
                    refreshConfigurationFile();
                }
            }
        }
        return Unpooled.wrappedBuffer(nameServerList.getBytes());
    }
}
