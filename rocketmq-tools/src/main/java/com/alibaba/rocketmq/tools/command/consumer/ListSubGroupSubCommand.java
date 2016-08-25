/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.tools.command.consumer;

import com.alibaba.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.srvutil.ServerUtil;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.Map;


/**
 * List subscription groups of a broker.
 */
public class ListSubGroupSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "listSubGroup";
    }


    @Override
    public String commandDesc() {
        return "List subscription groups";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddress", true, "list subscription groups of which broker");
        opt.setRequired(true);
        options.addOption(opt);
        return options;
    }


    @Override
    public void execute(final CommandLine commandLine, final Options options, RPCHook rpcHook) {
        final DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {

            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();

                defaultMQAdminExt.start();
                SubscriptionGroupWrapper subscriptionGroupWrapper = defaultMQAdminExt.fetchAllSubscriptionGroups(addr, 3000);
                String format = "%-80s  %-12s  %-12s\n";
                System.out.printf(format, "Consumer Group", "Enabled", "Broadcast");
                for (Map.Entry<String, SubscriptionGroupConfig> entry :
                        subscriptionGroupWrapper.getSubscriptionGroupTable().entrySet()) {
                    SubscriptionGroupConfig subscriptionGroupConfig = entry.getValue();
                    String groupName = subscriptionGroupConfig.getGroupName();
                    System.out.printf(format,
                            groupName.length() > 80 ? groupName.substring(0, 80) : groupName,
                            subscriptionGroupConfig.isConsumeEnable(),
                            subscriptionGroupConfig.isConsumeBroadcastEnable());
                }
                return;
            } else {
                System.err.println("Fatal error! Required broker address is absent!");
            }
            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
