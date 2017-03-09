package com.alibaba.rocketmq.example.verify;

import com.alibaba.rocketmq.common.UtilAll;
import org.apache.commons.cli.*;

public class SelectPartition {
    public static void main(String[] args) throws ParseException {
        Options options = new Options();

        Option option = new Option("p", "path", true, "Paths in CSV");
        options.addOption(option);

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);
        if (!commandLine.hasOption('p')) {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("Select Partition", options);
            return;
        }

        String pathCSV = commandLine.getOptionValue("p");
        String selectedPath = UtilAll.selectPath(pathCSV);
        System.out.println(selectedPath);
    }
}
