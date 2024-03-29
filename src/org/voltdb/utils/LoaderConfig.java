package org.voltdb.utils;
/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import org.voltdb.CLIConfig;

/**
 * Uses CLIConfig class to declaratively state command line options
 * with defaults and validation.
 */
public class LoaderConfig extends CLIConfig {

    @Option(desc = "The JDBC connection string for the source database")
    String jdbcurl = "jdbc:voltdb://192.168.200.135:21212";

    @Option(desc = "The class name of the JDBC driver. Place the driver JAR files in the lib directory.")
    String jdbcdriver = "org.voltdb.jdbc.Driver";

    @Option(desc = "Source database username")
    String jdbcuser = "";

    @Option(desc = "Source database password")
    String jdbcpassword = "";

    @Option(desc = "Comma separated list in the form of server[:port] for the target VoltDB cluster.")
    String volt_servers = "192.168.200.135";

    @Option(desc = "VoltDB user name")
    public String volt_user = "";

    @Option(desc = "VoltDB password")
    public String volt_password = "";

    @Option(desc = "Name of table to be loaded")
    public String tablename = "votes";

    @Option(desc = "Name of procedure for VoltDB insert (optional)")
    public String procname = "";

    @Option(desc = "JDBC fetch size. Give a reasonable number.Default is 1000.")
    public int fetchsize = 1000;

    @Option(desc = "Maximum queue size for the producer thread, the thread that reads data from Netezza, to stop pulling data temporarily allowing consumer thread to catchup. Reasonably higher number.Default is 20.")
    public int maxQueueSize = 20;

    @Option(desc = "Path to the properties files which specifies queries to be executed against the source database against the VoltDB procedure names")
    public String queriesFile = "";

    @Option(desc = "Maximum time consumer can wait without any new data in seconds")
    public long maxWaitTime = 10;

    @Option(desc = "Maximum number of errors before loading should be stopped")
    public long maxErrors = 100;

    public LoaderConfig() {
    }

    public static LoaderConfig getConfig(String classname, String[] args) {
        LoaderConfig config = new LoaderConfig();
        config.parse(classname, args);
        return config;
    }

    @Override
    public void validate() {
        // add validation rules here
        if (volt_servers.equals("")) exitWithMessageAndUsage("volt_servers cannot be blank");
        if (tablename.equals("") && queriesFile.equals(""))
            exitWithMessageAndUsage("either tablename or queriesFile should be specified");
    }
}
