# Open edX Data Pipeline

A data pipeline for analyzing Open edX data. This is a batch analysis engine that is capable of running complex data processing workflows.

The data pipeline takes large amounts of raw data, analyzes it and produces higher value outputs that are used by various downstream tools.

The primary consumer of this data is [Open edX Insights](http://edx.readthedocs.io/projects/edx-insights/en/latest/).

It is also used to generate a variety of packaged outputs for research, business intelligence and other reporting.

It gathers input from a variety of sources including (but not limited to):

* [Tracking log](http://edx.readthedocs.io/projects/devdata/en/latest/internal_data_formats/event_list.html) files - This is the primary data source.
* LMS database
* Otto database
* LMS APIs (course blocks, course listings)

It outputs to:

* S3 - CSV reports, packaged exports
* MySQL - This is known as the "result store" and is consumed by Insights
* Elasticsearch - This is also used by Insights
* Vertica - This is used for business intelligence and reporting purposes

This tool uses [spotify/luigi](https://github.com/spotify/luigi) as the core of the workflow engine.

Data transformation and analysis is performed with the assistance of the following third party tools (among others):

* Python
* [Pandas](http://pandas.pydata.org/)
* [Hive](https://hive.apache.org/)
* [Hadoop](http://hadoop.apache.org/)
* [Sqoop](http://sqoop.apache.org/)

The data pipeline is designed to be invoked on a periodic basis by an external scheduler. This can be cron, jenkins or any other system that can periodically run shell commands.

Here is a simplified, high level, view of the architecture:

![Open edX Analytics Architectural Overview](/images/architecture.png?raw=true)

## Setting up a Development Environment

We call this environment the "analyticstack". We are somewhat early adopters of [OEP-5](http://open-edx-proposals.readthedocs.io/en/latest/oep-0005.html). Our development environment consists mostly of Docker containers, however, some services are still run in a Vagrant virtual machine.

Here is a diagram showing how the components are related and connected to one another:

![the analyticstack](/images/analyticstack.png?raw=true)

### Network configuration

The network configuration is they most complicated part of the system. Unfortunately, not all edX components are well supported in docker (yet), so we have to connect the docker containers with the Vagrant VM used by devstack. We do this by forwarding the necessary ports to the loopback interface on the host machine and using that network to communicate. This means that each service must have a globally unique port. It also means that your firewall must be configured to allow these types of connections to be made. The docker containers and the vagrant VM both have the ability to connect to the host machine even though they can't directly connect to each other. To facilitate these connections we map the domain `open.edx` to this shared network in each environment. In the docker case the host's IP address is fixed and configured in the docker-compose.yml file, so we statically map that IP address to the `open.edx` domain name. In the vagrant case, we manually edit the `/etc/hosts` file to map the domain to the appropriate host IP address. Finally, on the host machine, we simply add an entry to `/etc/hosts` pointing the domain to the loopback interface.

Here is a mapping of all of the addresses and ports needed to run the docker services alongside the Vagrant services:

| Service | Internal Hostname | Environment | URL | Why is it exposed? |
| --- | --- | --- | --- | --- |
| LMS | N/A | vagrant | http://open.edx:8000 | Allows you to interact with the system and generate new data in the tracking log. |
| LMS Database | N/A | vagrant | mysql://open.edx:3307 | Allows the data pipeline to connect directly to the LMS database and read data out of it. |
| Insights | insights | docker | http://open.edx:8110 | For visualizing results of computations from the data pipeline. Note that it *must* be accessed using the `open.edx` domain in order for SSO to work with the LMS. |
| WebHDFS Namenode | namenode | docker | http://open.edx:50070 | Allows the LMS to upload tracking log files directly into HDFS. |
| WebHDFS Datanode | datanode | docker | http://open.edx:50075 | Allows the LMS to upload tracking log files directly into HDFS. |

Here are the other ports that are exposed on `open.edx` to enable simple access to various web UIs:

| Service | Internal Hostname | Environment | URL | Why is it exposed? |
| --- | --- | --- | --- | --- |
| Luigi Central Scheduler | luigid | docker | http://open.edx:9100 | Allows you to see what tasks are running and visualize dependency graphs. |
| Analytics API | analyticsapi | docker | http://open.edx:8100 | Allows you to interact with the API for debugging purposes. See available endpoints etc. |
| Resource Manager | resourcemanager | docker | http://open.edx:8088 | The Hadoop resource manager web UI. |
| Node Manager | nodemanager | docker | http://open.edx:8042 | The Hadoop node manager web UI. Useful for diagnosing problems with Hadoop jobs. |
| Node Manager Job History Server | nodemanager | docker | http://open.edx:19888 | Continues to provide information on hadoop jobs that ran a while ago. |

### Setting up a development environment

* Ensure you have a recent version of Docker and Docker Compose. This setup was tested on Docker Compose 1.9.0 and Docker 1.12.3 running on Ubuntu 16.04.
* If you are using a firewall (hint: you should be)
    * Ensure that docker containers with IP addresses in the CIDR 172.19.0.0/24 can connect to ports 8000 and 3307 on your host
        * For example, if you use `ufw`:

            ```
            sudo ufw allow from 172.19.0.0/24 to any port 8000
            sudo ufw allow from 172.19.0.0/24 to any port 3307
            ```

* Download the Vagrantfile for a normal edX devstack.
* Make the following edits to the Vagrantfile:
    * Expose port guest port 3306 on host port 3307 by adding the line `config.vm.network :forwarded_port, guest: 3306, host: 3307  # MySQL database` in the section that configures all of the forwarded ports.
    * Remove the following forwarded ports (if they are in there)

        ```
        config.vm.network :forwarded_port, guest: 8100, host: 8100  # Analytics Data API
        config.vm.network :forwarded_port, guest: 8110, host: 8110  # Insights
        ```

* In order to mount folders using NFS in the guest OS with a Ubuntu 16.04 host, you will need to update your firewall rules to allow NFS traffic from the vagrant machine.
    * Modify `/run/sysconfig/nfs-utils` to replace `RPCMOUNTDARGS="--manage-gids"` with `RPCMOUNTDARGS="-p 11026"` and then run:

        ```
        sudo ufw allow from 192.168.33.0/24 to any port 111
        sudo ufw allow from 192.168.33.0/24 to any port 2049
        sudo ufw allow from 192.168.33.0/24 to any port 11026
        ```

* On the host machine add the following entries to your `/etc/hosts` file. The first line allows you to seamlessly interact with Vagrant and Docker based services. The next few lines allow you to easily navigate links generated by the various Hadoop Web UIs.

```
127.0.0.1	open.edx
127.0.0.1	namenode
127.0.0.1	datanode
127.0.0.1	resourcemanager
127.0.0.1	nodemanager
```

* Start the devstack by running `vagrant up`
* Login to the devstack by running `vagrant ssh`.
    * Open up remote access to your mysql database on the vagrant host by modifying `/etc/mysql/my.cnf`. Change `"bind-address    = 127.0.0.1"` to `"bind-address    = 0.0.0.0"`.
    * Run the following commands:

        ```
        export HOST_IP=$(ip route show 0.0.0.0/0 | grep -Eo 'via \S+' | awk '{ print $2 }' | head -n 1)
        sudo /bin/bash -c "echo \"$HOST_IP open.edx\" >> /etc/hosts"
        mysql --user=root --password="" --execute "GRANT ALL PRIVILEGES ON *.* TO 'root'@'$HOST_IP' IDENTIFIED BY '' WITH GRANT OPTION; FLUSH PRIVILEGES;"
        sudo service mysql restart
        ```

    * Change users to the `edxapp` user by running `sudo su edxapp`, and then run:

        ```
        /edx/bin/python.edxapp /edx/bin/manage.edxapp lms --settings=aws create_oauth2_client http://open.edx:8110 \
            "http://open.edx:8110/complete/edx-oidc/" \
            confidential \
            --client_name insights \
            --client_id YOUR_OAUTH2_KEY \
            --client_secret secret \
            --trusted \
            --logout_uri http://open.edx:8110/accounts/logout/
        ```

    * Exit the sub-shell to return to the `vagrant` user's shell.
    * Create a file called `/edx/bin/sync-tracking-logs.sh` with the following content:

        ```bash
        #!/bin/bash

        cd /edx/var/log/tracking

        NAMENODE_HOST="namenode:8020"
        USER="root"

        for log_file_path in *
        do
            echo -n "Syncing $log_file_path... "
            curl -XPUT -T $log_file_path "http://open.edx:50075/webhdfs/v1/data/$log_file_path?op=CREATE&user.name=$USER&namenoderpcaddress=$NAMENODE_HOST&overwrite=true"
            echo "done"
        done
        ```

    * Create a cron job to execute the above script every 5 minutes:

        ```
        sudo chmod a+x /edx/bin/sync-tracking-logs.sh
        (sudo crontab -l ; echo "* * * * * /edx/bin/sync-tracking-logs.sh >/dev/null 2>&1") | sudo crontab -
        ```

* On you host, navigate to the directory where this README is checked out and run `make dev.watch` to start all analytics related services.

### Running unit tests

* To run unit tests and quality checks execute the command `make dev.coverage-local`.

### Running acceptance tests

* To run acceptance tests execute the command `make dev.test-acceptance`.
* If you want to run a subset of the test suite you can define the `ONLY_TESTS` environment variable: `ONLY_TESTS=edx.analytics.tasks.tests.acceptance.test_enrollments make dev.test-acceptance`.

### Using pycharm

* Ensure you are running pycharm version 2016.3.1 or later
* Ensure dockerd is bound to `127.0.0.1:2376`
    * On Ubuntu 16.04 you can do this by creating a file called `/etc/systemd/system/docker.service.d/exposetcp.conf` with the following content:

        ```
        [Service]
        EnvironmentFile=-/etc/default/docker
        ExecStart=
        ExecStart=/usr/bin/dockerd -H fd:// -H 127.0.0.1:2376 $DOCKER_OPTS
        ```

* Add a new remote interpreter in pycharm. Choose the "Docker Compose" interpreter type.
* Add a new docker server and point it at port 2376 on your local machine
* Add the docker-compose.yml file as a configuration file
* Select the "analyticspipeline" service
* Set the python interpreter location to "/edx/app/analytics_pipeline/venvs/analytics_pipeline/bin/python"
* Click "OK"


# Running In Production

For small installations, you may want to use our [single instance installation guide](https://openedx.atlassian.net/wiki/display/OpenOPS/edX+Analytics+Installation).

For larger installations, we do not have a similarly detailed guide, you can start with our [installation guide](http://edx.readthedocs.io/projects/edx-installing-configuring-and-running/en/latest/insights/index.html).


# How to Contribute

Contributions are very welcome, but for legal reasons, you must submit a signed
[individual contributor's agreement](http://code.edx.org/individual-contributor-agreement.pdf)
before we can accept your contribution. See our
[CONTRIBUTING](https://github.com/edx/edx-platform/blob/master/CONTRIBUTING.rst)
file for more information -- it also contains guidelines for how to maintain
high code quality, which will make your contribution more likely to be accepted.
