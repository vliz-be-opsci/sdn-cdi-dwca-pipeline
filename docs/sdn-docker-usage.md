## start docker stack

```
$ docker compose up -d
```

prefered to use `-d` detached mode so the system keeps running if you quit the cli env


## stop docker stack

```
$ docker compose down
```

## check running services

```
$ docker compose ps
```

Note:
* specify a format to get the running service names only:

```
$ docker compose ps --format '{{.Name}}'
```


## see running logs

```
$ docker compose logs -f [SERVICE_NAME]
```

Note:
* `-f` keeps the logs comming (like cat -f) 
* press ctrl+C to quit
* optionally one can add the service-name in the command too to only show logs from that one service


## step into a service to inspect

this launches a bash interpreter associated to the environment of a specific service name

```
$ docker exec -it SERVICE_NAME /bin/bash
```

Note: 
* no `compose` part in this case on the cli command
* Terminate the bash with ctrl-d -- this will end the process and return to the outer shell

You can use this technique to execute one-off commands into these environments too.

Some examples:

```
# verify the DB location in use by the sql-viewer resp the scheduler
$ docker exec -it sql-viewer /bin/sh -c "ls -al \${SQLITE_DATABASE}"
$ docker exec -it sched-trigger /bin/sh -c "ls -al \${SQLITE_DATABASE}"


```




