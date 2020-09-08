#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <mysql.h>
#include <ctype.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>

typedef unsigned long long ulonglong;
typedef long long longlong;

#ifndef LOCAL_ADDRESS
#define LOCAL_ADDRESS "127.0.0.1"
#endif
#ifndef SERVER_PORT
#define SERVER_PORT 2048
#endif
#ifndef SERVER_ADDRESS
#define SERVER_ADDRESS "127.0.0.1"
#endif

static int _server = -1;

bool MySQLNotification_init( UDF_INIT *initid, UDF_ARGS *args, char *message);

void MySQLNotification_deinit( UDF_INIT *initid);

// my_ulonglong sys_exec( UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);

bool MySQLNotification_init( UDF_INIT *initid, UDF_ARGS *args, char *message){
    struct sockaddr_in remote, saddr;

    // create a socket that will talk to our node server
    _server = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (_server == -1) {
       strcpy(message, "Failed to create socket");
       return 0;
    }

    // bind to local address
    memset(&saddr, 0, sizeof(saddr));
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(0);
    saddr.sin_addr.s_addr = inet_addr(LOCAL_ADDRESS);
    if (bind(_server, (struct sockaddr*)&saddr, sizeof(saddr)) != 0) {
        sprintf(message, "Failed to bind to %s", LOCAL_ADDRESS);
        return 0;
    }

    // connect to server
    memset(&remote, 0, sizeof(remote));
    remote.sin_family = AF_INET;
    remote.sin_port = htons(SERVER_PORT);
    remote.sin_addr.s_addr = inet_addr(SERVER_ADDRESS);
    if (connect(_server, (struct sockaddr*)&remote, sizeof(remote)) != 0) {
        sprintf(message, "Failed to connect to server %s:%d", SERVER_ADDRESS, SERVER_PORT);
        return 0;
    }

    return 0;
}

void MySQLNotification_deinit( UDF_INIT *initid){
    if (_server != -1) {
        close(_server);
    }
}

longlong MySQLNotification(UDF_INIT *initid,
                           UDF_ARGS *args,
                           char *is_null,
                           char *error) {
    char packet[512];

    if (strcmp((char *)args->args[0], "votes") == 0) {
      if (args->arg_count == 4) {
        sprintf(packet, "{\"table\":\"%s\", \"ts\":\"%s\", \"vote\":\"%d\", \"story_id\":\"%lld\"}",
          (char *)args->args[0],
          (char *)args->args[1],
          *((int*)args->args[2]),
          *((longlong*)args->args[3]));
      } else {
        sprintf(packet, "{\"table\":\"%s\", \"ts\":\"%s\", \"vote\":\"%d\", \"story_id\":\"%lld\", \"comment_id\":\"%lld\"}",
          (char *)args->args[0],
          (char *)args->args[1],
          *((int*)args->args[2]),
          *((longlong*)args->args[3]),
          *((longlong*)args->args[4]));
      }
    } else if (strcmp((char *)args->args[0], "stories")==0) {
      sprintf(packet, "{\"table\":\"%s\", \"ts\":\"%s\", \"story_id\":\"%lld\", \"user_id\":\"%lld\", \"title\":\"%s\", \"description\":\"%s\", \"short_id\":\"%s\"}",
          (char *)args->args[0],
          (char *)args->args[1],
          *((longlong*)args->args[2]),
          *((longlong*)args->args[3]),
          (char *)args->args[4],
          (char *)args->args[5],
          (char *)args->args[6]);

    } else if (strcmp((char *)args->args[0], "comments")==0) {
      sprintf(packet, "{\"table\":\"%s\", \"ts\":\"%s\", \"comment_id\":\"%lld\", \"user_id\":\"%lld\", \"story_id\":\"%lld\", \"comment\":\"%s\"}",
          (char *)args->args[0],
          (char *)args->args[1],
          *((longlong*)args->args[2]),
          *((longlong*)args->args[3]),
          *((longlong*)args->args[4]),
          (char *)args->args[5]);
    } else {
      return 0;
    }

    if (_server != -1) {
        send(_server, packet, strlen(packet), 0);
    }

    return 0;
}