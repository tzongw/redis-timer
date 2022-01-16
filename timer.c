#include <string.h>
#include <strings.h>
#include <stdbool.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/tcp.h>

#define REDISMODULE_EXPERIMENTAL_API
#include "redismodule.h"

/* structure with timer information */
typedef struct TimerData {
    RedisModuleString *key;        /* user timer id */
    RedisModuleString *data;     /* user timer data */
    RedisModuleString *sha1;    /* sha1 for the script to execute */
    mstime_t interval;          /* interval */
    bool loop;                   /* loop timer */
    RedisModuleTimerID tid;     /* internal id for the timer API */
} TimerData;


void TimerCallback(RedisModuleCtx *ctx, void *data);
void DeleteTimerData(TimerData *td);

/* internal structure for storing timers */
static RedisModuleDict *timers;
static int serv = -1;  /* used to wake up redis(fixed at 6.0) */
static const char ping[] = "PING\r\n";
static char pong[1024];
static const ssize_t pingSize = sizeof(ping)-1; // exclude '\0'
static ssize_t pingOffset = 0;
 

/* release all the memory used in timer structure */
void DeleteTimerData(TimerData *td) {
    RedisModule_FreeString(NULL, td->key);
    RedisModule_FreeString(NULL, td->data);
    RedisModule_FreeString(NULL, td->sha1);
    RedisModule_Free(td);
}

/* callback called by the Timer API. Data contains a TimerData structure */
void TimerCallback(RedisModuleCtx *ctx, void *data) {
    RedisModuleCallReply *rep;
    TimerData *td;

    td = (TimerData*)data;

    /* execute the script */
    rep = RedisModule_Call(ctx, "EVALSHA", "sls", td->sha1, 0L, td->data);
    RedisModule_FreeCallReply(rep);

    /* if loop, create a new timer and reinsert
     * if not, delete the timer data
     */
    if (td->loop) {
        td->tid = RedisModule_CreateTimer(ctx, td->interval, TimerCallback, td);
    } else {
        RedisModule_DictDel(timers, td->key, NULL);
        DeleteTimerData(td);
    }
    if (serv != -1) {
        ssize_t received = recv(serv, pong, sizeof(pong), 0);
        REDISMODULE_NOT_USED(received);
        ssize_t sent = send(serv, ping+pingOffset, pingSize-pingOffset, 0);
        if (sent > 0) {
            pingOffset = (pingOffset+sent) % pingSize;
        }
    }
}

/* Entrypoint for TIMER.NEW command.
 * This command creates a new timer.
 * Syntax: TIMER.NEW key data sha1 interval [LOOP]
 * If LOOP is specified, after executing a new timer is created
 */
int TimerNewCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    long long interval;
    long long added = 1; /* new or replace */
    bool loop = false;
    TimerData *td = NULL;
    const char *s;
    RedisModuleString *key, *data, *sha1;

    // check arguments
    if (argc < 5 || argc > 6) {
        return RedisModule_WrongArity(ctx);
    }
    key = argv[1];
    data = argv[2];
    sha1 = argv[3];
    if (RedisModule_StringToLongLong(argv[4], &interval) != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx, "ERR invalid interval");
    }

    if (argc == 6) {
        s = RedisModule_StringPtrLen(argv[5], NULL);
        if (strcasecmp(s, "LOOP")) {
            return RedisModule_ReplyWithError(ctx, "ERR invalid argument");
        }
        loop = true;
    }
    
    if (RedisModule_DictDel(timers, key, &td) == REDISMODULE_OK) {
        RedisModule_StopTimer(ctx, td->tid, NULL);
        DeleteTimerData(td);
        td = NULL;
        added = 0;
    }

    /* allocate structure and init */
    td = (TimerData*)RedisModule_Alloc(sizeof(*td));
    td->key = RedisModule_CreateStringFromString(NULL, key);
    td->data = RedisModule_CreateStringFromString(NULL, data);
    td->sha1 = RedisModule_CreateStringFromString(NULL, sha1);
    td->interval = interval;
    td->loop = loop;

    /* create the timer through the Timer API */
    td->tid = RedisModule_CreateTimer(ctx, interval, TimerCallback, td);

    /* add the timer to the list of timers */
    RedisModule_DictSet(timers, td->key, td);
    
    RedisModule_ReplyWithLongLong(ctx, added);
    return REDISMODULE_OK;
}

/* Entrypoint for TIMER.KILL command.
 * This command terminates existing timers.
 * Syntax: TIMER.KILL key [key ...]
 */
int TimerKillCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    TimerData *td;
    long long deleted = 0;

    /* check arguments */
    if (argc <= 1) {
        return RedisModule_WrongArity(ctx);
    }

    for (int i = 1; i < argc; i++) {
        if (RedisModule_DictDel(timers, argv[1], &td) == REDISMODULE_OK) {
            /* stop timer and free*/
            RedisModule_StopTimer(ctx, td->tid, NULL);
            DeleteTimerData(td);
            deleted++;
        }
    }
    RedisModule_ReplyWithLongLong(ctx, deleted);
    return REDISMODULE_OK;
}

/* Module entrypoint */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    /* Register the module itself */
    if (RedisModule_Init(ctx, "timer", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    /* register commands */
    if (RedisModule_CreateCommand(ctx, "timer.new", TimerNewCommand, "write deny-oom", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "timer.kill", TimerKillCommand, "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    
    /* initialize map */
    timers = RedisModule_CreateDict(NULL);
    
    if (argc <= 0) return REDISMODULE_OK;
    
    long long port;
    if (RedisModule_StringToLongLong(argv[0], &port) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    serv = socket(AF_INET, SOCK_STREAM, 0);
    int flags = fcntl(serv, F_GETFL);
    fcntl(serv, F_SETFL, flags | O_NONBLOCK);
    int flag = 1;
    setsockopt(serv, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    struct sockaddr_in    servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &servaddr.sin_addr);
    connect(serv, (struct sockaddr*)&servaddr, sizeof(servaddr));
    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx) {
    TimerData *td = NULL;
    RedisModuleDictIter *di = RedisModule_DictIteratorStartC(timers, "^", NULL, 0);
    while (RedisModule_DictNextC(di, NULL, (void**)&td)) {
        RedisModule_StopTimer(ctx, td->tid, NULL);
        DeleteTimerData(td);
    }
    RedisModule_DictIteratorStop(di);
    RedisModule_FreeDict(NULL, timers);
    if (serv != -1) close(serv);
    return REDISMODULE_OK;
}
