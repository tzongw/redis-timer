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

#define MAX_DATA_LEN 8
#define MAX_DATABASES 16

/* structure with timer information */
typedef struct TimerData {
    RedisModuleString *key;        /* user timer id */
    RedisModuleString *function;    /* function for the script to execute */
    RedisModuleString *data[MAX_DATA_LEN];
    int datalen;
    long long numkeys;
    mstime_t interval;          /* interval */
    bool loop;                   /* loop timer */
    RedisModuleTimerID tid;     /* internal id for the timer API */
} TimerData;


void TimerCallback(RedisModuleCtx *ctx, void *data);
void DeleteTimerData(TimerData *td);

/* internal structure for storing timers */
static RedisModuleDict *timers[MAX_DATABASES];
static int serv = -1;  /* used to wake up redis(fixed at 6.0) */
static const char ping[] = "PING\r\n";
static char pong[1024];
static const ssize_t pingSize = sizeof(ping)-1; // exclude '\0'
static ssize_t pingOffset = 0;
static char fmt[2+MAX_DATA_LEN+1] = "slssssssss";
 

/* release all the memory used in timer structure */
void DeleteTimerData(TimerData *td) {
    RedisModule_FreeString(NULL, td->key);
    RedisModule_FreeString(NULL, td->function);
    for (int i = 0; i < td->datalen; i++) {
        RedisModule_FreeString(NULL, td->data[i]);
    }
    RedisModule_Free(td);
}

/* callback called by the Timer API. Data contains a TimerData structure */
void TimerCallback(RedisModuleCtx *ctx, void *data) {
    RedisModuleCallReply *rep;
    TimerData *td;

    td = (TimerData*)data;
    int db = RedisModule_GetSelectedDb(ctx);

    /* execute the script */
    fmt[2+td->datalen] = '\0';
    rep = RedisModule_Call(ctx, "FCALL", fmt, td->function, td->numkeys,
                           td->data[0], td->data[1], td->data[2], td->data[3],
                           td->data[4], td->data[5], td->data[6], td->data[7]);
    RedisModule_FreeCallReply(rep);
    fmt[2+td->datalen] = 's';

    /* if loop, create a new timer and reinsert
     * if not, delete the timer data
     */
    if (td->loop) {
        td->tid = RedisModule_CreateTimer(ctx, td->interval, TimerCallback, td);
    } else {
        RedisModule_DictDel(timers[db], td->key, NULL);
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
 * Syntax: TIMER.NEW key function interval [LOOP] numkeys [key [key ...]] [arg [arg ...]]
 * If LOOP is specified, after executing a new timer is created
 */
int TimerNewCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    long long interval;
    long long added = 1; /* new or replace */
    long long numkeys;
    bool loop = false;
    int pos;
    int datalen;
    TimerData *td = NULL;
    const char *s;
    RedisModuleString *key, *function;
    int db = RedisModule_GetSelectedDb(ctx);

    if (db >= MAX_DATABASES) {
        return RedisModule_ReplyWithError(ctx, "ERR DB index is out of range");
    }

    if (argc < 5) {
        return RedisModule_WrongArity(ctx);
    }
    key = argv[1];
    function = argv[2];
    if (RedisModule_StringToLongLong(argv[3], &interval) != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx, "ERR invalid interval");
    }

    pos = 4;
    s = RedisModule_StringPtrLen(argv[pos], NULL);
    if (strcasecmp(s, "LOOP") == 0) {
        loop = true;
        pos++;
    }
    if (pos >= argc) {
        return RedisModule_WrongArity(ctx);
    }
    if (RedisModule_StringToLongLong(argv[pos], &numkeys) != REDISMODULE_OK || numkeys < 0) {
        return RedisModule_ReplyWithError(ctx, "ERR invalid numkeys");
    }
    pos++;
    datalen = argc - pos;
    if (datalen < numkeys || datalen > MAX_DATA_LEN) {
        return RedisModule_WrongArity(ctx);
    }
    
    if (RedisModule_DictDel(timers[db], key, &td) == REDISMODULE_OK) {
        RedisModule_StopTimer(ctx, td->tid, NULL);
        DeleteTimerData(td);
        td = NULL;
        added = 0;
    }

    /* allocate structure and init */
    td = (TimerData*)RedisModule_Alloc(sizeof(*td));
    RedisModule_RetainString(NULL, key);
    td->key = key;
    RedisModule_RetainString(NULL, function);
    td->function = function;
    td->interval = interval;
    td->loop = loop;
    
    for (int i = 0; i < datalen; i++) {
        RedisModule_RetainString(NULL, argv[pos+i]);
        td->data[i] = argv[pos+i];
    }
    td->datalen = datalen;
    td->numkeys = numkeys;

    /* create the timer through the Timer API */
    td->tid = RedisModule_CreateTimer(ctx, interval, TimerCallback, td);

    /* add the timer to the list of timers */
    RedisModule_DictSet(timers[db], td->key, td);
    
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
    int db = RedisModule_GetSelectedDb(ctx);

    if (db >= MAX_DATABASES) {
        return RedisModule_ReplyWithError(ctx, "ERR DB index is out of range");
    }

    /* check arguments */
    if (argc <= 1) {
        return RedisModule_WrongArity(ctx);
    }

    for (int i = 1; i < argc; i++) {
        if (RedisModule_DictDel(timers[db], argv[1], &td) == REDISMODULE_OK) {
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
    for (int db = 0; db < MAX_DATABASES; db++) {
        timers[db] = RedisModule_CreateDict(NULL);
    }
    
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
    for (int db = 0; db < MAX_DATABASES; db++) {
        RedisModuleDictIter *di = RedisModule_DictIteratorStartC(timers[db], "^", NULL, 0);
        while (RedisModule_DictNextC(di, NULL, (void**)&td)) {
            RedisModule_StopTimer(ctx, td->tid, NULL);
            DeleteTimerData(td);
        }
        RedisModule_DictIteratorStop(di);
        RedisModule_FreeDict(NULL, timers[db]);
    }
    if (serv != -1) close(serv);
    return REDISMODULE_OK;
}
