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
    mstime_t interval;          /* looping interval. 0 if it is only once */
    RedisModuleTimerID tid;     /* internal id for the timer API */
} TimerData;


void TimerCallback(RedisModuleCtx *ctx, void *data);
void DeleteTimerData(TimerData *td);

/* internal structure for storing timers */
static RedisModuleDict *timers;

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
    rep = RedisModule_Call(ctx, "EVALSHA", "sls", td->sha1, 0, td->data);
    RedisModule_FreeCallReply(rep);

    /* if loop, create a new timer and reinsert
     * if not, delete the timer data
     */
    if (td->interval) {
        td->tid = RedisModule_CreateTimer(ctx, td->interval, TimerCallback, td);
    } else {
        RedisModule_DictDel(timers, td->key, NULL);
        DeleteTimerData(td);
    }
}

/* Entrypoint for TIMER.NEW command.
 * This command creates a new timer.
 * Syntax: TIMER.NEW key data sha1 interval [LOOP]
 * If LOOP is specified, after executing a new timer is created
 */
int TimerNewCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    long long interval;
    bool loop = false;
    TimerData *td = NULL;
    const char *s;

    // check arguments
    if (argc < 5 || argc > 6) {
        return RedisModule_WrongArity(ctx);
    }

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
    
    if (RedisModule_DictDel(timers, argv[1], &td) == REDISMODULE_OK) {
        RedisModule_StopTimer(ctx, td->tid, NULL);
        DeleteTimerData(td);
        td = NULL;
    }

    /* allocate structure and init */
    td = (TimerData*)RedisModule_Alloc(sizeof(*td));
    td->key = RedisModule_CreateStringFromString(NULL, argv[1]);
    td->data = RedisModule_CreateStringFromString(NULL, argv[2]);
    td->sha1 = RedisModule_CreateStringFromString(NULL, argv[3]);
    td->interval = loop ? interval : 0;

    /* create the timer through the Timer API */
    td->tid = RedisModule_CreateTimer(ctx, interval, TimerCallback, td);

    /* add the timer to the list of timers */
    RedisModule_DictSet(timers, td->key, td);
    
    RedisModule_ReplyWithSimpleString(ctx, "OK");
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
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
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
    return REDISMODULE_OK;
}
