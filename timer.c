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

/* structure with timer information */
typedef struct TimerData {
    RedisModuleString *key;        /* user timer id */
    RedisModuleString *function;    /* function for the script to execute */
    RedisModuleString *data[MAX_DATA_LEN];
    long long datalen;
    long long numkeys;
    mstime_t interval;          /* interval */
    bool loop;                   /* loop timer */
    bool deleted;              /* timer been deleted */
    RedisModuleTimerID tid;     /* internal id for the timer API */
} TimerData;

static char fmt[2+MAX_DATA_LEN+1] = "slssssssss";
static RedisModuleType *moduleType;
 
static bool isMaster = true;


void roleChangeCallback(RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data)
{
    REDISMODULE_NOT_USED(e);
    REDISMODULE_NOT_USED(data);
    isMaster = (sub == REDISMODULE_EVENT_REPLROLECHANGED_NOW_MASTER);
    RedisModule_Log(ctx, "notice", "role change: %s", isMaster ? "master": "slave");
}

/* release all the memory used in timer structure */
void DeleteTimerData(RedisModuleCtx *ctx, TimerData *td) {
    RedisModule_FreeString(ctx, td->key);
    RedisModule_FreeString(ctx, td->function);
    for (int i = 0; i < td->datalen; i++) {
        RedisModule_FreeString(ctx, td->data[i]);
    }
    RedisModule_Free(td);
}

/* callback called by the Timer API. Data contains a TimerData structure */
void TimerCallback(RedisModuleCtx *ctx, void *data) {
    RedisModule_AutoMemory(ctx);
    RedisModuleCallReply *rep;
    TimerData *td;

    td = (TimerData*)data;
    if (td->deleted) { /* already deleted, clear it */
        DeleteTimerData(ctx, td);
        return;
    }

    /* if master, execute the script */
    if (isMaster) {
        fmt[2+td->datalen] = '\0';
        rep = RedisModule_Call(ctx, "FCALL", fmt, td->function, td->numkeys,
                                td->data[0], td->data[1], td->data[2], td->data[3],
                                td->data[4], td->data[5], td->data[6], td->data[7]);
        RedisModule_FreeCallReply(rep);
        fmt[2+td->datalen] = 's';
    }

    /* if loop, create a new timer and reinsert
     * if not, delete the timer data
     */
    if (td->loop) {
        td->tid = RedisModule_CreateTimer(ctx, td->interval, TimerCallback, td);
    } else {
        RedisModuleKey *mk = RedisModule_OpenKey(ctx, td->key, REDISMODULE_WRITE);
        RedisModule_DeleteKey(mk);
        RedisModule_CloseKey(mk);
        RedisModule_Replicate(ctx, "DEL", "s", td->key);
        DeleteTimerData(ctx, td);
    }
}

/* Entrypoint for TIMER.NEW command.
 * This command creates a new timer.
 * Syntax: TIMER.NEW key function interval [LOOP] numkeys [key [key ...]] [arg [arg ...]]
 * If LOOP is specified, after executing a new timer is created
 */
int TimerNewCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    long long interval;
    long long numkeys;
    bool loop = false;
    int pos;
    int datalen;
    TimerData *td = NULL;
    TimerData *old = NULL;
    const char *s;
    RedisModuleString *key, *function;

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
    
    /* allocate structure and init */
    td = (TimerData*)RedisModule_Alloc(sizeof(*td));
    RedisModule_RetainString(ctx, key);
    td->key = key;
    RedisModule_RetainString(ctx, function);
    td->function = function;
    td->interval = interval;
    td->loop = loop;
    
    for (int i = 0; i < datalen; i++) {
        RedisModule_RetainString(ctx, argv[pos+i]);
        td->data[i] = argv[pos+i];
    }
    td->datalen = datalen;
    td->numkeys = numkeys;

    /* create the timer through the Timer API */
    td->tid = RedisModule_CreateTimer(ctx, interval, TimerCallback, td);
    td->deleted = false;
    
    RedisModuleKey *mk = RedisModule_OpenKey(ctx, key, REDISMODULE_WRITE);
    if (RedisModule_ModuleTypeGetType(mk) == moduleType) {
        old = RedisModule_ModuleTypeGetValue(mk);  // replace timer
    }
    RedisModule_ModuleTypeSetValue(mk, moduleType, td);
    RedisModule_CloseKey(mk);
    if (old) {
        RedisModule_StopTimer(ctx, old->tid, NULL);
        DeleteTimerData(ctx, old);
    }
    RedisModule_ReplicateVerbatim(ctx);
    
    RedisModule_ReplyWithLongLong(ctx, old ? 0 : 1);
    return REDISMODULE_OK;
}

int TimerKillCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }
    if (!RedisModule_KeyExists(ctx, argv[1])) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    }

    RedisModuleKey *mk = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE); /* auto closed */
    if (RedisModule_ModuleTypeGetType(mk) != moduleType) {
        return RedisModule_ReplyWithError(ctx, "ERR wrong type");
    }
    TimerData *td = RedisModule_ModuleTypeGetValue(mk);

    RedisModule_DeleteKey(mk);
    RedisModule_StopTimer(ctx, td->tid, NULL);
    DeleteTimerData(ctx, td);
    return RedisModule_ReplyWithLongLong(ctx, 1);
}

int TimerInfoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModuleKey *mk = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ); /* auto closed */
    if (RedisModule_ModuleTypeGetType(mk) != moduleType) {
        if (mk) {
            return RedisModule_ReplyWithError(ctx, "ERR wrong type");
        } else {
            return RedisModule_ReplyWithNull(ctx);
        }
    }
    TimerData *td = RedisModule_ModuleTypeGetValue(mk);
    uint64_t remaining = td->interval;
    RedisModule_GetTimerInfo(ctx, td->tid, &remaining, NULL);
    RedisModule_ReplyWithMap(ctx, 4+td->datalen);
    RedisModule_ReplyWithCString(ctx, "function");
    RedisModule_ReplyWithString(ctx, td->function);
    RedisModule_ReplyWithCString(ctx, "interval");
    RedisModule_ReplyWithLongLong(ctx, td->interval);
    RedisModule_ReplyWithCString(ctx, "remaining");
    RedisModule_ReplyWithLongLong(ctx, remaining);
    RedisModule_ReplyWithCString(ctx, "loop");
    RedisModule_ReplyWithBool(ctx, td->loop);
    for (int i = 0; i < td->datalen; i++) {
        static char keys[] = "key*";
        static char args[] = "arg*";
        char *name = i < td->numkeys ? keys : args;
        name[3] = "123456789"[i<td->numkeys ? i : i-td->numkeys];
        RedisModule_ReplyWithCString(ctx, name);
        RedisModule_ReplyWithString(ctx, td->data[i]);
    }
    return REDISMODULE_OK;
}

void *timer_RDBLoadCallBack(RedisModuleIO *io, int encver) {
    REDISMODULE_NOT_USED(encver);
    RedisModuleCtx *ctx = RedisModule_GetContextFromIO(io);
    RedisModule_AutoMemory(ctx);
    TimerData *td = (TimerData*)RedisModule_Alloc(sizeof(*td));
    td->key = RedisModule_LoadString(io);
    td->function = RedisModule_LoadString(io);
    td->datalen = RedisModule_LoadSigned(io);
    for (int i = 0; i < td->datalen; i++) {
        td->data[i] = RedisModule_LoadString(io);
    }
    td->numkeys = RedisModule_LoadSigned(io);
    td->interval = RedisModule_LoadSigned(io);
    td->loop = RedisModule_LoadSigned(io) == 1;
    td->deleted = false;
    td->tid = RedisModule_CreateTimer(ctx, td->interval, TimerCallback, td);
    return td;
}

void timer_RDBSaveCallBack(RedisModuleIO *io, void *value) {
    RedisModuleCtx *ctx = RedisModule_GetContextFromIO(io);
    RedisModule_AutoMemory(ctx);
    TimerData *td = value;
    RedisModule_SaveString(io, td->key);
    RedisModule_SaveString(io, td->function);
    RedisModule_SaveSigned(io, td->datalen);
    for (int i = 0; i < td->datalen; i++) {
        RedisModule_SaveString(io, td->data[i]);
    }
    RedisModule_SaveSigned(io, td->numkeys);
    uint64_t remaining = td->interval;
    if (!td->loop) {
        RedisModule_GetTimerInfo(ctx, td->tid, &remaining, NULL);
    }
    RedisModule_SaveSigned(io, remaining);
    RedisModule_SaveSigned(io, td->loop ? 1 : 0);
}

void timer_AOFRewriteCallBack(RedisModuleIO *io, RedisModuleString *key, void *value) {
    REDISMODULE_NOT_USED(key);
    RedisModuleCtx *ctx = RedisModule_GetContextFromIO(io);
    RedisModule_AutoMemory(ctx);
    TimerData *td = value;
    if (td->loop) {
        char fmt[5+MAX_DATA_LEN+1] = "sslclssssssss";
        fmt[5+td->datalen] = '\0';
        RedisModule_EmitAOF(io, "timer.new", fmt, td->key, td->function, td->interval, "LOOP", td->numkeys,
                            td->data[0], td->data[1], td->data[2], td->data[3],
                            td->data[4], td->data[5], td->data[6], td->data[7]);
    } else {
        char fmt[4+MAX_DATA_LEN+1] = "ssllssssssss";
        fmt[4+td->datalen] = '\0';
        uint64_t remaining = td->interval;
        RedisModule_GetTimerInfo(ctx, td->tid, &remaining, NULL);
        RedisModule_EmitAOF(io, "timer.new", fmt, td->key, td->function, remaining, td->numkeys,
                            td->data[0], td->data[1], td->data[2], td->data[3],
                            td->data[4], td->data[5], td->data[6], td->data[7]);
    }
}

void timer_FreeCallBack(void *value) {
    TimerData *td = (TimerData *)value;
    td->deleted = true; /* we don't have ctx to call StopTimer, so mark it as deleted, will clear it in TimerCallback, sigh */
}

/* Module entrypoint */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    /* Register the module itself */
    if (RedisModule_Init(ctx, "timer", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    RedisModule_AutoMemory(ctx);
    /* register commands */
    if (RedisModule_CreateCommand(ctx, "timer.new", TimerNewCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "timer.kill", TimerKillCommand, "write", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "timer.info", TimerInfoCommand, "readonly fast", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    
    RedisModuleTypeMethods tm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = timer_RDBLoadCallBack,
        .rdb_save = timer_RDBSaveCallBack,
        .aof_rewrite = timer_AOFRewriteCallBack,
        .free = timer_FreeCallBack,
    };
    moduleType = RedisModule_CreateDataType(ctx, "timer-tzw", 0, &tm);
    if (moduleType == NULL) {
        return REDISMODULE_ERR;
    }
    
    RedisModule_SubscribeToServerEvent(ctx,
            RedisModuleEvent_ReplicationRoleChanged, roleChangeCallback);
    RedisModuleServerInfoData *info = RedisModule_GetServerInfo(ctx, "replication");
    const char *role = RedisModule_ServerInfoGetFieldC(info, "role");
    isMaster = strcasecmp(role, "master") == 0;
    RedisModule_Log(ctx, "notice", "role: %s", isMaster ? "master": "slave");
    RedisModule_FreeServerInfo(ctx, info);
    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx) {
    RedisModule_AutoMemory(ctx);
    return REDISMODULE_ERR;
}
