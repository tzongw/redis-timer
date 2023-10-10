#include <string.h>
#include <strings.h>
#include <stdbool.h>

#define REDISMODULE_EXPERIMENTAL_API
#include "redismodule.h"


/* structure with timer information */
typedef struct TimerData {
    RedisModuleString *key;        /* timer key */
    RedisModuleString *function;    /* function for the timer to execute */
    mstime_t interval;          /* interval */
    int datalen;   /* data length */
    int numkeys;     /* function numkeys */
    bool loop;                   /* loop timer */
    bool deleted;              /* timer key been deleted from db */
    int dbid;       /* key's dbid, -1 if timer's dbid is the same with key's */
    RedisModuleTimerID tid;     /* internal id for the timer API */
    RedisModuleString *data[];  /* function keys & args */
} TimerData;

static RedisModuleType *moduleType;
static long long timers = 0;
static bool isMaster = true;

static const int MODULE_VERSION = 1;
static const int ENCODE_VERSION = 1;


void roleChangeCallback(RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data)
{
    RedisModule_AutoMemory(ctx);
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
    timers--;
}

/* callback called by the Timer API. Data contains a TimerData structure */
void TimerCallback(RedisModuleCtx *ctx, void *data) {
    RedisModule_AutoMemory(ctx);
    TimerData *td = (TimerData*)data;
    bool delete_td = false;

    if (td->dbid != -1) {
        RedisModule_SelectDb(ctx, td->dbid);
    }
    RedisModule_KeyExists(ctx, td->key);  // actively expire key
    if (td->deleted) { /* already deleted from db, clear it */
        DeleteTimerData(ctx, td);
        return;
    }
    /* if loop, create a new timer and reinsert
     * if not, delete the timer data
     */
    if (td->loop) {
        td->tid = RedisModule_CreateTimer(ctx, td->interval, TimerCallback, td);
        td->dbid = -1; /* no need to select db again next time */
    } else {
        // replica also delete timer data, there is a race condition between replica timer firing
        // and receiving master's 'timer.kill' action
        RedisModuleKey *mk = RedisModule_OpenKey(ctx, td->key, REDISMODULE_WRITE);
        RedisModule_DeleteKey(mk);
        RedisModule_Replicate(ctx, "timer.kill", "s", td->key);
        RedisModule_Assert(td->deleted);
        // will delete `td` after function execution
        delete_td = true;
    }
    // execution at last to avoid function making `td` invalid (e.g. timer.kill `key` in function)
    // also make interval more reliable for loop timer with slow function
    if (isMaster) {
        // if master, execute the script, replica will copy master's actions
        RedisModule_Call(ctx, "FCALL", "!slv", td->function, (long long)td->numkeys, td->data, (size_t)td->datalen);
    }
    if (delete_td) {
        // key was removed from db, so function has no way to invalidate `td`
        DeleteTimerData(ctx, td);
    }
}


int keyEventsCallback(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key) {
    RedisModule_AutoMemory(ctx);
    REDISMODULE_NOT_USED(type);
    if (strcasecmp(event, "rename_to") == 0) {
        RedisModuleKey *mk = RedisModule_OpenKey(ctx, key, REDISMODULE_READ);
        if (RedisModule_ModuleTypeGetType(mk) == moduleType) {
            TimerData *td = RedisModule_ModuleTypeGetValue(mk);
            RedisModule_FreeString(ctx, td->key);
            RedisModule_RetainString(ctx, key);
            td->key = key;
        }
    } else if (strcasecmp(event, "move_to") == 0) {
        RedisModuleKey *mk = RedisModule_OpenKey(ctx, key, REDISMODULE_READ);
        if (RedisModule_ModuleTypeGetType(mk) == moduleType) {
            TimerData *td = RedisModule_ModuleTypeGetValue(mk);
            uint64_t remaining = td->interval;
            RedisModule_GetTimerInfo(ctx, td->tid, &remaining, NULL);
            RedisModule_StopTimer(ctx, td->tid, NULL);
            td->tid = RedisModule_CreateTimer(ctx, remaining, TimerCallback, td);
            td->dbid = -1;
        }
    }
    return REDISMODULE_OK;
}

/* Entrypoint for TIMER.NEW command.
 * This command creates a new timer.
 * Syntax: TIMER.NEW key function interval [LOOP] numkeys [key [key ...]] [arg [arg ...]]
 * If LOOP is specified, after executing a new timer is created
 * Return 1 if new timer created, 0 if replace old timer
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
    if (datalen < numkeys) {
        return RedisModule_WrongArity(ctx);
    }
    
    /* allocate structure and init */
    td = (TimerData*)RedisModule_Alloc(sizeof(*td)+sizeof(RedisModuleString*)*datalen);
    timers++;
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
    td->numkeys = (int)numkeys;

    /* create the timer through the Timer API */
    td->tid = RedisModule_CreateTimer(ctx, interval, TimerCallback, td);
    td->dbid = -1; /* timer' dbid is always the same with key's in this way */
    td->deleted = false;
    
    RedisModuleKey *mk = RedisModule_OpenKey(ctx, key, REDISMODULE_WRITE); /* auto closed */
    if (RedisModule_ModuleTypeGetType(mk) == moduleType) {
        old = RedisModule_ModuleTypeGetValue(mk);  // reset timer
    }
    RedisModule_ModuleTypeSetValue(mk, moduleType, td);
    if (old) {  // clear asap
        RedisModule_Assert(old->deleted);
        RedisModule_StopTimer(ctx, old->tid, NULL);
        DeleteTimerData(ctx, old);
    }
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithLongLong(ctx, old ? 0 : 1);
    return REDISMODULE_OK;
}


/* Syntax: TIMER.KILL key
*  Return 1 if a timer been kill, else 0
*  More effective than DEL key, will clear all the resources
*/
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
    RedisModule_Assert(td->deleted);
    RedisModule_StopTimer(ctx, td->tid, NULL);
    DeleteTimerData(ctx, td);
    RedisModule_ReplicateVerbatim(ctx);
    return RedisModule_ReplyWithLongLong(ctx, 1);
}

/* Syntax: TIMER.INFO key
*  Return timer info, remaining is the next fire time interval
*/
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
        const char *fmt = i < td->numkeys ? "key%d" : "arg%d";
        int index = i<td->numkeys ? i : i-td->numkeys;
        RedisModuleString *name = RedisModule_CreateStringPrintf(ctx, fmt, index+1);
        RedisModule_ReplyWithString(ctx, name);
        RedisModule_ReplyWithString(ctx, td->data[i]);
    }
    return REDISMODULE_OK;
}

void *timer_RDBLoadCallBack(RedisModuleIO *io, int encver) {
    if (encver != ENCODE_VERSION) {
        RedisModule_LogIOError(io, "warning", "decode failed, rdb ver: %d, my ver: %d", encver, ENCODE_VERSION);
        return NULL;
    }
    RedisModuleCtx *ctx = RedisModule_GetContextFromIO(io);
    RedisModule_AutoMemory(ctx);
    int datalen = (int)RedisModule_LoadSigned(io);
    TimerData *td = (TimerData*)RedisModule_Alloc(sizeof(*td)+sizeof(RedisModuleString*)*datalen);
    timers++;
    td->datalen = datalen;
    for (int i = 0; i < td->datalen; i++) {
        td->data[i] = RedisModule_LoadString(io);
    }
    td->key = RedisModule_LoadString(io);
    td->function = RedisModule_LoadString(io);
    td->numkeys = (int)RedisModule_LoadSigned(io);
    td->interval = RedisModule_LoadSigned(io);
    td->loop = RedisModule_LoadSigned(io) == 1;
    td->deleted = false;
    td->tid = RedisModule_CreateTimer(ctx, td->interval, TimerCallback, td);
    /* see https://github.com/redis/redis/pull/11361 */
    td->dbid = RedisModule_GetDbIdFromIO(io);
    return td;
}

void timer_RDBSaveCallBack(RedisModuleIO *io, void *value) {
    RedisModuleCtx *ctx = RedisModule_GetContextFromIO(io);
    RedisModule_AutoMemory(ctx);
    TimerData *td = value;
    RedisModule_SaveSigned(io, td->datalen);
    for (int i = 0; i < td->datalen; i++) {
        RedisModule_SaveString(io, td->data[i]);
    }
    RedisModule_SaveString(io, td->key);
    RedisModule_SaveString(io, td->function);
    RedisModule_SaveSigned(io, td->numkeys);
    uint64_t interval = td->interval;
    if (!td->loop) {
        RedisModule_GetTimerInfo(ctx, td->tid, &interval, NULL);
    }
    RedisModule_SaveSigned(io, interval);
    RedisModule_SaveSigned(io, td->loop ? 1 : 0);
}

void timer_AOFRewriteCallBack(RedisModuleIO *io, RedisModuleString *key, void *value) {
    REDISMODULE_NOT_USED(key);
    RedisModuleCtx *ctx = RedisModule_GetContextFromIO(io);
    RedisModule_AutoMemory(ctx);
    TimerData *td = value;
    if (td->loop) {
        RedisModule_EmitAOF(io, "timer.new", "sslclv", td->key, td->function, td->interval, "LOOP", (long long)td->numkeys, td->data, (size_t)td->datalen);
    } else {
        uint64_t remaining = td->interval;
        RedisModule_GetTimerInfo(ctx, td->tid, &remaining, NULL);
        RedisModule_EmitAOF(io, "timer.new", "ssllv", td->key, td->function, remaining, (long long)td->numkeys, td->data, (size_t)td->datalen);
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
    if (RedisModule_Init(ctx, "timer", MODULE_VERSION, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    RedisModule_AutoMemory(ctx);
    /* register commands */
    if (RedisModule_CreateCommand(ctx, "timer.new", TimerNewCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "timer.kill", TimerKillCommand, "write fast", 1, 1, 1) == REDISMODULE_ERR) {
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
    moduleType = RedisModule_CreateDataType(ctx, "timer-tzw", ENCODE_VERSION, &tm);
    if (moduleType == NULL) {
        return REDISMODULE_ERR;
    }
    
    RedisModule_SubscribeToServerEvent(ctx,
            RedisModuleEvent_ReplicationRoleChanged, roleChangeCallback);
    isMaster = RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_MASTER;
    RedisModule_Log(ctx, "notice", "role: %s", isMaster ? "master": "slave");
    
    RedisModule_SubscribeToKeyspaceEvents(ctx, REDISMODULE_NOTIFY_GENERIC, keyEventsCallback);
    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx) {
    RedisModule_AutoMemory(ctx);
    // can't unload if have running timers
    return timers>0 ? REDISMODULE_ERR : REDISMODULE_OK;
}
