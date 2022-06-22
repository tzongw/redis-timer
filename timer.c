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


void TimerCallback(RedisModuleCtx *ctx, void *data);
void DeleteTimerData(TimerData *td);

static int serv = -1;  /* used to wake up redis(fixed at 6.0) */
static const char ping[] = "PING\r\n";
static char pong[1024];
static const ssize_t pingSize = sizeof(ping)-1; // exclude '\0'
static ssize_t pingOffset = 0;
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
    if (td->deleted) { /* already deleted, clear it */
        DeleteTimerData(td);
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
    td->deleted = false;
    
    RedisModuleKey *mk = RedisModule_OpenKey(ctx, key, REDISMODULE_WRITE);
    if (RedisModule_ModuleTypeGetType(mk) == moduleType) {
        added = 0; // replace old timer
    }
    RedisModule_ModuleTypeSetValue(mk, moduleType, td);
    RedisModule_CloseKey(mk);
    RedisModule_ReplicateVerbatim(ctx);
    
    RedisModule_ReplyWithLongLong(ctx, added);
    return REDISMODULE_OK;
}

void *timer_RDBLoadCallBack(RedisModuleIO *io, int encver) {
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
    td->tid = RedisModule_CreateTimer(RedisModule_GetContextFromIO(io), td->interval, TimerCallback, td);
    return td;
}

void timer_RDBSaveCallBack(RedisModuleIO *io, void *value) {
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
        RedisModule_GetTimerInfo(RedisModule_GetContextFromIO(io), td->tid, &remaining, NULL);
    }
    RedisModule_SaveSigned(io, remaining);
    RedisModule_SaveSigned(io, td->loop ? 1 : 0);
}

void timer_AOFRewriteCallBack(RedisModuleIO *io, RedisModuleString *key, void *value) {
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
        RedisModule_GetTimerInfo(RedisModule_GetContextFromIO(io), td->tid, &remaining, NULL);
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
    /* Register the module itself */
    if (RedisModule_Init(ctx, "timer", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    /* register commands */
    if (RedisModule_CreateCommand(ctx, "timer.new", TimerNewCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR) {
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
    return REDISMODULE_ERR;
}
