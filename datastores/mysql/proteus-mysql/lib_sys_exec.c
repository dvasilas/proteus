#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <mysql.h>
#include <stdlib.h>
#include <ctype.h>

bool sys_exec_init( UDF_INIT *initid, UDF_ARGS *args, char *message);

void sys_exec_deinit( UDF_INIT *initid);

my_ulonglong sys_exec( UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);

bool sys_exec_init( UDF_INIT *initid, UDF_ARGS *args, char *message){
        unsigned int i=0;
        if(args->arg_count == 1
        && args->arg_type[i]==STRING_RESULT){
                return 0;
        } else {
                strcpy(
                        message
                ,       "Expected exactly one string type parameter"
                );
                return 1;
        }
}

void sys_exec_deinit( UDF_INIT *initid){ }

my_ulonglong sys_exec( UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error){
        return system(args->args[0]);
}