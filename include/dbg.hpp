#pragma once

    /*
     * Escape codes for controlling text color, brightness, etc.
     */

    #define TXT_CLRSCREEN           "\e[2J"
    #define TXT_NORMAL              "\e[0m"
    #define TXT_BRIGHT              "\e[1m"
    #define TXT_REVERSED            "\e[7m"
    #define TXT_FG_BLACK            "\e[30m"
    #define TXT_FG_RED              "\e[31m"
    #define TXT_FG_GREEN            "\e[32m"
    #define TXT_FG_YELLOW           "\e[33m"
    #define TXT_FG_BLUE             "\e[34m"
    #define TXT_FG_MAGENTA          "\e[35m"
    #define TXT_FG_CYAN             "\e[36m"
    #define TXT_FG_WHITE            "\e[37m"
    #define TXT_BG_BLACK            "\e[40m"
    #define TXT_BG_RED              "\e[41m"
    #define TXT_BG_GREEN            "\e[42m"
    #define TXT_BG_YELLOW           "\e[43m"
    #define TXT_BG_BLUE             "\e[44m"
    #define TXT_BG_MAGENTA          "\e[45m"
    #define TXT_BG_CYAN             "\e[46m"
    #define TXT_BG_WHITE            "\e[47m"

#ifdef VERBOSE

    #define DBG(_f, _a...)   do {                               \
        printf("DBG:(%s) "                                      \
                _f,  __func__, ## _a);                          \
    } while (0)

    #define DBG_ON(_g, _f, _a...) do {                          \
            if (_g) {                                           \
                DBG(_f, ## _a);                                 \
            }                                                   \
    } while (0)

    #define DBG_DUMP( _p, _len_p, _max_len) do {                \
        int _i, _j;                                             \
        unsigned long _len = _len_p;                            \
                                                                \
        if ( (_max_len) && (_len > _max_len)) {                 \
            _len = _max_len;                                    \
            printf("Buffer exceeds max length, dumping first %i bytes\n", _max_len);\
        }                                                                           \
                                                                                    \
        for ( _i = 0; _i < (_len); ) {                                              \
            for ( _j = 0; ( _j < 16) && (_i < (_len)); _j++, _i++ ) {               \
                printf("%02x ", (unsigned char)*((char *)(_p) + _i) );              \
            }                                                                       \
            printf("\n");                                                           \
        }                                                                           \
        printf("\n");                                                               \
    } while (0)

    #define DBG_DUMP_ON(_g, _p, _len_p, _max_len) do {          \
            if (_g) {                                           \
                DBG_DUMP(_p, _len_p, _max_len);                 \
            }                                                   \
    } while (0)

#else
    #define DBG(_f, _a...)       do { } while (0)
    #define DBG_ON(_g, _f, _a...)    do { } while (0)
#endif 


    #define WARN(_f, _a...)  do {                               \
        printf(TXT_FG_YELLOW "WARN:" TXT_FG_WHITE "(%s)"        \
                _f, __func__, ## _a);                           \
    } while (0)

    #define WARN_ON(_g, _f, _a...) do {                         \
            if (_g) {                                           \
                WARN(_f, ## _a);                                \
            }                                                   \
    } while (0)

    #define WARN_ONCE( _f, _a...)                               \
            ({                                                  \
            static int __warn_once = 1;                         \
            int __ret = 0;                                      \
                                                                \
            if (__warn_once) {                                  \
                __warn_once = 0;                                \
                WARN(_f, ## _a);                                \
                __ret = 1;                                      \
            }                                                   \
            __ret;                                              \
    })


    #define WARN_ON_ONCE(_g, _f, _a...)                         \
            ({                                                  \
            static int __warn_once = 1;                         \
            int __ret = 0;                                      \
                                                                \
            if (unlikely((_g) && __warn_once)) {                \
                __warn_once = 0;                                \
                WARN(_f, ## _a);                                \
                __ret = 1;                                      \
            }                                                   \
            __ret;                                              \
    })


    #define DO_ON(__condition, __action)                        \
    ({                                                          \
            int __ret = 0;                                      \
                                                                \
            if (unlikely(__condition)) {                        \
                (__action);                                     \
                __ret = 1                                       \
            }                                                   \
            __ret;                                              \
     })
    

    #define DO_ONCE(__action)                                   \
    ({                                                          \
            static int __warn_once = 1;                         \
            int __ret = 0;                                      \
                                                                \
            if (unlikely(__warn_once)) {                        \
                (__action);                                     \
                __warn_once = 0;                                \
                __ret = 1;                                      \
            }                                                   \
            __ret;                                              \
     })

    #define DO_ONCE_ON(__condition, __action)                   \
    ({                                                          \
            static int __warn_once = 1;                         \
            int __ret = 0;                                      \
                                                                \
            if (unlikely(__warn_once && __condition)) {         \
                (__action);                                     \
                __warn_once = 0;                                \
                __ret = 1;                                      \
            }                                                   \
            __ret;                                              \
     })



    #define ERR(_f, _a...)   do {                               \
        /* console_force_unlock();  */                          \
        printf(TXT_FG_RED "ERR:" TXT_FG_WHITE "(%s)"            \
                _f,  __func__, ## _a);                          \
    } while (0)

    #define ERR_ON(_g, _f, _a...) do {                          \
            if (_g) {                                           \
                ERR(_f, ## _a);                                 \
            }                                                   \
    } while (0)

    #define BUG(_f, _a...)   do {                               \
        printk("BUG:(%s) "                                      \
                _f, __func__, ## _a);                           \
        dump_execution_state();                                 \
    } while (0)

          

