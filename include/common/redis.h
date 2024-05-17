#include <stdlib.h>
#include <stdint.h>
#include <cstring>
#include <cmath>
#include <errno.h>
#include <ctype.h>
#include <stdio.h>
#include <climits>

#define MAX_LONG_DOUBLE_CHARS 5*1024

typedef enum {
    LD_STR_AUTO,     /* %.17Lg */
    LD_STR_HUMAN,    /* %.17Lf + Trimming of trailing zeros */
    LD_STR_HEX       /* %La */
} ld2string_mode;

int string2ld(const char *s, size_t slen, long double *dp);
int ld2string(char *buf, size_t len, long double value, ld2string_mode mode);
int string2ll(const char *s, size_t slen, long long *value);
int string2d(const char *s, size_t slen, double *dp);
int ll2string(char *dst, size_t dstlen, long long svalue);
uint32_t digits10(uint64_t v) ;
