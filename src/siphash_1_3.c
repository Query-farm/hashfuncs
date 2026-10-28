// Compiles the SipHash reference implementation as SipHash-1-3.
#define cROUNDS 1
#define dROUNDS 3
#define siphash hashfuncs_siphash_1_3
#include "third_party/siphash/siphash.c"
