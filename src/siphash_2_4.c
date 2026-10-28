// Compiles the SipHash reference implementation as SipHash-2-4.
#define cROUNDS 2
#define dROUNDS 4
#define siphash hashfuncs_siphash_2_4
#include "third_party/siphash/siphash.c"
