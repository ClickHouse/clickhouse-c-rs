/* Compile clickhouse-c implementation and selected compression codecs */

/* Expose POSIX clock and poll APIs under C23 */
#define _POSIX_C_SOURCE 200809L
#define _DARWIN_C_SOURCE 200809L

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION

#ifndef CHC_RS_LZ4
#define CHC_NO_LZ4
#endif

#ifndef CHC_RS_ZSTD
#define CHC_NO_ZSTD
#endif

#include "clickhouse.h"
#include "clickhouse-posix-io.h"
#include "clickhouse-compression.h"
#include "clickhouse-client.h"
#include "clickhouse-async.h"

#include <time.h>

/* Return monotonic microseconds used by POSIX I/O deadlines */
int64_t
chc_rs_monotonic_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t) ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

/* Allocate implementation-private reader retained across block reads */
int
chc_rs_in_new(chc_io *io, const chc_alloc *al, size_t cap,
              chc_in **out, chc_err *err)
{
    chc_in *in = al->alloc(al->ud, sizeof *in);
    if (!in) return chc__err_set(err, CHC_ERR_OOM, "chc_in alloc failed");
    int rc = chc_in_init(in, io, al, cap, err);
    if (rc != CHC_OK) {
        al->free(al->ud, in, sizeof *in);
        return rc;
    }
    *out = in;
    return CHC_OK;
}

/* Allocate reader that only processes bytes passed to chc_in_submit */
int
chc_rs_in_new_ioless(const chc_alloc *al, chc_in **out, chc_err *err)
{
    chc_in *in = al->alloc(al->ud, sizeof *in);
    if (!in) return chc__err_set(err, CHC_ERR_OOM, "chc_in alloc failed");
    int rc = chc_in_init_ioless(in, al);
    if (rc != CHC_OK) {
        al->free(al->ud, in, sizeof *in);
        return chc__err_set(err, rc, "chc_in_init_ioless failed");
    }
    *out = in;
    return CHC_OK;
}

void
chc_rs_in_destroy(chc_in *in, const chc_alloc *al)
{
    if (!in) return;
    chc_in_free(in);
    al->free(al->ud, in, sizeof *in);
}
