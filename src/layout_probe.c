/* Expose C structure sizes, alignments, and field offsets for tests/layout.rs
 * No implementations are included because wrapper.c provides them */

#include <stddef.h>

#include "clickhouse.h"
#include "clickhouse-posix-io.h"
#include "clickhouse-compression.h"
#include "clickhouse-client.h"

#define CHC_RS_LAYOUT(T)                                       \
    size_t chc_rs_size_##T(void)  { return sizeof(T); }        \
    size_t chc_rs_align_##T(void) { return _Alignof(T); }
#define CHC_RS_FIELD(T, F)                                     \
    size_t chc_rs_off_##T##__##F(void) { return offsetof(T, F); }

/* clickhouse.h */
CHC_RS_LAYOUT(chc_err)
CHC_RS_FIELD(chc_err, server_code)
CHC_RS_FIELD(chc_err, msg)
CHC_RS_FIELD(chc_err, server_name)

CHC_RS_LAYOUT(chc_alloc)
CHC_RS_FIELD(chc_alloc, ud)
CHC_RS_FIELD(chc_alloc, alloc)
CHC_RS_FIELD(chc_alloc, realloc)
CHC_RS_FIELD(chc_alloc, free)

CHC_RS_LAYOUT(chc_io)
CHC_RS_FIELD(chc_io, ud)
CHC_RS_FIELD(chc_io, read)
CHC_RS_FIELD(chc_io, write)
CHC_RS_FIELD(chc_io, check_cancel)

/* clickhouse-posix-io.h */
CHC_RS_LAYOUT(chc_posix_io)
CHC_RS_FIELD(chc_posix_io, fd)
CHC_RS_FIELD(chc_posix_io, check_cancel)
CHC_RS_FIELD(chc_posix_io, cancel_ud)
CHC_RS_FIELD(chc_posix_io, deadline_us)

CHC_RS_LAYOUT(chc_block_opts)
CHC_RS_FIELD(chc_block_opts, has_block_info)
CHC_RS_FIELD(chc_block_opts, has_custom_serialization)
CHC_RS_FIELD(chc_block_opts, read_buffer_bytes)

/* Check public fields and anonymous union position */
CHC_RS_LAYOUT(chc_column)
CHC_RS_FIELD(chc_column, layout)
CHC_RS_FIELD(chc_column, n_rows)
size_t chc_rs_off_chc_column__payload(void) { return offsetof(chc_column, fixed); }

CHC_RS_LAYOUT(chc_block_col)
CHC_RS_FIELD(chc_block_col, name)
CHC_RS_FIELD(chc_block_col, name_len)
CHC_RS_FIELD(chc_block_col, type)
CHC_RS_FIELD(chc_block_col, col)

CHC_RS_LAYOUT(chc_block_builder)
CHC_RS_FIELD(chc_block_builder, cols)
CHC_RS_FIELD(chc_block_builder, n_cols)
CHC_RS_FIELD(chc_block_builder, n_rows)

/* clickhouse-compression.h */
CHC_RS_LAYOUT(chc_codec)
CHC_RS_FIELD(chc_codec, ud)
CHC_RS_FIELD(chc_codec, lz4_compress)
CHC_RS_FIELD(chc_codec, lz4_decompress)
CHC_RS_FIELD(chc_codec, zstd_compress)
CHC_RS_FIELD(chc_codec, zstd_decompress)
CHC_RS_FIELD(chc_codec, lz4_bound)
CHC_RS_FIELD(chc_codec, zstd_bound)

/* clickhouse-client.h */
CHC_RS_LAYOUT(chc_client_opts)
CHC_RS_FIELD(chc_client_opts, client_name)
CHC_RS_FIELD(chc_client_opts, client_version_major)
CHC_RS_FIELD(chc_client_opts, client_version_minor)
CHC_RS_FIELD(chc_client_opts, client_version_patch)
CHC_RS_FIELD(chc_client_opts, client_revision)
CHC_RS_FIELD(chc_client_opts, database)
CHC_RS_FIELD(chc_client_opts, user)
CHC_RS_FIELD(chc_client_opts, password)
CHC_RS_FIELD(chc_client_opts, compression)
CHC_RS_FIELD(chc_client_opts, codec)
CHC_RS_FIELD(chc_client_opts, read_buffer_bytes)

CHC_RS_LAYOUT(chc_server_info)
CHC_RS_FIELD(chc_server_info, name)
CHC_RS_FIELD(chc_server_info, timezone)
CHC_RS_FIELD(chc_server_info, display_name)
CHC_RS_FIELD(chc_server_info, version_major)
CHC_RS_FIELD(chc_server_info, version_minor)
CHC_RS_FIELD(chc_server_info, version_patch)
CHC_RS_FIELD(chc_server_info, revision)

CHC_RS_LAYOUT(chc_exception)
CHC_RS_FIELD(chc_exception, code)
CHC_RS_FIELD(chc_exception, name)
CHC_RS_FIELD(chc_exception, name_len)
CHC_RS_FIELD(chc_exception, display_text)
CHC_RS_FIELD(chc_exception, display_text_len)
CHC_RS_FIELD(chc_exception, stack_trace)
CHC_RS_FIELD(chc_exception, stack_trace_len)

CHC_RS_LAYOUT(chc_query_setting)
CHC_RS_FIELD(chc_query_setting, name)
CHC_RS_FIELD(chc_query_setting, value)
CHC_RS_FIELD(chc_query_setting, important)
CHC_RS_FIELD(chc_query_setting, custom)

CHC_RS_LAYOUT(chc_query_param)
CHC_RS_FIELD(chc_query_param, name)
CHC_RS_FIELD(chc_query_param, value)

CHC_RS_LAYOUT(chc_query_opts)
CHC_RS_FIELD(chc_query_opts, query_id)
CHC_RS_FIELD(chc_query_opts, query_id_len)
CHC_RS_FIELD(chc_query_opts, settings)
CHC_RS_FIELD(chc_query_opts, n_settings)
CHC_RS_FIELD(chc_query_opts, params)
CHC_RS_FIELD(chc_query_opts, n_params)

CHC_RS_LAYOUT(chc_packet)
CHC_RS_FIELD(chc_packet, kind)
/* First union member provides offset of Rust `payload` field */
size_t chc_rs_off_chc_packet__payload(void) { return offsetof(chc_packet, block); }

/* Check anonymous progress and profile structures mirrored as Rust types */
size_t chc_rs_size_progress(void)  { return sizeof(((chc_packet *) 0)->progress); }
size_t chc_rs_off_progress_rows(void)          { return offsetof(chc_packet, progress.rows); }
size_t chc_rs_off_progress_bytes(void)         { return offsetof(chc_packet, progress.bytes); }
size_t chc_rs_off_progress_total_rows(void)    { return offsetof(chc_packet, progress.total_rows); }
size_t chc_rs_off_progress_written_rows(void)  { return offsetof(chc_packet, progress.written_rows); }
size_t chc_rs_off_progress_written_bytes(void) { return offsetof(chc_packet, progress.written_bytes); }

size_t chc_rs_size_profile(void)   { return sizeof(((chc_packet *) 0)->profile); }
size_t chc_rs_off_profile_rows(void)             { return offsetof(chc_packet, profile.rows); }
size_t chc_rs_off_profile_blocks(void)           { return offsetof(chc_packet, profile.blocks); }
size_t chc_rs_off_profile_bytes(void)            { return offsetof(chc_packet, profile.bytes); }
size_t chc_rs_off_profile_rows_before_limit(void){ return offsetof(chc_packet, profile.rows_before_limit); }
size_t chc_rs_off_profile_applied_limit(void)    { return offsetof(chc_packet, profile.applied_limit); }
size_t chc_rs_off_profile_calc_rows(void)        { return offsetof(chc_packet, profile.calculated_rows_before_limit); }
