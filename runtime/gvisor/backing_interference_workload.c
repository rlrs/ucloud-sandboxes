/* Real guest dirty-memory/SQLite interference fixture. Build statically. */
#define _GNU_SOURCE
#include <errno.h>
#include <pthread.h>
#include <sqlite3.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

static unsigned char *memory;
static size_t resident, dirty;
static atomic_int stopping;
static unsigned int generations;

static uint64_t page_word(uint64_t value) {
  value += UINT64_C(0x9e3779b97f4a7c15);
  value = (value ^ (value >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
  value = (value ^ (value >> 27)) * UINT64_C(0x94d049bb133111eb);
  return value ^ (value >> 31);
}

static double seconds(void) {
  struct timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now)) abort();
  return now.tv_sec + now.tv_nsec / 1e9;
}

static void fail(sqlite3 *db, const char *operation) {
  fprintf(stderr, "%s: %s\n", operation, db ? sqlite3_errmsg(db) : strerror(errno));
  exit(1);
}

static void sql(sqlite3 *db, const char *statement) {
  if (sqlite3_exec(db, statement, NULL, NULL, NULL) != SQLITE_OK) fail(db, statement);
}

static void *dirty_memory(void *unused) {
  (void)unused;
  while (!atomic_load(&stopping)) {
    ++generations;
    /* Every dirty page receives a unique deterministic sentinel. */
    for (size_t offset = 0; offset < dirty; offset += 4096)
      memory[offset] = (unsigned char)(generations + offset / 4096);
    usleep(20000);
  }
  return NULL;
}

static int compare(const void *a, const void *b) {
  double x = *(const double *)a, y = *(const double *)b;
  return (x > y) - (x < y);
}

int main(int argc, char **argv) {
  if (argc != 4 && argc != 5) return 2;
  int duration = atoi(argv[1]), resident_mb = atoi(argv[2]), dirty_mb = atoi(argv[3]);
  int entropy_percent = argc == 5 ? atoi(argv[4]) : 0;
  if (duration < 1 || duration > 600 || resident_mb < 1 || resident_mb > 8192 ||
      dirty_mb < 1 || dirty_mb > resident_mb || entropy_percent < 0 || entropy_percent > 100) return 2;
  resident = (size_t)resident_mb * 1024 * 1024;
  dirty = (size_t)dirty_mb * 1024 * 1024;
  memory = mmap(NULL, resident, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (memory == MAP_FAILED) fail(NULL, "mmap");
  /* Sweep compressibility explicitly: zero-filled sentinels alone flatter a
     compressed backing store and do not represent every application heap. */
  for (size_t offset = 0; offset < resident; offset += 4096) {
    if ((int)((offset / 4096) % 100) < entropy_percent)
      for (size_t word = 0; word < 4096; word += 8)
        *(uint64_t *)(memory + offset + word) = page_word(offset + word);
    memory[offset] = (unsigned char)(offset / 4096);
  }
  sqlite3 *db = NULL;
  if (sqlite3_open("/workspace/commits.sqlite", &db) != SQLITE_OK) fail(db, "open");
  sql(db, "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=1000;"
          "CREATE TABLE records (id INTEGER PRIMARY KEY, payload BLOB NOT NULL);");
  sqlite3_stmt *insert = NULL;
  if (sqlite3_prepare_v2(db, "INSERT INTO records VALUES (?,?)", -1, &insert, NULL) != SQLITE_OK)
    fail(db, "prepare");
  unsigned char payload[16384];
  for (size_t i = 0; i < sizeof(payload); ++i) payload[i] = (unsigned char)(i * 17);
  if (sqlite3_bind_blob(insert, 2, payload, sizeof(payload), SQLITE_STATIC) != SQLITE_OK)
    fail(db, "bind payload");
  pthread_t worker;
  if (pthread_create(&worker, NULL, dirty_memory, NULL)) fail(NULL, "pthread_create");
  double *latency = calloc(100000, sizeof(double));
  if (!latency) fail(NULL, "calloc");
  double start = seconds();
  unsigned int commits = 0;
  while (seconds() - start < duration && commits < 100000) {
    double began = seconds();
    sqlite3_bind_int64(insert, 1, commits);
    if (sqlite3_step(insert) != SQLITE_DONE) fail(db, "commit");
    if (sqlite3_reset(insert) != SQLITE_OK) fail(db, "reset");
    latency[commits++] = seconds() - began;
    usleep(5000);
  }
  double elapsed = seconds() - start;
  atomic_store(&stopping, 1);
  if (pthread_join(worker, NULL)) fail(NULL, "pthread_join");
  for (size_t offset = 0; offset < resident; offset += 4096) {
    unsigned char expected = (unsigned char)((offset < dirty ? generations : 0) + offset / 4096);
    if (memory[offset] != expected) fail(NULL, "memory sentinel mismatch");
    if ((int)((offset / 4096) % 100) < entropy_percent) {
      for (size_t word = 0; word < 4096; word += 8) {
        uint64_t expected_word = page_word(offset + word);
        if (word == 0) ((unsigned char *)&expected_word)[0] = expected;
        if (*(uint64_t *)(memory + offset + word) != expected_word)
          fail(NULL, "memory payload mismatch");
      }
    }
  }
  sqlite3_finalize(insert);
  sqlite3_stmt *verify = NULL;
  if (sqlite3_prepare_v2(db, "SELECT payload FROM records ORDER BY id", -1, &verify, NULL) != SQLITE_OK)
    fail(db, "prepare verification");
  unsigned int verified = 0;
  int status;
  while ((status = sqlite3_step(verify)) == SQLITE_ROW) {
    if (sqlite3_column_bytes(verify, 0) != sizeof(payload) ||
        memcmp(sqlite3_column_blob(verify, 0), payload, sizeof(payload))) fail(NULL, "payload mismatch");
    ++verified;
  }
  if (status != SQLITE_DONE || verified != commits) fail(db, "row count mismatch");
  sqlite3_finalize(verify);
  sqlite3_close(db);
  qsort(latency, commits, sizeof(double), compare);
  printf("{\"commits\":%u,\"seconds\":%.6f,\"commit_p50\":%.9f,"
         "\"commit_p95\":%.9f,\"commit_p99\":%.9f,\"dirty_generations\":%u,"
         "\"resident_bytes\":%zu,\"dirty_bytes\":%zu,\"entropy_percent\":%d,\"verified\":true}\n",
         commits, elapsed, latency[commits / 2], latency[(commits - 1) * 95 / 100],
         latency[(commits - 1) * 99 / 100], generations, resident, dirty, entropy_percent);
  free(latency);
  munmap(memory, resident);
  return 0;
}
