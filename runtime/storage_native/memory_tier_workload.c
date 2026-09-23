/* Native-only memory-tier experiment. Keeps a TCP connection and SQLite WAL
 * writer open across reclamation/checkpoint; validates every heap word. */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#define MIB (1024UL * 1024UL)
#define HEAP_BYTES (1536UL * MIB)
#define DIRTY_BYTES (384UL * MIB)
#define SOCK "/tmp/memory-tier.sock"
static uint64_t *heap, expected;
static uint64_t generation;
static sqlite3 *db;
static int tcp[2];

static void die(const char *s) { perror(s); exit(1); }
static void require(int ok, const char *s) { if (!ok) die(s); }
static void sql(const char *s) {
  char *error = NULL;
  if (sqlite3_exec(db, s, NULL, NULL, &error) != SQLITE_OK) {
    fprintf(stderr, "SQLite: %s: %s\n", s, error); exit(1);
  }
}
static uint64_t checksum(void) {
  uint64_t total = 0;
  for (size_t i = 0; i < HEAP_BYTES / 8; i++) total += heap[i];
  return total;
}
static void *echo(void *arg) {
  (void)arg;
  for (;;) {
    unsigned char byte;
    require(read(tcp[1], &byte, 1) == 1, "TCP read");
    byte ^= 0x7b;
    require(write(tcp[1], &byte, 1) == 1, "TCP write");
  }
  return NULL;
}
static void initialize(void) {
  heap = mmap(NULL, HEAP_BYTES, PROT_READ | PROT_WRITE,
              MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  require(heap != MAP_FAILED, "mmap");
  uint64_t seed = 0x9e3779b97f4a7c15ULL;
  for (size_t i = 0; i < HEAP_BYTES / 8; i++) {
    seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17;
    heap[i] = seed; expected += seed;
  }
  require(sqlite3_open("/state.db", &db) == SQLITE_OK, "sqlite open");
  sql("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;"
      "CREATE TABLE proof(sequence INTEGER PRIMARY KEY, payload BLOB);"
      "INSERT INTO proof VALUES(0, zeroblob(4096));");
  int listener = socket(AF_INET, SOCK_STREAM, 0);
  require(listener >= 0, "TCP socket");
  struct sockaddr_in address = {.sin_family=AF_INET,
      .sin_addr={.s_addr=htonl(INADDR_LOOPBACK)}};
  require(bind(listener, (struct sockaddr *)&address, sizeof(address)) == 0, "bind");
  require(listen(listener, 1) == 0, "listen");
  socklen_t length = sizeof(address);
  require(getsockname(listener, (struct sockaddr *)&address, &length) == 0, "getsockname");
  tcp[0] = socket(AF_INET, SOCK_STREAM, 0);
  require(connect(tcp[0], (struct sockaddr *)&address, length) == 0, "connect");
  tcp[1] = accept(listener, NULL, NULL);
  require(tcp[1] >= 0, "accept"); close(listener);
  pthread_t worker;
  require(pthread_create(&worker, NULL, echo, NULL) == 0, "pthread");
}
static void verify(void) {
  require(checksum() == expected, "heap checksum mismatch");
  unsigned char byte = 0x35;
  require(write(tcp[0], &byte, 1) == 1, "TCP request");
  require(read(tcp[0], &byte, 1) == 1 && byte == (0x35 ^ 0x7b), "TCP response");
  sqlite3_stmt *stmt;
  require(sqlite3_prepare_v2(db, "SELECT MAX(sequence) FROM proof", -1, &stmt, NULL) == SQLITE_OK, "prepare");
  require(sqlite3_step(stmt) == SQLITE_ROW && (uint64_t)sqlite3_column_int64(stmt, 0) == generation, "SQLite sequence");
  sqlite3_finalize(stmt);
  require(sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &stmt, NULL) == SQLITE_OK, "integrity prepare");
  require(sqlite3_step(stmt) == SQLITE_ROW && !strcmp((const char *)sqlite3_column_text(stmt, 0), "ok"), "SQLite integrity");
  sqlite3_finalize(stmt);
}
static void dirty(void) {
  verify();
  uint64_t mask = 0x9e3779b97f4a7c15ULL + ++generation;
  for (size_t i = 0; i < DIRTY_BYTES / 8; i++) {
    expected -= heap[i]; heap[i] ^= mask; expected += heap[i];
  }
  char query[200];
  snprintf(query, sizeof(query), "INSERT INTO proof VALUES(%llu, randomblob(4096))", (unsigned long long)generation);
  sql(query);
}
static int server(void) {
  initialize();
  int listener = socket(AF_UNIX, SOCK_STREAM, 0);
  struct sockaddr_un address = {.sun_family=AF_UNIX};
  strcpy(address.sun_path, SOCK);
  require(bind(listener, (struct sockaddr *)&address, sizeof(address)) == 0, "UNIX bind");
  require(listen(listener, 8) == 0, "UNIX listen");
  for (;;) {
    int client = accept(listener, NULL, NULL);
    require(client >= 0, "UNIX accept");
    char command[32] = {0};
    require(read(client, command, sizeof(command)-1) > 0, "command");
    if (!strcmp(command, "dirty")) dirty();
    else if (!strcmp(command, "verify")) verify();
    else require(!strcmp(command, "ping"), "unknown command");
    dprintf(client, "ok %llu %llu\n", (unsigned long long)generation, (unsigned long long)expected);
    close(client);
  }
  return 0;
}
int main(int argc, char **argv) {
  if (argc != 2) return 2;
  if (!strcmp(argv[1], "server")) return server();
  if (!strcmp(argv[1], "client")) argv[1] = "verify";
  int client = socket(AF_UNIX, SOCK_STREAM, 0);
  struct sockaddr_un address = {.sun_family=AF_UNIX};
  strcpy(address.sun_path, SOCK);
  require(connect(client, (struct sockaddr *)&address, sizeof(address)) == 0, "client connect");
  require(write(client, argv[1], strlen(argv[1])) == (ssize_t)strlen(argv[1]), "client write");
  char result[128] = {0};
  require(read(client, result, sizeof(result)-1) > 0, "client response");
  fputs(result, stdout);
  return strncmp(result, "ok ", 3) ? 1 : 0;
}
