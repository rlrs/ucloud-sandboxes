// Copyright 2026 UCloud Sandboxes contributors.
//
// A deliberately small stateful workload for comparing gVisor's normal memfd
// MemoryFile with an experimental regular-file-backed MemoryFile. The server
// keeps an anonymous mapping alive and the client asks it to scan or dirty the
// pages after the host has reclaimed the sandbox cgroup.

#define _GNU_SOURCE

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#define SOCKET_PATH "/tmp/gvisor-memory-workload.sock"
#define PAGE_SIZE_BYTES 4096ULL

static uint8_t *mapping;
static size_t logical_pages;
static size_t populated_pages;
static uint64_t expected_sum;

static void die(const char *message) {
  perror(message);
  exit(1);
}

static uint8_t initial_value(size_t page_index) {
  return (uint8_t)((page_index * UINT64_C(1315423911) + 17) & 0xff);
}

static size_t populated_page_index(size_t ordinal) {
  return (ordinal * logical_pages) / populated_pages;
}

static uint64_t monotonic_ns(void) {
  struct timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
    die("clock_gettime");
  }
  return (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
}

static void write_all(int fd, const char *buffer, size_t length) {
  while (length > 0) {
    ssize_t written = write(fd, buffer, length);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      die("write");
    }
    buffer += written;
    length -= (size_t)written;
  }
}

static void initialize_mapping(size_t logical_mib, size_t populated_mib) {
  if (logical_mib == 0 || populated_mib == 0 || populated_mib > logical_mib) {
    fprintf(stderr, "invalid logical/populated MiB: %zu/%zu\n", logical_mib,
            populated_mib);
    exit(2);
  }
  logical_pages = logical_mib * 1024ULL * 1024ULL / PAGE_SIZE_BYTES;
  populated_pages = populated_mib * 1024ULL * 1024ULL / PAGE_SIZE_BYTES;
  size_t length = logical_pages * PAGE_SIZE_BYTES;
  mapping = mmap(NULL, length, PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (mapping == MAP_FAILED) {
    die("mmap");
  }
  if (madvise(mapping, length, MADV_NOHUGEPAGE) != 0 && errno != EINVAL) {
    die("madvise(MADV_NOHUGEPAGE)");
  }
  for (size_t ordinal = 0; ordinal < populated_pages; ++ordinal) {
    size_t page = populated_page_index(ordinal);
    uint8_t value = initial_value(page);
    mapping[page * PAGE_SIZE_BYTES] = value;
    expected_sum += value;
  }
}

static void scan_mapping(char *response, size_t response_size) {
  uint64_t started = monotonic_ns();
  uint64_t sum = 0;
  for (size_t ordinal = 0; ordinal < populated_pages; ++ordinal) {
    size_t page = populated_page_index(ordinal);
    sum += *(volatile uint8_t *)&mapping[page * PAGE_SIZE_BYTES];
  }
  uint64_t elapsed = monotonic_ns() - started;
  if (sum != expected_sum) {
    snprintf(response, response_size,
             "error checksum=%" PRIu64 " expected=%" PRIu64 "\n", sum,
             expected_sum);
    return;
  }
  snprintf(response, response_size,
           "scan ns=%" PRIu64 " checksum=%" PRIu64 " pages=%zu bytes=%zu\n",
           elapsed, sum, populated_pages,
           (size_t)(populated_pages * PAGE_SIZE_BYTES));
}

static void dirty_mapping(unsigned int percent, char *response,
                          size_t response_size) {
  if (percent > 100) {
    snprintf(response, response_size, "error invalid-percent\n");
    return;
  }
  size_t count = (populated_pages * percent + 99) / 100;
  for (size_t ordinal = 0; ordinal < count; ++ordinal) {
    size_t page = populated_page_index(ordinal);
    uint8_t *value = &mapping[page * PAGE_SIZE_BYTES];
    expected_sum -= *value;
    *value ^= 0x5a;
    expected_sum += *value;
  }
  snprintf(response, response_size, "dirty percent=%u pages=%zu\n", percent,
           count);
}

static void handle_command(const char *command, char *response,
                           size_t response_size) {
  if (strcmp(command, "ready") == 0) {
    snprintf(response, response_size,
             "ready logical_pages=%zu populated_pages=%zu checksum=%" PRIu64
             "\n",
             logical_pages, populated_pages, expected_sum);
    return;
  }
  if (strcmp(command, "scan") == 0) {
    scan_mapping(response, response_size);
    return;
  }
  unsigned int percent;
  if (sscanf(command, "dirty:%u", &percent) == 1) {
    dirty_mapping(percent, response, response_size);
    return;
  }
  snprintf(response, response_size, "error unknown-command\n");
}

static int run_server(size_t logical_mib, size_t populated_mib) {
  initialize_mapping(logical_mib, populated_mib);
  unlink(SOCKET_PATH);
  int server = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (server < 0) {
    die("socket");
  }
  struct sockaddr_un address = {.sun_family = AF_UNIX};
  snprintf(address.sun_path, sizeof(address.sun_path), "%s", SOCKET_PATH);
  if (bind(server, (struct sockaddr *)&address, sizeof(address)) != 0) {
    die("bind");
  }
  if (listen(server, 8) != 0) {
    die("listen");
  }

  for (;;) {
    int client = accept4(server, NULL, NULL, SOCK_CLOEXEC);
    if (client < 0) {
      if (errno == EINTR) {
        continue;
      }
      die("accept4");
    }
    char command[128];
    ssize_t length = read(client, command, sizeof(command) - 1);
    if (length < 0) {
      die("read");
    }
    command[length] = '\0';
    char *newline = strchr(command, '\n');
    if (newline != NULL) {
      *newline = '\0';
    }
    char response[256];
    handle_command(command, response, sizeof(response));
    write_all(client, response, strlen(response));
    close(client);
  }
  return 0;
}

static int run_client(const char *command) {
  int client = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (client < 0) {
    die("socket");
  }
  struct sockaddr_un address = {.sun_family = AF_UNIX};
  snprintf(address.sun_path, sizeof(address.sun_path), "%s", SOCKET_PATH);
  if (connect(client, (struct sockaddr *)&address, sizeof(address)) != 0) {
    die("connect");
  }
  write_all(client, command, strlen(command));
  write_all(client, "\n", 1);
  char response[256];
  ssize_t length = read(client, response, sizeof(response) - 1);
  if (length < 0) {
    die("read");
  }
  response[length] = '\0';
  fputs(response, stdout);
  return strncmp(response, "error ", 6) == 0;
}

int main(int argc, char **argv) {
  if (argc == 4 && strcmp(argv[1], "server") == 0) {
    return run_server(strtoull(argv[2], NULL, 10),
                      strtoull(argv[3], NULL, 10));
  }
  if (argc == 3 && strcmp(argv[1], "client") == 0) {
    return run_client(argv[2]);
  }
  fprintf(stderr,
          "usage: %s server LOGICAL_MIB POPULATED_MIB | client COMMAND\n",
          argv[0]);
  return 2;
}
