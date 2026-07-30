// Copyright 2026 UCloud Sandboxes contributors.
//
// A guest execution canary for restore-start-paused qualification. The host
// observes this fixed-width counter through the overlay upper directory.

#define _GNU_SOURCE

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>

#define COUNTER_PATH "/handoff-probe/counter"

static void die(const char *operation) {
  perror(operation);
  exit(1);
}

int main(void) {
  int fd = open(COUNTER_PATH, O_CREAT | O_RDWR | O_CLOEXEC, 0600);
  if (fd < 0) {
    die("open counter");
  }
  uint64_t counter = 0;
  const struct timespec interval = {.tv_nsec = 1000000};
  for (;;) {
    counter++;
    ssize_t written = pwrite(fd, &counter, sizeof(counter), 0);
    if (written != (ssize_t)sizeof(counter)) {
      die("pwrite counter");
    }
    if (fdatasync(fd) != 0) {
      die("fdatasync counter");
    }
    while (nanosleep(&interval, NULL) != 0 && errno == EINTR) {
    }
  }
}
