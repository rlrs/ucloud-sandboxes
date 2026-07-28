#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/timerfd.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <time.h>
#include <unistd.h>

#define ERROR_PATH "/conformance-error.log"
#define CHECK(call) do { if ((call) < 0) { fail(#call); } } while (0)
#define SOCKET_PATH "/tmp/conformance.sock"
#define MAPPING_BYTES (16U * 1024U * 1024U)

static int child_in[2];
static int child_out[2];
static int pair_fd[2];
static int tcp_fd[2];
static int event_fd;
static int epoll_fd;
static int timer_fd;
static int deleted_fd;
static uint8_t *mapping;
static volatile sig_atomic_t signal_count;
static pthread_mutex_t cond_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cond_request = PTHREAD_COND_INITIALIZER;
static pthread_cond_t cond_response = PTHREAD_COND_INITIALIZER;
static uint64_t cond_requested;
static uint64_t cond_completed;

static void fail(const char *operation) {
  int saved_errno = errno;
  int fd = open(ERROR_PATH, O_CREAT | O_APPEND | O_WRONLY, 0600);
  if (fd >= 0) {
    dprintf(fd, "%s: %s\n", operation, strerror(saved_errno));
    close(fd);
  }
  errno = saved_errno;
  perror(operation);
  exit(1);
}

static void on_signal(int signo) {
  (void)signo;
  signal_count++;
}

static void *socket_worker(void *unused) {
  (void)unused;
  for (;;) {
    uint8_t value;
    ssize_t got = read(pair_fd[1], &value, 1);
    if (got == 0) return NULL;
    if (got != 1) {
      perror("socket worker read");
      abort();
    }
    value ^= 0x5a;
    if (write(pair_fd[1], &value, 1) != 1) {
      perror("socket worker write");
      abort();
    }
  }
}

static void *tcp_worker(void *unused) {
  (void)unused;
  for (;;) {
    uint8_t value;
    ssize_t got = read(tcp_fd[1], &value, 1);
    if (got == 0) return NULL;
    if (got != 1) {
      perror("tcp worker read");
      abort();
    }
    value ^= 0x3c;
    if (write(tcp_fd[1], &value, 1) != 1) {
      perror("tcp worker write");
      abort();
    }
  }
}

static void *condition_worker(void *unused) {
  (void)unused;
  pthread_mutex_lock(&cond_mu);
  for (;;) {
    while (cond_completed == cond_requested) {
      pthread_cond_wait(&cond_request, &cond_mu);
    }
    cond_completed = cond_requested;
    pthread_cond_signal(&cond_response);
  }
  return NULL;
}

static void child_worker(void) {
  close(child_in[1]);
  close(child_out[0]);
  for (;;) {
    uint8_t value;
    ssize_t got = read(child_in[0], &value, 1);
    if (got == 0) _exit(0);
    if (got != 1) _exit(2);
    value ^= 0xa5;
    if (write(child_out[1], &value, 1) != 1) _exit(3);
  }
}

static uint64_t verify_state(void) {
  uint8_t value = 0x31;
  if (write(child_in[1], &value, 1) != 1 ||
      read(child_out[0], &value, 1) != 1 ||
      value != (uint8_t)(0x31 ^ 0xa5)) {
    return 1;
  }

  value = 0x22;
  if (write(pair_fd[0], &value, 1) != 1 ||
      read(pair_fd[0], &value, 1) != 1 ||
      value != (uint8_t)(0x22 ^ 0x5a)) {
    return 2;
  }

  value = 0x47;
  if (write(tcp_fd[0], &value, 1) != 1 ||
      read(tcp_fd[0], &value, 1) != 1 ||
      value != (uint8_t)(0x47 ^ 0x3c)) {
    return 10;
  }

  uint64_t one = 1;
  if (write(event_fd, &one, sizeof(one)) != sizeof(one)) return 3;
  struct epoll_event event;
  if (epoll_wait(epoll_fd, &event, 1, 1000) != 1 ||
      event.data.u64 != 0xc0ffee) {
    return 4;
  }
  if (read(event_fd, &one, sizeof(one)) != sizeof(one) || one != 1) return 5;

  struct itimerspec timer;
  if (timerfd_gettime(timer_fd, &timer) != 0 ||
      (timer.it_value.tv_sec == 0 && timer.it_value.tv_nsec == 0)) {
    return 6;
  }

  char deleted[32] = {0};
  if (pread(deleted_fd, deleted, sizeof(deleted) - 1, 0) < 0 ||
      strcmp(deleted, "deleted-but-open-state") != 0) {
    return 7;
  }

  uint64_t checksum = 0;
  for (size_t offset = 0; offset < MAPPING_BYTES; offset += 4096) {
    checksum += mapping[offset];
  }
  if (checksum != 522240) return 8;

  pthread_mutex_lock(&cond_mu);
  cond_requested++;
  pthread_cond_signal(&cond_request);
  while (cond_completed != cond_requested) {
    pthread_cond_wait(&cond_response, &cond_mu);
  }
  uint64_t generation = cond_completed;
  pthread_mutex_unlock(&cond_mu);

  sig_atomic_t before = signal_count;
  if (kill(getpid(), SIGUSR1) != 0 || signal_count != before + 1) return 9;
  return generation << 32 | checksum;
}

static void run_server(void) {
  CHECK(pipe(child_in));
  CHECK(pipe(child_out));
  pid_t child = fork();
  CHECK(child);
  if (child == 0) child_worker();
  close(child_in[0]);
  close(child_out[1]);

  CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, pair_fd));
  int tcp_listener = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  CHECK(tcp_listener);
  struct sockaddr_in tcp_address = {
      .sin_family = AF_INET,
      .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)},
  };
  CHECK(bind(tcp_listener, (struct sockaddr *)&tcp_address, sizeof(tcp_address)));
  CHECK(listen(tcp_listener, 1));
  socklen_t tcp_address_size = sizeof(tcp_address);
  CHECK(getsockname(tcp_listener, (struct sockaddr *)&tcp_address,
                    &tcp_address_size));
  tcp_fd[0] = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  CHECK(tcp_fd[0]);
  CHECK(connect(tcp_fd[0], (struct sockaddr *)&tcp_address,
                sizeof(tcp_address)));
  tcp_fd[1] = accept4(tcp_listener, NULL, NULL, SOCK_CLOEXEC);
  CHECK(tcp_fd[1]);

  pthread_t socket_thread;
  pthread_t tcp_thread;
  pthread_t condition_thread;
  int pthread_error = pthread_create(
      &socket_thread, NULL, socket_worker, NULL);
  if (pthread_error == 0) {
    pthread_error = pthread_create(&tcp_thread, NULL, tcp_worker, NULL);
  }
  if (pthread_error == 0) {
    pthread_error = pthread_create(
        &condition_thread, NULL, condition_worker, NULL);
  }
  if (pthread_error != 0) {
    errno = pthread_error;
    fail("pthread_create");
  }

  event_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  CHECK(event_fd);
  epoll_fd = epoll_create1(EPOLL_CLOEXEC);
  CHECK(epoll_fd);
  struct epoll_event event = {.events = EPOLLIN, .data.u64 = 0xc0ffee};
  CHECK(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, event_fd, &event));

  timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC);
  CHECK(timer_fd);
  struct itimerspec timer = {.it_value = {.tv_sec = 24 * 60 * 60}};
  CHECK(timerfd_settime(timer_fd, 0, &timer, NULL));

  deleted_fd = open("/tmp/deleted-state", O_CREAT | O_EXCL | O_RDWR, 0600);
  CHECK(deleted_fd);
  const char deleted[] = "deleted-but-open-state";
  if (write(deleted_fd, deleted, sizeof(deleted)) != sizeof(deleted)) exit(1);
  CHECK(unlink("/tmp/deleted-state"));

  mapping = mmap(NULL, MAPPING_BYTES, PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (mapping == MAP_FAILED) {
    perror("mmap");
    exit(1);
  }
  for (size_t offset = 0; offset < MAPPING_BYTES; offset += 4096) {
    mapping[offset] = (uint8_t)(offset / 4096);
  }

  struct sigaction action = {.sa_handler = on_signal};
  sigemptyset(&action.sa_mask);
  CHECK(sigaction(SIGUSR1, &action, NULL));

  int listener = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
  CHECK(listener);
  struct sockaddr_un address = {.sun_family = AF_UNIX};
  strcpy(address.sun_path, SOCKET_PATH);
  unlink(SOCKET_PATH);
  CHECK(bind(listener, (struct sockaddr *)&address, sizeof(address)));
  CHECK(listen(listener, 8));

  for (;;) {
    int client = accept4(listener, NULL, NULL, SOCK_CLOEXEC);
    if (client < 0) {
      if (errno == EINTR) continue;
      CHECK(client);
    }
    char request[16] = {0};
    ssize_t got = read(client, request, sizeof(request) - 1);
    if (got > 0 && strcmp(request, "verify") == 0) {
      uint64_t result = verify_state();
      dprintf(client, result > UINT32_MAX ? "ok %llu\n" : "error %llu\n",
              (unsigned long long)result);
    } else {
      dprintf(client, "error request\n");
    }
    close(client);
  }
}

static int run_client(void) {
  int client = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
  CHECK(client);
  struct sockaddr_un address = {.sun_family = AF_UNIX};
  strcpy(address.sun_path, SOCKET_PATH);
  for (int attempt = 0; connect(client, (struct sockaddr *)&address,
                                sizeof(address)) != 0; attempt++) {
    if (attempt == 100 || (errno != ENOENT && errno != ECONNREFUSED)) {
      perror("connect");
      return 1;
    }
    usleep(10000);
  }
  if (write(client, "verify", 7) != 7) return 1;
  char response[128] = {0};
  ssize_t got = read(client, response, sizeof(response) - 1);
  if (got <= 0) return 1;
  fputs(response, stdout);
  return strncmp(response, "ok ", 3) == 0 ? 0 : 1;
}

int main(int argc, char **argv) {
  if (argc != 2) {
    fprintf(stderr, "usage: %s server|client\n", argv[0]);
    return 2;
  }
  if (strcmp(argv[1], "server") == 0) {
    run_server();
    return 0;
  }
  if (strcmp(argv[1], "client") == 0) return run_client();
  return 2;
}
