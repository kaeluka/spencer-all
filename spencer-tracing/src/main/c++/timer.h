#ifndef TIMER_H
#define TIMER_H
#include <sys/time.h>

struct timeval then;

void tik() {
    gettimeofday(&then, NULL);
}

int tok_us() {
  struct timeval now;
  gettimeofday(&now, NULL);

  int us = (now.tv_sec - then.tv_sec)*1e6 + (now.tv_usec - then.tv_usec);
  return us;
}

#endif /* end of include guard: TIMER_H */
