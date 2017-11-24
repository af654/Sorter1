#include <pthread.h>

// Support for multiple stacks (each one has a mutex)
typedef struct stack {
  int count;
  pthread_mutex_t m; 
  Row *** rows; 
} stack_safe;

stack_safe* stack_create(int capacity) {
  stack_safe *result = malloc(sizeof(stack_safe));
  result->count = 0;
  result->rows = malloc(sizeof(Row **) * capacity);
  pthread_mutex_init(&result->m, NULL);
  return result;
}

void stack_destroy(stack_safe *s) {
  free(s->rows);
  pthread_mutex_destroy(&s->m);
  free(s);
}
// Warning no underflow or overflow checks!

void push(stack_safe *s, Row ** row) { 
  pthread_mutex_lock(&s->m); 
  s->rows[(s->count)++] = row; 
  pthread_mutex_unlock(&s->m); 
}

Row ** pop(stack_safe *s) { 
  pthread_mutex_lock(&s->m); 
  Row ** v = s->rows[--(s->count)]; 
  pthread_mutex_unlock(&s->m); 
  return v;
}

//Prints 1 if the stack is empty
int is_empty(stack_safe *s) { 
  pthread_mutex_lock(&s->m); 
  int result = s->count == 0; 
  pthread_mutex_unlock(&s->m);
  return result;
}
