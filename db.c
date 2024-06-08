#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// struct which holds the bytes read from stdin,
// the length of buffer and input length
typedef struct {
  char *buffer;
  size_t buffer_length;
  ssize_t input_length;
} InputBuffer;

// this method is used to create a pointer to the newly created input buffer
InputBuffer *new_input_buffer() {
  // creating a new pointer after 
  // allocating block of memory on the heap
  InputBuffer *input_buffer = (InputBuffer *)malloc(sizeof(InputBuffer));

  // initializaing to default values
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;

  return input_buffer;
}

// this is a utility method for prepending "db >"
void print_prompt() { printf("db > "); }

// this method is used to free up the memory after the pointer
// has been used and no longer needed
void close_input_buffer(InputBuffer *input_buffer) {
  // we free up the buffer bit because after being set by the getline
  // we need to manually free up the space used by the buffer
  free(input_buffer->buffer);
  free(input_buffer);
}

// this method is used to get read from the input stream
void read_input(InputBuffer *input_buffer) {
  // we calculate the bytes read
  // ref: getline description
  ssize_t bytes_read =
      getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

  // check so that if the number of bytes read is 0
  // we break out of the REPL
  if (bytes_read <= 0) {
    printf("Error reading input\n");
    exit(EXIT_FAILURE);
  }

  // ignoring the trailing newline
  input_buffer->input_length = bytes_read - 1;
  input_buffer->buffer[bytes_read - 1] = 0;
}

// this method is the driver method
int main(int argc, char *argv[]) {
  // we allocate an input buffer
  InputBuffer *input_buffer = new_input_buffer();

  // the REPL starting point
  while (true) {
    print_prompt();
    // we read the input
    read_input(input_buffer);

    // we read and see if the buffer is equal to ".exit"
    // if yes then we close and exit the REPL else we say the command was not supported
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
      close_input_buffer(input_buffer);
      exit(EXIT_SUCCESS);
    } else {
      printf("Unrecognized command '%s' .\n", input_buffer->buffer);
    }
  }
}