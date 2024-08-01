#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// struct which holds the bytes read from stdin,
// the length of buffer and input length
typedef struct {
  char *buffer;
  size_t buffer_length;
  ssize_t input_length;
} InputBuffer;

// defines types for statement execution
typedef enum { EXECUTE_SUCCESS, EXECUTE_TABLE_FULL } ExecuteResult;

// defines types for meta command
typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

// defines types for statement commands
typedef enum {
  PREPARE_SUCCESS,
  PREPARE_SYNTAX_ERROR,
  PREPARE_UNRECOGNIZED_STATEMENT,
  PREPARE_STRING_TOO_LONG,
  PREPARE_NEGATIVE_ID
} PrepareResult;

// defines types for statements, will grow over time
typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;

// defining types for nodes
typedef enum { NODE_INTERNAL, NODE_LEAF } NodeType;

// constant for schema
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

// fixed row schema
typedef struct {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE];
  char email[COLUMN_EMAIL_SIZE];
} Row;

// creating a statement dict to keep track of types
typedef struct {
  StatementType type;
  Row row_to_insert;
} Statement;

// defining constants for node header layout
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// constants for leaf node layout
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE =
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// constants for node body layout
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

// representation bits for calculating size
#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

// defining size and offsets of cells in row
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

// defining page and maximum pages for a table
const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100

// pager abstraction
// pager acts as a cache, if it doesnt find the page number,
// it loads it from the disk, also responsible for writing to the disk
typedef struct {
  int file_descriptor;
  uint32_t file_length;
  uint32_t num_pages;
  void *pages[TABLE_MAX_PAGES];
} Pager;

// currently we use array based paging
typedef struct {
  Pager *pager;
  uint32_t root_page_num;
} Table;

// cursor to keep track of which row we are at
typedef struct {
  Table *table;
  uint32_t page_num;
  uint32_t cell_num;
  bool end_of_table;
} Cursor;

// TODO: add defs
uint32_t * leaf_node_num_cells(void * node) {
  return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

// TODO: add defs
void * leaf_node_cell(void * node, uint32_t cell_num) {
  return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

// TODO: add defs
uint32_t * leaf_node_key(void * node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num);
}

// TODO: add defs
void * leaf_node_value(void * node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

// this method is used to initialize a node
void initialize_leaf_node(void * node) { *leaf_node_num_cells(node) = 0;}

// this method is used to initialize a cursor
// at the 0th row of the table
Cursor *table_start(Table *table) {
  Cursor *cursor = malloc(sizeof(Cursor));

  cursor->table = table;
  // cursor->row_num = 0;
  // cursor->end_of_table = (table->num_rows == 0);
  cursor->page_num = table->root_page_num;
  cursor->cell_num = 0;

  void * root_node = get_page(table->pager, table->root_page_num);
  uint32_t num_cells = *leaf_node_num_cells(root_node);
  cursor->end_of_table = (num_cells == 0);

  return cursor;
}

// this method is used to position the cursor to the end of the table
Cursor *table_end(Table *table) {
  Cursor *cursor = malloc(sizeof(Cursor));

  cursor->table = table;
  cursor->page_num = table->root_page_num;

  void * root_node = get_page(table->pager, table->root_page_num);
  uint32_t num_cells = *leaf_node_num_cells(root_node);
  cursor->cell_num = num_cells;
  cursor->end_of_table = true;

  return cursor;
}

// utility to print row using select statement
void print_row(Row *row) {
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

// copies from source to pages
void serialize_row(Row *source, void *destination) {
  memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
  memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
  memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

// copies from pages to destination
void deserialize_row(void *source, Row *destination) {
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

// the logic to retrive contents from the pager.
// it acts like a cache allocates if we have not found contents
// for a particular page number otherwise returns contents for
// the give cached page number
void *get_page(Pager *pager, uint32_t page_num) {
  // we check if the page number exceeds
  // the maximum specified pages limit
  if (page_num > TABLE_MAX_PAGES) {
    printf("Tried to fetch page number out of bounds. %d > %d\n", page_num,
           TABLE_MAX_PAGES);
    exit(EXIT_FAILURE);
  }

  // check if we have the content in the pager cache
  // this case is for missed cache
  if (pager->pages[page_num] == NULL) {
    void *page = malloc(PAGE_SIZE);
    uint32_t num_pages = pager->file_length / PAGE_SIZE;

    // We might save a partial page at the end of the file
    if (pager->file_length % PAGE_SIZE) {
      num_pages += 1;
    }

    if (page_num <= num_pages) {
      // used to read a file. we use file descriptor to keep track of which file
      // is opened in the OS additional docs:
      // https://www.ibm.com/docs/zh-tw/zos/2.4.0?topic=functions-lseek-change-offset-file
      // i think lseek is used to create a file, second param is used to specify
      // size of the file and seek set is used to point to the start of the
      // file.
      lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
      // since we are now at the start of the file, we will try to read the
      // first PAGE_SIZE bits to the page object
      ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
      if (bytes_read == -1) {
        printf("Error reading the file: %d\n", errno);
        exit(EXIT_FAILURE);
      }
    }

    // we store the read bytes into the array of pages
    pager->pages[page_num] = page;

    // if we get page number higher than current page we
    // increment the max number of pages for the pager
    if (page_num >= pager->num_pages) {
      pager->num_pages = page_num + 1;
    }
  }

  // we return the specific page
  return pager->pages[page_num];
}

// this method is used to see row fits in which page of the table
void *cursor_value(Cursor *cursor) {

  // for a particular page number
  // we retrive the page from the pager cache
  uint32_t page_num = cursor->page_num;
  void *page = get_page(cursor->table->pager, page_num);

  return leaf_node_value(page, cursor->cell_num);
}

// incrementing the cursor to the next row / cell
void cursor_advance(Cursor *cursor) {
  uint32_t page_num = cursor->page_num;
  void * node = get_page(cursor->table->pager, page_num);

  cursor->cell_num += 1;
  if (cursor->cell_num >= (*leaf_node_num_cells(node))) {
    cursor->end_of_table = true;
  }
}

// this method reads from the database file where existing writes have occured
Pager *pager_open(const char *filename) {
  // we read the file with specific permissions
  // we get the file descriptor for the read file
  int fd = open(filename,
                O_RDWR |       // read / write mode
                    O_CREAT,   // make new file if it does not exists
                S_IWUSR |      // write permission
                    S_IRUSR);  // read permission

  // file descriptor is -1 if opening of file has failed for some reason
  if (fd == -1) {
    printf("Unable to open file.\n");
    exit(EXIT_FAILURE);
  }

  // we get the length of the file where database entries have been made
  off_t file_length = lseek(fd, 0, SEEK_END);

  // we allocate a new pager and setup the coressponding file
  // descriptor and length of the pager abstraction / file length
  Pager *pager = malloc(sizeof(Pager));
  pager->file_descriptor = fd;
  pager->file_length = file_length;
  pager->num_pages = (file_length / PAGE_SIZE);

  // is intended as a check to see if length of file is
  // divisible by PAGE_SIZE and check if the file is corrupted or not
  if (file_length % PAGE_SIZE != 0) {
    printf("Db file is not a whole number of pages. Corrupt file.\n");
    exit(EXIT_FAILURE);
  }

  // we initialize the pages in a pager to be null
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    pager->pages[i] = NULL;
  }

  return pager;
}

// this method is used to flush the contents of the page to the database file
void pager_flush(Pager *pager, uint32_t page_num) {
  // before flushing contents we check if the current page is null
  if (pager->pages[page_num] == NULL) {
    printf("Tried to flush null page\n");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

  if (offset == -1) {
    printf("Error seeking: %d\n", errno);
    exit(EXIT_FAILURE);
  }

  // we write the contents of the current page to the
  // file represented by file descriptor
  ssize_t bytes_written =
      write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);

  if (bytes_written == -1) {
    printf("Error writing: %d\n", errno);
    exit(EXIT_FAILURE);
  }
}

// this method is used to perform processes before the program exits safely
void db_close(Table *table) {
  Pager *pager = table->pager;
  // we check how many pages are completely full and then flush these to the
  // disk after flushing the contents to the disk we free up the page memory

  for (uint32_t i = 0; i < pager->num_pages; i++) {
    if (pager->pages[i] == NULL) {
      continue;
    }
    pager_flush(pager, i);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }

  // after writing is complete we close the file represented
  // by file descriptor to indicate to the OS that the file has been closed
  int result = close(pager->file_descriptor);
  if (result == -1) {
    printf("Error closing db file.\n");
    exit(EXIT_FAILURE);
  }

  // after flushing contents to the disk we free up all the pages
  // the pager abstraction and the table object
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    void *page = pager->pages[i];
    if (page) {
      free(page);
      pager->pages[i] = NULL;
    }
  }

  free(pager);
  free(table);
}

// this method is used to create an empty new table
Table *db_open(const char *filename) {
  Pager *pager = pager_open(filename);

  Table *table = malloc(sizeof(Table));
  table->pager = pager;
  table->root_page_num = 0;

  // create a new node from scratch and new db file
  if (pager->num_pages == 0) {
    void * root_node = get_page(pager, 0);
    initialize_leaf_node(root_node);
  }

  return table;
}

// TODO: add defs
void leaf_node_insert(Cursor * cursor, uint32_t key, Row * value) {
  void * node = get_page(cursor->table->pager, cursor->page_num);

  uint32_t num_cells = *leaf_node_num_cells(node);
  if (num_cells >= LEAF_NODE_MAX_CELLS) {
    printf("Need to split a leaf node !\n");
    exit(EXIT_FAILURE);
  }

  if (cursor->cell_num < num_cells) {
    // to make space for new cell
    for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
      memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i-1), LEAF_NODE_CELL_SIZE);
    }
  }

  *(leaf_node_num_cells(node)) += 1;
  *(leaf_node_key(node, cursor->cell_num)) = key;
  serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

// this method is used to create a pointer to the newly created input buffer
InputBuffer *new_input_buffer() {
  // creating a new pointer after
  // allocating block of memory on the heap
  InputBuffer *input_buffer = (InputBuffer *)malloc(sizeof(InputBuffer));

  // initializing to default values
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;

  return input_buffer;
}

// this method is used to free up the memory after the pointer
// has been used and no longer needed
void close_input_buffer(InputBuffer *input_buffer) {
  // we free up the buffer bit because after being set by the getline
  // we need to manually free up the space used by the buffer
  free(input_buffer->buffer);
  free(input_buffer);
}

// this is a utility method for prepending "db >"
void print_prompt() { printf("db > "); }

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

// this method is used to process meta commands
MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    close_input_buffer(input_buffer);
    db_close(table);
    exit(EXIT_SUCCESS);
  } else if (strcmp(input_buffer->buffer, ".constants") == 0) {
    printf("Constants: \n");
    print_constants();
    return META_COMMAND_SUCCESS;
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

// this method is used to perform checks before executing insert operation
PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement) {
  statement->type = STATEMENT_INSERT;

  char *keyword = strtok(input_buffer->buffer, " ");
  char *id_string = strtok(NULL, " ");
  char *username = strtok(NULL, " ");
  char *email = strtok(NULL, " ");

  if (id_string == NULL || username == NULL || email == NULL) {
    return PREPARE_SYNTAX_ERROR;
  }

  int id = atoi(id_string);

  if (id < 0) {
    return PREPARE_NEGATIVE_ID;
  }
  if (strlen(username) > COLUMN_USERNAME_SIZE) {
    return PREPARE_STRING_TOO_LONG;
  }
  if (strlen(email) > COLUMN_EMAIL_SIZE) {
    return PREPARE_STRING_TOO_LONG;
  }

  statement->row_to_insert.id = id;
  strcpy(statement->row_to_insert.username, username);
  strcpy(statement->row_to_insert.email, email);

  return PREPARE_SUCCESS;
}

// this method is used to compare the input syntax and infer types
PrepareResult prepare_statement(InputBuffer *input_buffer,
                                Statement *statement) {
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    return prepare_insert(input_buffer, statement);
  }
  if (strncmp(input_buffer->buffer, "select", 6) == 0) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }
  return PREPARE_UNRECOGNIZED_STATEMENT;
}

// this method is used to insert rows into table
ExecuteResult execute_insert(Statement *statement, Table *table) {
  void * node = get_page(table->pager, table->root_page_num);
  if ((*leaf_node_num_cells(node) >= LEAF_NODE_MAX_CELLS)) {
    return EXECUTE_TABLE_FULL;
  }

  Row *row_to_insert = &(statement->row_to_insert);
  // we create a cursor to the current last row to insert a new record
  Cursor *cursor = table_end(table);

  leaf_node_insert(cursor, row_to_insert->id, row_to_insert);

  free(cursor);

  return EXECUTE_SUCCESS;
}

// this method is used to show all the rows in a table
ExecuteResult execute_select(Statement *statement, Table *table) {
  // we initialize the cursor at the start of the table
  Cursor *cursor = table_start(table);
  Row row;

  // we keep incrementing rows unless we have reached the end of the table
  while (!(cursor->end_of_table)) {
    deserialize_row(cursor_value(cursor), &row);
    print_row(&row);
    // after printing the row to console we increment
    // the cursor to point to the next row
    cursor_advance(cursor);
  }

  free(cursor);

  return EXECUTE_SUCCESS;
}

// this method is a meta command to print of some node and row
// related constants
void print_constants() {
  printf("ROW_SIZE: %d\n", ROW_SIZE);
  printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
  printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
  printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
  printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
  printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

// we execute actual SQL statements here
ExecuteResult execute_statement(Statement *statement, Table *table) {
  switch (statement->type) {
    case STATEMENT_INSERT:
      return execute_insert(statement, table);
    case STATEMENT_SELECT:
      return execute_select(statement, table);
  }
}

// this method is the driver method
int main(int argc, char *argv[]) {
  // we allocate an input buffer
  if (argc < 2) {
    printf("Must supply a database filename.\n");
    exit(EXIT_FAILURE);
  }

  char *filename = argv[1];
  Table *table = db_open(filename);

  InputBuffer *input_buffer = new_input_buffer();

  // the REPL starting point
  while (true) {
    print_prompt();
    // we read the input
    read_input(input_buffer);

    // we process meta commands in this section
    if (input_buffer->buffer[0] == '.') {
      switch (do_meta_command(input_buffer, table)) {
        case META_COMMAND_SUCCESS:
          continue;
        case META_COMMAND_UNRECOGNIZED_COMMAND:
          printf("Unrecognized command '%s'.\n", input_buffer->buffer);
          continue;
      }
    }

    // we process actual SQL queries here
    Statement statement;
    switch (prepare_statement(input_buffer, &statement)) {
      case PREPARE_SUCCESS:
        break;
      case PREPARE_STRING_TOO_LONG:
        printf("String is too long.\n");
        continue;
      case PREPARE_NEGATIVE_ID:
        printf("ID must be positive.\n");
        continue;
      case PREPARE_SYNTAX_ERROR:
        printf("Syntax error. Could not parse statement.\n");
        continue;
      case PREPARE_UNRECOGNIZED_STATEMENT:
        printf("Unrecognized keyword at start of '%s'.\n",
               input_buffer->buffer);
        continue;
    }

    // we execute actual queries here
    switch (execute_statement(&statement, table)) {
      case EXECUTE_SUCCESS:
        printf("Executed.\n");
        break;
      case EXECUTE_TABLE_FULL:
        printf("Error: Table full.\n");
        break;
    }
  }
}
