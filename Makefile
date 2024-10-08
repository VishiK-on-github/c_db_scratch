compile: db.c
	clang db.c -o bin/db

format: *.c
	clang-format -style=Google -i *.c

run:
	./bin/db mydb.db

test: db
	bundle exec rspec