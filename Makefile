compile: db.c
	clang db.c -o bin/db.exe

format: *.c
	clang-format -style=Google -i *.c