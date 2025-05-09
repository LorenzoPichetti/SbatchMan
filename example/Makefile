BIN_FOLDER=bin
SRC_FOLDER=src
FLAGS=-fopenmp

all: $(BIN_FOLDER)/testInt $(BIN_FOLDER)/testFloat $(BIN_FOLDER)/testDouble

$(BIN_FOLDER)/testInt: $(SRC_FOLDER)/vecAddTest.c 
	mkdir -p $(BIN_FOLDER)
	gcc -o $@ $< -Ddtype=int $(FLAGS)

$(BIN_FOLDER)/testFloat: $(SRC_FOLDER)/vecAddTest.c
	mkdir -p $(BIN_FOLDER)
	gcc -o $@ $< -Ddtype=float $(FLAGS)

$(BIN_FOLDER)/testDouble: $(SRC_FOLDER)/vecAddTest.c
	mkdir -p $(BIN_FOLDER)
	gcc -o $@ $< -Ddtype=double $(FLAGS)

clean:
	rm -rf $(BIN_FOLDER)