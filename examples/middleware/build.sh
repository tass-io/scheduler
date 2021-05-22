function make_build {
	mkdir ./build
	cp ../../main.go ./build/
	cp demo.go ./build/ 
	cd ./build
	go build -o main
	mv main	../
	cd ../
	rm -rf ./build
}

make_build
